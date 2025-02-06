if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from decimal import Decimal
from logging import getLogger
from re import compile
from string import Template
from typing import Any, Type

from pandas import DataFrame, Series

from dataframe_utils import combine_same_coupons, distribute_discount
from types_column_names import BulkRateCols, ItemizedInvoiceCols
from types_custom import AddressInfoType, BulkRateDataType, StoreNum
from validation_config import CustomBaseModel

logger = getLogger(__name__)


def apply_model_to_df_transforming(
  row: Series, new_rows: list[Series], model: Type[CustomBaseModel], errors: list[Any]
) -> Series:
  """
  Apply a model to a row of a DataFrame.
  This version transforms the dataframe and should be passed an empty list to append new rows to
  for concatenation.

  :param row: The row to transform.
  :param new_rows: The list of new rows to append to.
  :param model: The model to apply.
  :return: The transformed row.
  """

  # serialize row to a dict
  row_dict = row.to_dict()

  context = {
    "row_id": row.name,
    "errors": errors,
    "input": row_dict,
  }

  # create a new instance of the model
  model = model.model_validate(row_dict, context=context)

  # serialize the model to a dict
  model_dict = model.model_dump()

  # create a new Series from the model dict
  new_row = Series(model_dict, name=row.name, dtype=object)

  new_rows.append(new_row)

  return row


def apply_model_to_df(
  row: Series, model: Type[CustomBaseModel], errors: list[Any] = None
) -> Series:
  """
  Apply a model to a row of a DataFrame.

  :param row: The row to transform.
  :param model: The model to apply.
  :param errors: The list of errors to append to.
  :return: The transformed row.
  """

  # serialize row to a dict
  row_dict = row.to_dict()

  context = {
    "row_id": row.name,
    "input": row_dict,
  }
  if errors:
    context["errors"] = errors

  # create a new instance of the model
  model = model.model_validate(row_dict, context=context)

  # serialize the model to a dict
  model_dict = model.model_dump()

  row.update(model_dict)

  return row


def apply_addrinfo_and_initial_validation(
  row: Series,
  new_rows: list[Series],
  model: Type[CustomBaseModel],
  addr_data: AddressInfoType,
  errors: list[Any],
) -> Series:
  """
  Apply a model to a row of a DataFrame.

  :param row: The row to transform.
  :param model: The model to apply.
  :param errors: The list of errors to append to.
  :return: The transformed row.
  """

  storenum: StoreNum = row[ItemizedInvoiceCols.Store_Number]

  address_info = addr_data.loc[storenum].to_dict()

  # serialize row to a dict
  row_dict = row.to_dict()
  row_dict.update(address_info)

  context = {
    "row_id": row.name,
    "errors": errors,
    "input": row_dict,
    "storenum": storenum,
    "row_err": {},
  }

  # create a new instance of the model
  model = model.model_validate(row_dict, context=context)

  # if dept_id failed to validate, skip this row and exclude from dataset
  if "Dept_ID" in context["row_err"]:
    return row

  # serialize the model to a dict
  model_dict = model.model_dump()

  new_row = Series(model_dict, name=row.name, dtype=object)

  new_rows.append(new_row)

  return row


COUPON_DEPARTMENTS = ["Coupon$", "PMPromos", "PromosLT", "PromosST"]
PM_LOYALTY_DEPARTMENTS = ["PMCOUPON"]
PM_LOYALTY_APPLICABLE_DEPARTMENTS = ["CigsMarl"]

NOT_COUPON_DEPARTMENTS = COUPON_DEPARTMENTS + PM_LOYALTY_DEPARTMENTS


def process_item_lines(
  group: DataFrame, bulk_rate_data: dict[StoreNum, BulkRateDataType]
) -> DataFrame:
  group = calculate_coupons(group)
  group = identify_bulk_rates(group, bulk_rate_data)

  group = identify_loyalty(group)

  group = identify_multipack(group)

  return group


def calculate_coupons(group: DataFrame) -> DataFrame:
  # sourcery skip: extract-method

  # grab the dept_id of each row
  dept_ids = group[ItemizedInvoiceCols.Dept_ID]

  is_coupon = dept_ids.isin(COUPON_DEPARTMENTS)

  is_coupon_applicable = ~dept_ids.isin(NOT_COUPON_DEPARTMENTS)

  # if the group is nothing by coupon departments, then this invoice only contained items
  # that don't need to be reported
  if is_coupon.all():
    group.drop(index=group.index, inplace=True)
    return group

  # check if any of the dept_ids are in the COUPON_DEPARTMENTS list
  has_coupon = any(is_coupon)

  # check if the group has multiple lines in a valid coupon department
  # has_multiple_coupons = sum(is_coupon) > 1

  if has_coupon:
    coupon_line_indexes = group.loc[
      is_coupon & ~group.duplicated(ItemizedInvoiceCols.ItemNum, keep="first")
    ].index

    group = combine_same_coupons(group, coupon_line_indexes)

    biggest_coupon_index = group.loc[is_coupon, ItemizedInvoiceCols.Inv_Price].idxmax()

    biggest_coupon_row = group.loc[biggest_coupon_index]

    biggest_coupon_value = biggest_coupon_row[ItemizedInvoiceCols.Inv_Price]
    biggest_coupon_name = biggest_coupon_row[ItemizedInvoiceCols.ItemName]

    group.drop(index=coupon_line_indexes, inplace=True)

    invoice_prices = group.loc[is_coupon_applicable, ItemizedInvoiceCols.Inv_Price]
    invoice_quantities = group.loc[is_coupon_applicable, ItemizedInvoiceCols.Quantity]

    distributed_discounts = distribute_discount(
      invoice_prices, invoice_quantities, biggest_coupon_value
    )

    group.loc[is_coupon_applicable, ItemizedInvoiceCols.Acct_Promo_Name] = biggest_coupon_name
    group.loc[is_coupon_applicable, ItemizedInvoiceCols.Acct_Discount_Amt] = distributed_discounts

  return group


# 02848527
# 78437
def identify_bulk_rates(
  group: DataFrame, bulk_rate_data: dict[StoreNum, BulkRateDataType]
) -> DataFrame:
  if group.empty:
    return group

  storenum = group[ItemizedInvoiceCols.Store_Number].iloc[0]
  store_bulk_data = bulk_rate_data[storenum]

  for index, row in group.iterrows():
    # check if the itemnum is in the bulk rate data and whether the quantity meets the minimum required for a bulk rate
    itemnum = row[ItemizedInvoiceCols.ItemNum]
    if itemnum in store_bulk_data[BulkRateCols.ItemNum].values:
      bulk_rate_row: Series = store_bulk_data.loc[
        store_bulk_data[BulkRateCols.ItemNum] == itemnum
      ].iloc[0]
      bulk_quan = bulk_rate_row[BulkRateCols.Bulk_Quan]
      bulk_price = bulk_rate_row[BulkRateCols.Bulk_Price]

      bulk_price_per_item = bulk_price / bulk_quan

      item_price = row[ItemizedInvoiceCols.Inv_Price]

      bulk_disc_per_item = item_price - bulk_price_per_item

      quantity = row[ItemizedInvoiceCols.Quantity]

      if quantity >= bulk_quan:
        group.loc[index, ItemizedInvoiceCols.Retail_Multipack_Disc_Amt] = bulk_disc_per_item
        group.loc[index, ItemizedInvoiceCols.Retail_Multipack_Quantity] = bulk_quan

  return group


mixnmatch_rate_pattern = Template(r"(?P<Quantity>\d+) ${uom}/\$(?P<Price>[\d\.]+)")


def identify_multipack(group: DataFrame):
  for index, row in group.iterrows():
    pattern = compile(mixnmatch_rate_pattern.substitute(uom=row[ItemizedInvoiceCols.Unit_Type]))
    if match := pattern.match(row[ItemizedInvoiceCols.MixNMatchRate]):
      multipack_quantity = int(match["Quantity"])
      multipack_price = Decimal(match["Price"])

      multipack_price_per_item = multipack_price / multipack_quantity

      discount_per_item = row[ItemizedInvoiceCols.Inv_Price] - multipack_price_per_item

      # TODO check if this mix n match is a manufacturer multipack or a retailer multipack
      if True:
        # group.loc[index, ItemizedInvoiceCols.Manufacturer_Multipack_Desc] = ...
        group.loc[index, ItemizedInvoiceCols.Manufacturer_Multipack_Discount_Amt] = (
          discount_per_item
        )
        group.loc[index, ItemizedInvoiceCols.Manufacturer_Multipack_Quantity] = multipack_quantity

  pass


def identify_loyalty(group: DataFrame) -> DataFrame:
  # sourcery skip: extract-method

  # grab the dept_id of each row
  dept_ids = group[ItemizedInvoiceCols.Dept_ID]

  is_coupon = dept_ids.isin(PM_LOYALTY_DEPARTMENTS)

  is_coupon_applicable = dept_ids.isin(PM_LOYALTY_APPLICABLE_DEPARTMENTS)

  # if the group is nothing by coupon departments, then this invoice only contained items
  # that don't need to be reported
  if is_coupon.all():
    group.drop(index=group.index, inplace=True)
    return group

  # check if any of the dept_ids are in the COUPON_DEPARTMENTS list
  has_coupon = any(is_coupon)

  # check if the group has multiple lines in a valid coupon department
  # has_multiple_coupons = sum(is_coupon) > 1

  if has_coupon:
    coupon_line_indexes = group.loc[
      is_coupon & ~group.duplicated(ItemizedInvoiceCols.ItemNum, keep="first")
    ].index

    biggest_coupon_index = group.loc[is_coupon, ItemizedInvoiceCols.Inv_Price].idxmax()

    biggest_coupon_row = group.loc[biggest_coupon_index]

    biggest_coupon_value = biggest_coupon_row[ItemizedInvoiceCols.Inv_Price]

    group.drop(index=coupon_line_indexes, inplace=True)

    invoice_prices = group.loc[is_coupon_applicable, ItemizedInvoiceCols.Inv_Price]
    invoice_quantities = group.loc[is_coupon_applicable, ItemizedInvoiceCols.Quantity]

    distributed_discounts = distribute_discount(
      invoice_prices, invoice_quantities, biggest_coupon_value
    )

    group.loc[is_coupon_applicable, ItemizedInvoiceCols.loyalty_disc_desc] = biggest_coupon_row[
      ItemizedInvoiceCols.ItemName_Extra
    ]
    group.loc[is_coupon_applicable, ItemizedInvoiceCols.PID_Coupon_Discount_Amt] = (
      distributed_discounts
    )

  return group
