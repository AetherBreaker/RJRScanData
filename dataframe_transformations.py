if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from decimal import Decimal
from logging import getLogger
from re import compile
from string import Template
from typing import Any, Callable, Type

from dataframe_utils import NULL_VALUES, combine_same_coupons, distribute_discount, fix_decimals
from pandas import DataFrame, Series, concat, isna
from reporting_validation_errs import report_errors
from rich.progress import Progress
from types_column_names import (
  BulkRateCols,
  GSheetsBuydownsCols,
  GSheetsVAPDiscountsCols,
  ItemizedInvoiceCols,
)
from types_custom import (
  AddressInfoType,
  BulkRateDataType,
  BuydownsDataType,
  DeptIDsEnum,
  ModelContextType,
  StoreNum,
  StoreScanData,
  VAPDataType,
)
from utils import convert_storenum_to_str, taskgen_whencalled
from validation_config import CustomBaseModel
from validation_itemizedinvoice import ItemizedInvoiceModel
from validation_other import BulkRateModel

logger = getLogger(__name__)


base_context: ModelContextType = {
  "skip_fields": {"Dept_ID": None, "Quantity": None},
}


COUPON_DEPARTMENTS = ["Coupon$", "PMPromos", "PromosLT", "PromosST"]
PM_LOYALTY_DEPARTMENTS = ["PMCOUPON"]
PM_LOYALTY_APPLICABLE_DEPARTMENTS = ["CigsMarl"]

NOT_COUPON_DEPARTMENTS = COUPON_DEPARTMENTS + PM_LOYALTY_DEPARTMENTS

mixnmatch_rate_pattern = Template(r"(?P<Quantity>\d+) ${uom}/\$$(?P<Price>[\d\.]+)")

VALID_MANUFACTURER_MULTIPACK_PATTERNS: dict[tuple[tuple[int, Decimal]], str] = {}

rjr_depts = DeptIDsEnum.rjr_depts()
pm_depts = DeptIDsEnum.pm_depts()
scan_depts = rjr_depts.union(pm_depts)


def context_setup(func: Callable) -> Callable:
  def wrapper(
    row: Series,
    errors: dict[int, Any] = None,
    *args,
    **kwargs,
  ):
    context = base_context.copy()

    update = {
      "row_id": row.name,
      "input": row.to_dict(),
      "row_err": {},
    }

    # if key storenum in row
    if ItemizedInvoiceCols.Store_Number in row.index:
      update["store_num"] = row[ItemizedInvoiceCols.Store_Number]

    context.update(update)

    result = func(
      *args,
      **kwargs,
      context=context,
      row=row,
    )

    if context["row_err"]:
      if errors is not None:
        row_errors = errors.get(row.name, [])
        row_errors.append(context["row_err"])
      report_errors(context)

    return result

  return wrapper


@context_setup
def apply_model_to_df_transforming(
  context: ModelContextType,
  row: Series,
  new_rows: list[Series],
  model: Type[CustomBaseModel],
) -> Series:
  """
  Apply a model to a row of a DataFrame.
  This version transforms the dataframe and should be passed an empty list to append new rows to
  for concatenation.

  :param context: The context to use.
  :param row: The row to transform.
  :param new_rows: The list of new rows to append to.
  :param model: The model to apply.
  :return: The transformed row.
  """

  context["model"] = model

  # create a new instance of the model
  model = model.model_validate(context["input"], context=context)

  # serialize the model to a dict
  model_dict = model.model_dump()

  # create a new Series from the model dict
  new_row = Series(model_dict, name=context["row_id"], dtype=object)

  new_rows.append(new_row)

  return row


@context_setup
def apply_model_to_df(
  row: Series,
  context: ModelContextType,
  model: Type[CustomBaseModel],
) -> Series:
  """
  Apply a model to a row of a DataFrame.

  :param context: The context to use.
  :param row: The row to transform.
  :param model: The model to apply.
  :return: The transformed row.
  """

  context["model"] = model

  # create a new instance of the model
  model = model.model_validate(context["input"], context=context)

  # serialize the model to a dict
  model_dict = model.model_dump()

  row.update(model_dict)

  return row


def first_validation_pass(
  pbar: Progress,
  data: StoreScanData,
  addr_info: AddressInfoType,
  errors: dict[int, list[Any]] = None,
) -> StoreScanData:
  storenum = data.storenum
  bulk_rate_data = data.bulk_rate_data
  itemized_invoice_data = data.itemized_invoice_data

  itemized_invoice_data[ItemizedInvoiceCols.Store_Number] = storenum
  itemized_invoice_data[ItemizedInvoiceCols.Store_Name] = convert_storenum_to_str(storenum)

  # filter itemized invoices down to only RJR and PM departments
  itemized_invoice_data = itemized_invoice_data[
    itemized_invoice_data[ItemizedInvoiceCols.Dept_ID].isin(scan_depts)
  ]

  # bulk_rate_data = bulk_rate_data.map(fillnas)
  # itemized_invoice_data = itemized_invoice_data.map(fillnas)

  # bulk_rate_data = bulk_rate_data.map(fix_str_decimals)
  # itemized_invoice_data = itemized_invoice_data.map(fix_str_decimals)

  bulk_rate_data = bulk_rate_data.map(fix_decimals)
  itemized_invoice_data = itemized_invoice_data.map(fix_decimals)

  itemized_invoice_data = itemized_invoice_data.astype(object)

  bulk_rate_data = bulk_rate_data.replace(NULL_VALUES, value=None)
  itemized_invoice_data = itemized_invoice_data.replace(NULL_VALUES, value=None)

  itemized_invoice_data.sort_values(ItemizedInvoiceCols.DateTime, inplace=True)

  # bulk_rate_data.to_csv(f"bulk_rate_data_{storenum}.csv", index=False)
  # itemized_invoice_data.to_csv(f"itemized_invoice_data_{storenum}.csv", index=False)

  bulk_rate_data = bulk_rate_data.apply(
    taskgen_whencalled(
      pbar,
      description=f"Validating {storenum:0>3} bulk rates",
      total=len(bulk_rate_data),
      clear_when_finished=True,
    )(apply_model_to_df)(),
    model=BulkRateModel,
    axis=1,
    result_type="broadcast",
  )
  logger.info(
    f"SFT {storenum:0>3}: [bold orange_red1]Finished[/] validating bulk rates",
    extra={"markup": True},
  )

  series_to_concat = []

  itemized_invoice_data.apply(
    taskgen_whencalled(
      pbar,
      description=f"Validating {storenum:0>3} itemized invoices",
      total=len(itemized_invoice_data),
      clear_when_finished=True,
    )(apply_addrinfo_and_initial_validation)(),
    axis=1,
    model=ItemizedInvoiceModel,
    errors=errors,
    new_rows=series_to_concat,
    addr_data=addr_info,
  )
  logger.info(
    f"SFT {storenum:0>3}: [bold yellow]Finished[/] validating itemized invoices",
    extra={"markup": True},
  )

  itemized_invoice_data = concat(series_to_concat, axis=1).T

  itemized_invoice_data = itemized_invoice_data[
    itemized_invoice_data[ItemizedInvoiceCols.Dept_ID].isin(DeptIDsEnum.all_columns())
  ]

  logger.info(
    f"SFT {storenum:0>3}: [bold bright_green]Finished[/] first validation pass",
    extra={"markup": True},
  )

  return StoreScanData(
    storenum=storenum,
    bulk_rate_data=bulk_rate_data,
    itemized_invoice_data=itemized_invoice_data,
  )


@context_setup
def apply_addrinfo_and_initial_validation(
  row: Series,
  context: ModelContextType,
  new_rows: list[Series],
  model: Type[CustomBaseModel],
  addr_data: AddressInfoType,
) -> Series:
  """
  Apply a model to a row of a DataFrame.

  :param context: The context to use.
  :param row: The row to transform.
  :param new_rows: The list of new rows to append to.
  :param model: The model to apply.
  :return: The transformed row.
  """

  context["model"] = model

  address_info = addr_data.loc[context["store_num"]].to_dict()

  context["input"].update(address_info)

  # create a new instance of the model
  model = model.model_validate(context["input"], context=context)

  if quantity_err := context["row_err"].get("Quantity", None):
    if isinstance(quantity_err[0], Decimal):
      return row

  # serialize the model to a dict
  model_dict = model.model_dump()

  new_row = Series(model_dict, name=context["row_id"], dtype=object)

  new_rows.append(new_row)

  return row


def process_item_lines(
  group: DataFrame,
  bulk_rate_data: dict[StoreNum, BulkRateDataType],
  buydowns_data: BuydownsDataType,
  vap_data: VAPDataType,
) -> DataFrame:
  group = apply_vap(group, vap_data)
  group = apply_buydowns(group, buydowns_data)

  group = calculate_scanned_coupons(group)
  group = identify_bulk_rates(group, bulk_rate_data)
  group = identify_multipack(group)
  group = identify_loyalty(group)

  return group


def apply_vap(group: DataFrame, vap_data: VAPDataType) -> DataFrame:
  for index, row in group.iterrows():
    upc = row[ItemizedInvoiceCols.ItemNum]

    vap_data_match = vap_data.loc[vap_data[GSheetsVAPDiscountsCols.UPC] == upc, :]

    if not vap_data_match.empty:
      assert len(vap_data_match) == 1

      vap_row = vap_data_match.iloc[0]

      vap_amt = vap_row[GSheetsVAPDiscountsCols.Discount_Amt]
      vap_desc = vap_row[GSheetsVAPDiscountsCols.Discount_Type]

      group.loc[index, ItemizedInvoiceCols.Manufacturer_Discount_Amt] = vap_amt
      group.loc[index, ItemizedInvoiceCols.Manufacturer_Promo_Desc] = vap_desc

  return group


def apply_buydowns(group: DataFrame, buydowns_data: BuydownsDataType) -> DataFrame:
  for index, row in group.iterrows():
    # lookup the item by State and UPC in the buydowns data
    state = row[ItemizedInvoiceCols.Store_State]
    upc = row[ItemizedInvoiceCols.ItemNum]

    buydowns_data_match = buydowns_data.loc[
      (buydowns_data[GSheetsBuydownsCols.State] == state)
      & (buydowns_data[GSheetsBuydownsCols.UPC] == upc),
      :,
    ]

    if not buydowns_data_match.empty:
      assert len(buydowns_data_match) == 1

      buydown_row = buydowns_data_match.iloc[0]

      buydown_amt = buydown_row[GSheetsBuydownsCols.Buydown_Amt]

      if buydown_amt is not None:
        buydown_desc = buydown_row[GSheetsBuydownsCols.Buydown_Desc]

        fixed_item_price = row[ItemizedInvoiceCols.Inv_Price] + buydown_amt

        group.loc[index, ItemizedInvoiceCols.Manufacturer_Buydown_Amt] = buydown_amt
        group.loc[index, ItemizedInvoiceCols.Manufacturer_Buydown_Desc] = buydown_desc

        group.loc[index, ItemizedInvoiceCols.Inv_Price] = fixed_item_price

  return group


def calculate_scanned_coupons(group: DataFrame) -> DataFrame:
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

    # TODO account for percentage discounts

    distributed_discounts = distribute_discount(
      invoice_prices, invoice_quantities, biggest_coupon_value
    )

    # TODO update to apply to PID coupon after identifying SCANNED coupons
    group.loc[is_coupon_applicable, ItemizedInvoiceCols.Acct_Promo_Name] = biggest_coupon_name
    group.loc[is_coupon_applicable, ItemizedInvoiceCols.Acct_Discount_Amt] = distributed_discounts

  return group


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


def identify_multipack(group: DataFrame):
  # sourcery skip: move-assign, remove-redundant-if
  for index, row in group.iterrows():
    if mixnmatchrate := row[ItemizedInvoiceCols.MixNMatchRate]:
      uom = row[ItemizedInvoiceCols.Unit_Type] or ""
      if isna(uom):
        uom = ""
      pattern = compile(mixnmatch_rate_pattern.substitute(uom=uom))
      if match := pattern.match(mixnmatchrate):
        multipack_quantity = int(match["Quantity"])
        multipack_price = Decimal(match["Price"])

        multipack_price_per_item = multipack_price / multipack_quantity

        discount_per_item = row[ItemizedInvoiceCols.Inv_Price] - multipack_price_per_item

        # TODO check if this mix n match is a manufacturer multipack or a retailer multipack
        if multipack_desc := VALID_MANUFACTURER_MULTIPACK_PATTERNS.get(
          (multipack_quantity, multipack_price)
        ):
          disc_amt_set_field = ItemizedInvoiceCols.Manufacturer_Multipack_Discount_Amt
          multi_quantity_set_field = ItemizedInvoiceCols.Manufacturer_Multipack_Quantity
          group.loc[index, ItemizedInvoiceCols.Manufacturer_Multipack_Desc] = multipack_desc
        else:
          disc_amt_set_field = ItemizedInvoiceCols.Retail_Multipack_Disc_Amt
          multi_quantity_set_field = ItemizedInvoiceCols.Retail_Multipack_Quantity

        group.loc[index, disc_amt_set_field] = discount_per_item
        group.loc[index, multi_quantity_set_field] = multipack_quantity

  return group


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


# TODO
def apply_outlet_promos(group: DataFrame) -> DataFrame:
  raise NotImplementedError
