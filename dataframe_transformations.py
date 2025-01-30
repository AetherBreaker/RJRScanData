if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from logging import getLogger
from typing import Any, Type

from pandas import DataFrame, Series

from types_column_names import ItemizedInvoiceCols
from types_custom import AddressInfoType, StoreNum
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
  }

  # create a new instance of the model
  model = model.model_validate(row_dict, context=context)

  # serialize the model to a dict
  model_dict = model.model_dump()

  new_row = Series(model_dict, name=row.name, dtype=object)

  new_rows.append(new_row)

  return row


COUPON_DEPARTMENTS = [
  "Coupon$",
  "PMPromos",
]
PM_LOYALTY_DEPARTMENTS = [
  "PMCOUPON",
]


def calculate_coupons(group: DataFrame) -> DataFrame:
  # grab the dept_id of each row
  dept_ids = group[ItemizedInvoiceCols.Dept_ID]

  is_coupon = dept_ids.isin(COUPON_DEPARTMENTS)

  # check if any of the dept_ids are in the COUPON_DEPARTMENTS list
  has_coupon = any(is_coupon)

  # check if the group has multiple lines in a valid coupon department
  # has_multiple_coupons = sum(is_coupon) > 1

  coupon_line_indexes = group.index[is_coupon]

  biggest_coupon_index = group[ItemizedInvoiceCols.PricePer].idxmin()

  biggest_coupon_row = group.loc[biggest_coupon_index]

  biggest_coupon_value = biggest_coupon_row[ItemizedInvoiceCols.PricePer]
  biggest_coupon_name = biggest_coupon_row[ItemizedInvoiceCols.ItemName]

  # if has_coupon:
  #   pass

  group.loc[:, ItemizedInvoiceCols.Acct_Promo_Name] = biggest_coupon_name
  group.loc[:, ItemizedInvoiceCols.Acct_Discount_Amt] = biggest_coupon_value

  # drop the coupon rows
  group.drop(index=coupon_line_indexes, inplace=True)

  # if has_coupon:
  #   pass

  return group
