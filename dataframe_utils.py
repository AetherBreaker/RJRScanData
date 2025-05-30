if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from decimal import Decimal
from logging import getLogger
from typing import Any

from numpy import nan
from pandas import DataFrame, Index, Series, isna
from types_column_names import ItemizedInvoiceCols
from utils import truncate_decimal

logger = getLogger(__name__)


def distribute_discount(prices: Series, quantities: Series, flat_discount: Decimal) -> Series:
  subtotal: Decimal = (prices * quantities).sum()
  percentages_of_total: Series = prices / subtotal

  discount_per_item: Series = percentages_of_total * flat_discount

  distributed_discounts = discount_per_item * quantities

  distributed_discounts = distributed_discounts.map(truncate_decimal)

  # fix rounding errors
  if distributed_discounts.sum() != flat_discount:
    difference = flat_discount - distributed_discounts.sum()
    largest_index = prices.idxmin()

    distributed_discounts[largest_index] += difference

  distributed_discounts = distributed_discounts.map(truncate_decimal)

  return distributed_discounts


def distribute_multipack(
  quantities: Series, per_item_discounnt: Decimal, disc_qty: int
) -> tuple[Series, Series]:
  per_item_discounnt = abs(per_item_discounnt / 2)
  disc_qty *= 2

  applied_count = 0

  # create a new series with the same index to store the distributed discounts
  distributed_discounts = Series(data=Decimal("0.00"), index=quantities.index, dtype="object")

  distributed_quantities = Series(data=0, index=quantities.index, dtype="int")

  while applied_count < disc_qty:
    for index, quantity in quantities.items():
      cur_qty = distributed_quantities[index]
      if quantity > cur_qty and applied_count < disc_qty:
        distributed_quantities.loc[index] += 1
        distributed_discounts.loc[index] += per_item_discounnt
        applied_count += 1

  return distributed_discounts, distributed_quantities


def combine_same_coupons(group: DataFrame, coupon_line_indexes: Index) -> DataFrame:
  itemnum_frequency = group[ItemizedInvoiceCols.ItemNum].value_counts()

  # combine like coupon rows
  for coupon_line_index in coupon_line_indexes:
    coupon_row = group.loc[coupon_line_index]

    coupon_itemnum = coupon_row[ItemizedInvoiceCols.ItemNum]

    # check if the coupon num occurs more than once
    if itemnum_frequency[coupon_itemnum] > 1:
      specific_coupon_duplicate_indexes = group.loc[
        group[ItemizedInvoiceCols.ItemNum] == coupon_itemnum
      ].index.difference([coupon_line_index])

      for specific_coupon_duplicate_index in specific_coupon_duplicate_indexes:
        specific_coupon_row = group.loc[specific_coupon_duplicate_index]

        group.loc[coupon_line_index, ItemizedInvoiceCols.Inv_Price] += specific_coupon_row[
          ItemizedInvoiceCols.Inv_Price
        ]
        group.loc[coupon_line_index, ItemizedInvoiceCols.Quantity] += specific_coupon_row[
          ItemizedInvoiceCols.Quantity
        ]

        group.drop(index=specific_coupon_duplicate_index, inplace=True)

  return group


NULL_VALUES = ["NULL", "", " ", float("nan"), nan]


def fillnas(value: Any) -> None | Any:
  if value is not None and (isna(value) or value in NULL_VALUES):
    return None

  return value


def applymap(row: Series, apply_func: callable) -> Series:
  for index, value in row.items():
    row[index] = apply_func(value)

  return row


def fix_decimals(x: Decimal) -> Decimal:
  return Decimal("0.00") if isinstance(x, Decimal) and str(x) == "0E-8" else x
