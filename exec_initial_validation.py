from concurrent.futures import Future, ThreadPoolExecutor
from typing import Annotated, Callable

from dataframe_transformations import (
  bulk_rate_validation_pass,
  itemized_inv_first_validation_pass,
  process_item_lines,
)
from gsheet_data_processing import SheetCache
from pandas import concat
from rich.progress import Progress
from rich_custom import LiveCustom
from sql_querying import CUR_WEEK
from types_column_names import ItemizedInvoiceCols
from types_custom import (
  BulkDataPackage,
  BulkRateDataType,
  ItemizedDataPackage,
  ItemizedInvoiceDataType,
  StoreNum,
)
from utils import cached_for_testing, taskgen_whencalled

addr_data = SheetCache().info


def validate_and_concat_itemized(
  pbar: Progress,
  remaining_pbar: Callable[[int], None],
  data: dict[StoreNum, ItemizedInvoiceDataType],
  empty: list[int],
) -> ItemizedInvoiceDataType:
  itemized_invoice_results = []

  first_validation_task = pbar.add_task("Validating Itemized Invoices", total=len(data))

  def itemized_done_callback(future: Future):
    result: ItemizedDataPackage = future.result()
    if isinstance(result, int):
      empty.append(result)
      return
    itemized_invoice_results.append(result.itemized_invoice_data)
    pbar.update(first_validation_task, advance=1)
    remaining_pbar(result.storenum)

  store_validating_futures: list[Future] = []
  with (
    ThreadPoolExecutor(
      # max_workers=1,
    ) as executor
  ):
    for storenum, invoices in data.items():
      itemized_future = executor.submit(
        itemized_inv_first_validation_pass,
        pbar=pbar,
        storenum=storenum,
        itemized_invoice_data=invoices,
        addr_info=addr_data,
      )
      itemized_future.add_done_callback(itemized_done_callback)
      store_validating_futures.append(itemized_future)

  item_lines = concat(itemized_invoice_results, ignore_index=True)

  return item_lines


def validate_bulk(
  pbar: Progress, remaining_pbar: Callable[[int], None], data: dict[StoreNum, BulkRateDataType]
) -> BulkRateDataType:
  bulk_results = {}

  bulk_validation_task = pbar.add_task("Validating Bulk Rate Data", total=len(data))

  def bulk_done_callback(future: Future):
    result: BulkDataPackage = future.result()
    bulk_results[result.storenum] = result.bulk_rate_data
    pbar.update(bulk_validation_task, advance=1)
    remaining_pbar(result.storenum)

  with (
    ThreadPoolExecutor(
      # max_workers=4,
    ) as executor
  ):
    for storenum, bulk_data in data.items():
      bulk_future = executor.submit(
        bulk_rate_validation_pass,
        pbar=pbar,
        storenum=storenum,
        bulk_dat=bulk_data,
      )
      bulk_future.add_done_callback(bulk_done_callback)

  return bulk_results


# @cached_for_testing(date_for_sig=CUR_WEEK)
def process_promo_data[T: ItemizedInvoiceDataType](
  item_lines: Annotated[T, "ignore_for_sig"],
  live: Annotated[LiveCustom, "ignore_for_sig"],
  bulk_rates: Annotated[BulkRateDataType, "ignore_for_sig"],
  pbar: Annotated[Progress, "ignore_for_sig"],
  buydowns_data: Annotated[dict, "ignore_for_sig"],
  vap_data: Annotated[dict, "ignore_for_sig"],
) -> T:
  store_invoice_groups = item_lines.groupby(
    by=[ItemizedInvoiceCols.Store_Number, ItemizedInvoiceCols.Invoice_Number],
    as_index=False,
    group_keys=False,
    dropna=False,
  )[item_lines.columns]

  item_lines = store_invoice_groups.apply(
    taskgen_whencalled(
      progress=pbar,
      description="Applying promotion data to invoices",
      total=len(store_invoice_groups),
    )(process_item_lines)(),
    bulk_rate_data=bulk_rates,
    buydowns_data=buydowns_data,
    vap_data=vap_data,
  )

  return item_lines
