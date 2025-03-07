from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from logging import getLogger
from pathlib import Path
from string import Template
from typing import Callable

from dataframe_transformations import (
  apply_model_to_df_transforming,
  bulk_rate_validation_pass,
  itemized_inv_first_validation_pass,
  process_item_lines,
)
from gsheet_data_processing import SheetCache
from logging_config import RICH_CONSOLE, configure_logging
from pandas import concat
from rich.progress import Progress
from rich.table import Table
from rich_custom import LiveCustom
from sql_query_builders import build_bulk_info_query, build_itemized_invoice_query
from sql_querying import DEFAULT_STORES_LIST, query_all_stores_multithreaded
from types_column_names import (
  BulkRateCols,
  GSheetsUnitsOfMeasureCols,
  ItemizedInvoiceCols,
  RJRScanHeaders,
)
from types_custom import (
  BulkDataPackage,
  BulkRateDataType,
  ItemizedDataPackage,
  ItemizedInvoiceDataType,
  QueryDict,
  QueryPackage,
  StoreNum,
)
from utils import get_full_dates, rjr_start_end_dates, taskgen_whencalled
from validation_rjr import RJRValidationModel

configure_logging()

logger = getLogger(__name__)


CWD = Path.cwd()


rjr_scan_file_format = Template(
  "B56192_${year}${month}${day}_${hour}${minute}_SWEETFIRETOBACCO.txt"
)


date_start, date_end = get_full_dates()
queries: QueryDict = {
  "bulk_rates": QueryPackage(query=build_bulk_info_query(), cols=BulkRateCols),
  "invoices": QueryPackage(
    query=build_itemized_invoice_query(date_start, date_end), cols=ItemizedInvoiceCols
  ),
}


queries_result = query_all_stores_multithreaded(queries=queries)

logger.info("Initializing sheet data")
sheet_data = SheetCache()
logger.info("Sheet data initialized")
addr_data = sheet_data.info
unit_measure_data = sheet_data.uom

buydowns_data = sheet_data.bds
vap_data = sheet_data.vap


errors = {}


itemized: dict[StoreNum, ItemizedInvoiceDataType] = queries_result["invoices"]
bulk: dict[StoreNum, BulkRateDataType] = queries_result["bulk_rates"]

PRECOMBINATION_ITEM_LINES_FOLDER = CWD / "item_lines"
PRECOMBINATION_ITEM_LINES_FOLDER.mkdir(exist_ok=True)

for storenum, invoices in itemized.items():
  invoices.to_csv(PRECOMBINATION_ITEM_LINES_FOLDER / f"{storenum}.csv")


empty = []


def validate_and_concat_itemized(
  pbar: Progress,
  remaining_pbar: Callable[[int], None],
  data: dict[StoreNum, ItemizedInvoiceDataType],
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
      # max_workers=4,
    ) as executor
  ):
    for storenum, invoices in data.items():
      itemized_future = executor.submit(
        itemized_inv_first_validation_pass,
        pbar=pbar,
        storenum=storenum,
        itemized_invoice_data=invoices,
        addr_info=addr_data,
        errors=errors,
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


items = {storenum: str(storenum) for storenum in DEFAULT_STORES_LIST}


with LiveCustom(
  console=RICH_CONSOLE,
  # transient=True,
) as live:
  pbar = live.pbar

  remaining_callable = live.init_remaining((items, "Itemized Invoices"))
  item_lines = validate_and_concat_itemized(pbar, remaining_callable, itemized)

  if empty:
    for storenum in empty:
      bulk.pop(storenum)

  remaining_callable = live.init_remaining((items, "Bulk Rates"))
  bulk_rates = validate_bulk(pbar, remaining_callable, bulk)

  item_lines.sort_values(ItemizedInvoiceCols.DateTime, inplace=True)

  item_lines.loc[:, ItemizedInvoiceCols.Unit_Type] = item_lines[ItemizedInvoiceCols.Unit_Type].map(
    unit_measure_data[GSheetsUnitsOfMeasureCols.Unit_of_Measure]
  )

  series_to_concat = []

  item_lines.sort_values(
    by=[
      ItemizedInvoiceCols.Store_Number,
      ItemizedInvoiceCols.DateTime,
    ],
    inplace=True,
  )

  store_invoice_groups = item_lines.groupby(
    by=[ItemizedInvoiceCols.Store_Number, ItemizedInvoiceCols.Invoice_Number],
    as_index=False,
    group_keys=False,
    dropna=False,
  )[item_lines.columns]

  updated_display = Table.grid()
  updated_display.add_row(pbar)
  live.update(updated_display, refresh=True)

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

  item_lines.to_csv("item_lines.csv")

  new_rows = []

  item_lines.apply(
    taskgen_whencalled(
      pbar,
      "Validating RJR scan data",
      len(item_lines),
    )(apply_model_to_df_transforming)(),
    axis=1,
    new_rows=new_rows,
    model=RJRValidationModel,
    errors=errors,
  )

  rjr_scan = concat(new_rows, axis=1).T

  rjr_scan = rjr_scan[RJRScanHeaders.all_columns()]

  rjr_scan_start_date, rjr_scan_end_date = rjr_start_end_dates()

  # filter by date range
  rjr_scan = rjr_scan[
    (rjr_scan[RJRScanHeaders.transaction_date] >= rjr_scan_start_date)
    & (rjr_scan[RJRScanHeaders.transaction_date] < rjr_scan_end_date)
  ]

  now = datetime.now()

  rjr_scan.to_csv(
    rjr_scan_file_format.substitute(
      year=f"{now.year:0>4}",
      month=f"{now.month:0>2}",
      day=f"{now.day:0>2}",
      hour=f"{now.hour:0>2}",
      minute=f"{now.minute:0>2}",
    ),
    sep="|",
    index=False,
  )
