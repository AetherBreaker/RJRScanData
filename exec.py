from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime
from logging import getLogger
from string import Template

from dataframe_transformations import (
  apply_model_to_df_transforming,
  first_validation_pass,
  process_item_lines,
)
from gsheet_data_processing import SheetCache
from logging_config import configure_logging, rich_console
from pandas import concat
from rich.live import Live
from rich.progress import (
  BarColumn,
  MofNCompleteColumn,
  Progress,
  TaskProgressColumn,
  TextColumn,
  TimeRemainingColumn,
)
from rich.table import Table
from sql_query_builders import build_bulk_info_query, build_itemized_invoice_query
from sql_querying import DEFAULT_STORES_LIST, query_all_stores_multithreaded
from types_column_names import (
  GSheetsUnitsOfMeasureCols,
  ItemizedInvoiceCols,
  RJRScanHeaders,
)
from types_custom import QueryDict, ScanDataPackage, StoreScanData
from utils import TableColumn, get_full_dates, rjr_start_end_dates, taskgen_whencalled
from validation_rjr import RJRValidationModel

configure_logging()

logger = getLogger(__name__)


rjr_scan_file_format = Template(
  "B56192_${year}${month}${day}_${hour}${minute}_SWEETFIRETOBACCO.txt"
)


start_date, last_sun = get_full_dates()
queries: QueryDict = {
  "bulk_rate_data": build_bulk_info_query(),
  "itemized_invoice_data": build_itemized_invoice_query(start_date, last_sun),
}


result = query_all_stores_multithreaded(queries=queries)

sheet_data = SheetCache()
addr_data = sheet_data.info
unit_measure_data = sheet_data.uom

buydowns_data = sheet_data.bds
vap_data = sheet_data.vap


errors = {}


# @cached_for_testing


pbar = Progress(
  BarColumn(),
  TaskProgressColumn(),
  MofNCompleteColumn(),
  TimeRemainingColumn(),
  TextColumn("[progress.description]{task.description}"),
  console=rich_console,
)
items = {storenum: f"{storenum:0>3}" for storenum in DEFAULT_STORES_LIST}
remaining_pbar = Progress(
  TableColumn("Store Queries Remaining", items),
  console=rich_console,
)

remaining_task = remaining_pbar.add_task("", total=len(DEFAULT_STORES_LIST))
first_validation_task = pbar.add_task("Peforming first validation pass", total=len(result))

display_table = Table.grid()
display_table.add_row(remaining_pbar, pbar)


def validate_and_concat(
  pbar: Progress, remaining_pbar: Progress, data: list[StoreScanData]
) -> ScanDataPackage:
  # sourcery skip: for-append-to-extend
  store_validating_futures: list[Future] = []
  with (
    ThreadPoolExecutor(
      # max_workers=4,
    ) as executor
  ):
    for store_data in data:
      store_validating_futures.append(
        executor.submit(
          first_validation_pass,
          pbar=pbar,
          data=store_data,
          addr_info=addr_data,
          errors=errors,
        )
      )

    itemized_invoice_results = []
    bulk_rates = {}

    for future in as_completed(store_validating_futures):
      result: StoreScanData = future.result()
      itemized_invoice_results.append(result.itemized_invoice_data)
      bulk_rates[result.storenum] = result.bulk_rate_data
      pbar.update(first_validation_task, advance=1)
      remaining_pbar.update(remaining_task, advance=1, description=result.storenum)

  item_lines = concat(itemized_invoice_results, ignore_index=True)

  return ScanDataPackage(
    bulk_data=bulk_rates,
    itemized_invoice_data=item_lines,
  )


with Live(
  display_table,
  console=rich_console,
  # transient=True,
) as live:
  package = validate_and_concat(pbar, remaining_pbar, result)

  bulk_rates = package["bulk_data"]
  item_lines = package["itemized_invoice_data"]

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
    index=False,
  )
