from logging import getLogger

from config import SETTINGS
from exec_final_validation import apply_altria_validation, apply_rjr_validation
from exec_initial_validation import process_promo_data, validate_and_concat_itemized, validate_bulk
from gsheet_data_processing import SheetCache
from logging_config import RICH_CONSOLE, configure_logging
from reporting_validation_errs import LoadReportingFiles
from rich_custom import LiveCustom
from sql_query_builders import build_bulk_info_query, build_itemized_invoice_query
from sql_querying import DEFAULT_STORES_LIST, query_all_stores_multithreaded
from types_column_names import (
  BulkRateCols,
  GSheetsUnitsOfMeasureCols,
  ItemizedInvoiceCols,
)
from types_custom import (
  BulkRateDataType,
  ItemizedInvoiceDataType,
  QueryDict,
  QueryPackage,
  StoreNum,
)
from utils import CWD, get_full_dates

if __debug__:
  from utils import cached_for_testing  # noqa: F401

configure_logging()

logger = getLogger(__name__)


full_period_start, full_period_end = get_full_dates(SETTINGS.week_shift)

queries: QueryDict = {
  "bulk_rates": QueryPackage(query=build_bulk_info_query(), cols=BulkRateCols),
  "invoices": QueryPackage(
    query=build_itemized_invoice_query(full_period_start, full_period_end), cols=ItemizedInvoiceCols
  ),
}


queries_result = query_all_stores_multithreaded(queries=queries)

logger.info("Initializing sheet data")
sheet_data = SheetCache()
logger.info("Sheet data initialized")

unit_measure_data = sheet_data.uom
buydowns_data = sheet_data.bds
vap_data = sheet_data.vap


itemized: dict[StoreNum, ItemizedInvoiceDataType] = queries_result["invoices"]
bulk: dict[StoreNum, BulkRateDataType] = queries_result["bulk_rates"]

PRECOMBINATION_ITEM_LINES_FOLDER = CWD / "item_lines"
PRECOMBINATION_ITEM_LINES_FOLDER.mkdir(exist_ok=True)

for storenum, invoices in itemized.items():
  sorted_invoices = invoices.sort_values(ItemizedInvoiceCols.Invoice_Number)
  sorted_invoices.to_csv(PRECOMBINATION_ITEM_LINES_FOLDER / f"{storenum:0>3}.csv", index=False)


empty = []


items = {storenum: str(storenum) for storenum in DEFAULT_STORES_LIST}


with LiveCustom(
  console=RICH_CONSOLE,
  # transient=True,
) as live:
  pbar = live.pbar

  with LoadReportingFiles():
    remaining_callable = live.init_remaining((items, "Itemized Invoices"))
    rjr_item_lines = validate_and_concat_itemized(
      pbar=pbar, remaining_pbar=remaining_callable, data=itemized, empty=empty
    )

    if empty:
      for storenum in empty:
        bulk.pop(storenum)

    remaining_callable = live.init_remaining((items, "Bulk Rates"))
    bulk_rates = validate_bulk(pbar, remaining_callable, bulk)

    rjr_item_lines.sort_values(ItemizedInvoiceCols.DateTime, inplace=True)

    rjr_item_lines.loc[:, ItemizedInvoiceCols.Unit_Type] = rjr_item_lines[
      ItemizedInvoiceCols.Unit_Type
    ].map(unit_measure_data[GSheetsUnitsOfMeasureCols.Unit_of_Measure])

    rjr_item_lines.sort_values(
      by=[
        ItemizedInvoiceCols.Store_Number,
        ItemizedInvoiceCols.DateTime,
      ],
      inplace=True,
    )

    rjr_item_lines[ItemizedInvoiceCols.Altria_Manufacturer_Multipack_Discount_Amt] = None
    rjr_item_lines[ItemizedInvoiceCols.Altria_Manufacturer_Multipack_Quantity] = None

    # from cProfile import Profile

    # profiler = Profile(subcalls=False, builtins=False)
    # profiler.enable()

    rjr_item_lines = process_promo_data(
      item_lines=rjr_item_lines,
      live=live,
      bulk_rates=bulk_rates,
      pbar=pbar,
      buydowns_data=buydowns_data,
      vap_data=vap_data,
    )

    # from pathlib import Path

    # OUTPUT = Path.cwd() / "profiler_output.txt"
    # profiler.disable()
    # profiler.dump_stats(str(OUTPUT))
    # exit()

    altria_item_lines = rjr_item_lines.copy(deep=True)

    altria_item_lines.to_csv("item_lines.csv", index=False)

    apply_altria_validation(
      pbar=pbar,
      input_data=altria_item_lines,
    )
    apply_rjr_validation(
      pbar=pbar,
      input_data=rjr_item_lines,
    )
