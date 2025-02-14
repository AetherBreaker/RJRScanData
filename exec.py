from datetime import timedelta
from logging import getLogger

from dataframe_transformations import (
  apply_model_to_df_transforming,
  process_item_lines,
)
from dateutil.relativedelta import relativedelta
from gsheet_data_processing import SheetCache
from logging_config import configure_logging, rich_console
from pandas import concat
from rich.progress import (
  BarColumn,
  MofNCompleteColumn,
  Progress,
  TaskProgressColumn,
  TextColumn,
  TimeRemainingColumn,
)
from sql_query_builders import build_bulk_info_query, build_itemized_invoice_query
from sql_querying import query_all_stores_multithreaded
from types_column_names import (
  GSheetsUnitsOfMeasureCols,
  ItemizedInvoiceCols,
  PMUSAScanHeaders,
  RJRScanHeaders,
)
from types_custom import QueryDict
from utils import get_last_sun, taskgen_whencalled
from validation_rjr import RJRValidationModel

configure_logging()

logger = getLogger(__name__)


with open("pmusaheaders.csv", "w") as f:
  f.write("|".join(PMUSAScanHeaders.all_columns()))


last_sun = get_last_sun() + relativedelta(weeks=1)
start_date = last_sun - timedelta(days=8)
queries: QueryDict = {
  "bulk_rate_data": build_bulk_info_query(),
  "itemized_invoice_data": build_itemized_invoice_query(start_date, last_sun),
}

sheet_data = SheetCache()
addr_data = sheet_data.info
unit_measure_data = sheet_data.uom

buydowns_data = sheet_data.bds
vap_data = sheet_data.vap


errors = {}

result = query_all_stores_multithreaded(
  queries=queries,
  addr_info=addr_data,
  errors=errors,
)

bulk_rates = result["bulk_data"]


item_lines = result["itemized_invoice_data"]
item_lines.sort_values(ItemizedInvoiceCols.DateTime, inplace=True)


item_lines.loc[:, ItemizedInvoiceCols.Unit_Type] = item_lines[ItemizedInvoiceCols.Unit_Type].map(
  unit_measure_data[GSheetsUnitsOfMeasureCols.Unit_of_Measure]
)


series_to_concat = []


with Progress(
  BarColumn(),
  TaskProgressColumn(),
  MofNCompleteColumn(),
  TimeRemainingColumn(),
  TextColumn("[progress.description]{task.description}"),
  console=rich_console,
) as pbar:
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

  item_lines = store_invoice_groups.apply(
    taskgen_whencalled(
      progress=pbar,
      description="Processing coupons",
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

  rjr_scan.to_csv("rjr_scan.csv")
pass
