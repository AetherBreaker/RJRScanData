from datetime import timedelta
from logging import getLogger

from pandas import concat
from rich.progress import (
  BarColumn,
  MofNCompleteColumn,
  Progress,
  TaskProgressColumn,
  TextColumn,
  TimeRemainingColumn,
)

from dataframe_transformations import (
  apply_addrinfo_and_initial_validation,
  apply_model_to_df_transforming,
  calculate_coupons,
)
from gsheet_data_processing import SheetCache
from logging_config import configure_logging, rich_console
from sql_query_builders import build_bulk_info_query, build_itemized_invoice_query
from sql_querying import query_all_stores_multithreaded
from types_column_names import ItemizedInvoiceCols
from types_custom import QueryDict
from utils import get_last_sun, taskgen_whencalled
from validation_other import ItemizedInvoiceModel
from validation_rjr import RJRValidationModel

configure_logging()

logger = getLogger(__name__)


last_sun = get_last_sun()
start_date = last_sun - timedelta(days=8)
queries: QueryDict = {
  "bulk_rate_data": build_bulk_info_query(),
  "itemized_invoice_data": build_itemized_invoice_query(start_date, last_sun),
}

result = query_all_stores_multithreaded(queries)

bulk_rates = result["bulk_data"]


itemized_invoices = result["itemized_invoice_data"]
itemized_invoices.sort_values(ItemizedInvoiceCols.DateTime, inplace=True)


sheet_data = SheetCache()
addr_data = sheet_data.info

series_to_concat = []
errors = []

with Progress(
  BarColumn(),
  TaskProgressColumn(),
  MofNCompleteColumn(),
  TimeRemainingColumn(),
  TextColumn("[progress.description]{task.description}"),
  console=rich_console,
) as pbar:
  itemized_invoices.apply(
    taskgen_whencalled(
      pbar,
      description="Validating itemized invoices",
      total=len(itemized_invoices),
    )(apply_addrinfo_and_initial_validation)(),
    axis=1,
    model=ItemizedInvoiceModel,
    errors=errors,
    new_rows=series_to_concat,
    addr_data=addr_data,
  )

  item_lines = concat(series_to_concat, axis=1).T

  item_lines.sort_values(ItemizedInvoiceCols.DateTime, inplace=True)

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
    )(calculate_coupons)()
  )

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

  rjr_scan.to_csv("rjr_scan.csv")
pass
