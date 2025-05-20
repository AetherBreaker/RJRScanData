from logging import getLogger

from config import SETTINGS
from exec_initial_validation import validate_and_concat_itemized
from logging_config import RICH_CONSOLE, configure_logging
from pandas import DataFrame, concat
from reporting_validation_errs import LoadReportingFiles
from rich_custom import LiveCustom
from sql_query_builders import (
  build_employee_info_query,
  build_itemized_invoice_query,
)
from sql_querying import query_all_stores_multithreaded
from types_column_names import (
  ItemizedInvoiceCols,
)
from types_custom import (
  ItemizedInvoiceDataType,
  QueryDict,
  QueryPackage,
  StoreNum,
)
from utils import CWD, rjr_start_end_dates, taskgen_whencalled

configure_logging()

logger = getLogger(__name__)


full_period_start, full_period_end = rjr_start_end_dates(SETTINGS.week_shift)

# full_period_start = datetime(
#   year=2025,
#   month=4,
#   day=9,
# )

# full_period_end = datetime(
#   year=2025,
#   month=4,
#   day=28,
# )


queries: QueryDict = {
  "invoices": QueryPackage(
    query=build_itemized_invoice_query(full_period_start, full_period_end), cols=ItemizedInvoiceCols
  ),
  "employees": QueryPackage(build_employee_info_query(), cols=["Cashier_ID", "EmpName"]),
}

DEFAULT_STORES_LIST = [
  # 1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  16,
  17,
  18,
  19,
  20,
  21,
  22,
  23,
  25,
  26,
  27,
  28,
  29,
  30,
  # 31,
  32,
  34,
  35,
  36,
  37,
  38,
  40,
  42,
  43,
  44,
  45,
  46,
  48,
  49,
  50,
  51,
  53,
  54,
  55,
  56,
  57,
  # 59,
  60,
  62,
  63,
  64,
  65,
  66,
  67,
  82,
  84,
  85,
  86,
  88,
]


queries_result = query_all_stores_multithreaded(queries=queries, storenums=DEFAULT_STORES_LIST)


itemized: dict[StoreNum, ItemizedInvoiceDataType] = queries_result["invoices"]

employees: dict[StoreNum, DataFrame] = queries_result["employees"]


for storenum, df in employees.items():
  df["Store_Number"] = storenum

employee_info = concat(
  list(employees.values()),
  ignore_index=True,
)


# employee_info.set_index(
#   keys=["Cashier_ID", "Store_Number"],
#   drop=False,
#   inplace=True,
#   verify_integrity=True,
# )

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
    item_lines = validate_and_concat_itemized(
      pbar=pbar, remaining_pbar=remaining_callable, data=itemized, empty=empty
    )

    item_lines.sort_values(
      by=[
        ItemizedInvoiceCols.Store_Number,
        ItemizedInvoiceCols.DateTime,
      ],
      inplace=True,
    )

    item_lines.to_csv("item_lines.csv", index=False)

    ocurrences = []

    groups = item_lines.groupby(
      by=[ItemizedInvoiceCols.Invoice_Number, ItemizedInvoiceCols.Store_Name],
      as_index=False,
      group_keys=False,
    )

    loyalty_coupon_itemnums = {
      "PMUSALoyalty",
      "PMUSALoyalty1",
      "USSTCLoyaltyIA",
      "USSTCLoyaltyMI",
      "USSTCLoyaltyOH",
      "USSTCLoyaltyWI",
      "HelixLoyaltyIA",
      "HelixLoyaltyMI",
      "HelixLoyaltyOH",
      "HelixLoyaltyWI",
    }

    items = {storenum: str(storenum) for storenum in DEFAULT_STORES_LIST}

    occurred_groups = []

    def find_dupes(group: DataFrame) -> DataFrame:
      counts = group[ItemizedInvoiceCols.ItemNum].value_counts()

      for coupon in loyalty_coupon_itemnums:
        if coupon in counts.index:
          count = counts[coupon]
          if count > 1:
            ocurrences.append((group[ItemizedInvoiceCols.Invoice_Number].iloc[0]))
            occurred_groups.append(group)

      return group

    applied: DataFrame = groups.apply(
      taskgen_whencalled(
        progress=pbar,
        description="Finding Loyalty Errors",
        total=len(groups),
      )(find_dupes)()
    )

    bad_invoices = concat(occurred_groups, ignore_index=True, axis=0)

    # replace Cashier_ID with EmpName
    bad_invoices = bad_invoices.merge(
      employee_info,
      how="left",
      left_on=["Cashier_ID", "Store_Number"],
      right_on=["Cashier_ID", "Store_Number"],
    )

    bad_invoices = bad_invoices[
      [
        "Store_Name",
        "Invoice_Number",
        "CustNum",
        "LineNum",
        "DateTime",
        "EmpName",
        "ItemNum",
        "ItemName",
        "ItemName_Extra",
        "DiffItemName",
        "Dept_ID",
        "Quantity",
        "CostPer",
        "PricePer",
        "Tax1Per",
        "Store_Address",
        "Store_Address2",
        "Store_City",
        "Store_State",
        "Store_Zip",
      ]
    ]

    bad_invoices.sort_values(
      by=[
        "Store_Name",
        "Invoice_Number",
        "LineNum",
        "DateTime",
      ],
      inplace=True,
    )

    print(len(ocurrences))

    bad_invoices.to_csv(f"errored_loyalty_lines_{full_period_end:%Y%m%d}.csv", index=False)
