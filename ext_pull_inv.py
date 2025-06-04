from logging import getLogger

from logging_config import configure_logging
from pandas import concat
from sql_query_builders import (
  build_inventory_data_query,
)
from sql_querying import DEFAULT_STORES_LIST, query_all_stores_multithreaded
from types_custom import (
  QueryDict,
  QueryPackage,
)
from utils import CWD, upce_to_upca

configure_logging()

logger = getLogger(__name__)


storenum = 27


queries: QueryDict = {
  "inventory": QueryPackage(
    query=build_inventory_data_query(),
    cols=[
      "ItemNum",
      "ItemName",
      "Cost",
      "Price",
      "Retail_Price",
      "In_Stock",
      "Dept_ID",
      "ItemName_Extra",
    ],
  ),
}


storenums = [
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
  31,
  32,
  34,
  35,
  36,
  38,
  40,
  42,
  43,
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
  60,
  82,
  84,
  85,
  86,
  88,
  # 37,
  # 44,
  # 62,
  # 63,
  # 64,
  # 66,
  # 67,
  # 59,
  # 65,
]

#
# queries_result = query_all_stores_multithreaded(queries=queries, storenums=[storenum])
queries_result = query_all_stores_multithreaded(queries=queries, storenums=storenums)

store_data = []


for storenum, data in queries_result["inventory"].items():
  df = queries_result["inventory"].get(storenum)

  df["UPCA"] = df["ItemNum"].map(upce_to_upca)

  df["ItemNum"] = df["ItemNum"].str.replace("[\t\n\r]", " ", regex=True).str.strip()
  df["ItemName"] = df["ItemName"].str.replace("[\t\n\r]", " ", regex=True).str.strip()

  store_data.append(df)
final = concat(store_data, ignore_index=True)
final = final.drop_duplicates(subset=["ItemNum", "Price", "Cost"], keep="first")


# final = queries_result["inventory"].get(storenum)

final["ItemNum"] = final["ItemNum"].str.replace("[\t\n\r]", " ", regex=True).str.strip()
final["ItemName"] = final["ItemName"].str.replace("[\t\n\r]", " ", regex=True).str.strip()

final["UPCA"] = final["ItemNum"].map(upce_to_upca)


final.to_csv(CWD / "all_inventory_data.csv", index=False)
