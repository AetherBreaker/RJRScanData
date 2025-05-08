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


storenum = 59


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


# queries_result = query_all_stores_multithreaded(queries=queries, storenums=[storenum])
queries_result = query_all_stores_multithreaded(queries=queries, storenums=DEFAULT_STORES_LIST)

store_data = []


for storenum, data in queries_result["inventory"].items():
  df = queries_result["inventory"].get(storenum)

  df["UPCA"] = df["ItemNum"].map(upce_to_upca)

  store_data.append(df)


final = concat(store_data, ignore_index=True)
final = final.drop_duplicates(subset=["ItemNum"], keep="first")

final.to_csv(CWD / "all_inventory_data.csv", index=False)
