from logging import getLogger

from logging_config import configure_logging
from sql_query_builders import (
  build_inventory_data_query,
)
from sql_querying import query_all_stores_multithreaded
from types_custom import (
  QueryDict,
  QueryPackage,
)
from utils import CWD, upce_to_upca

configure_logging()

logger = getLogger(__name__)


storenum = 65


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


queries_result = query_all_stores_multithreaded(queries=queries, storenums=[storenum])


df = queries_result["inventory"].get(storenum)


df["UPCA"] = df["ItemNum"].map(upce_to_upca)

df.to_csv(CWD / f"{storenum}_inventory_data.csv", index=False)
