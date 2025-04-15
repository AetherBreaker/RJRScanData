from logging import getLogger

from logging_config import configure_logging
from sql_query_builders import (
  build_volume_report_query,
)
from sql_querying import query_all_stores_multithreaded
from types_custom import (
  QueryDict,
  QueryPackage,
)
from validators_shared import map_to_upca

configure_logging()

logger = getLogger(__name__)


queries: QueryDict = {
  "volume": QueryPackage(
    query=build_volume_report_query(),
    cols=[
      "Invoice_Number",
      "ItemNum",
      "Cost",
      "Price",
      "Retail_Price",
      "In_Stock",
      "Dept_ID",
      "ItemName_Extra",
    ],
  ),
}


queries_result = query_all_stores_multithreaded(queries=queries, storenums=[31])


df = queries_result["inventory"].get(31)


df["UPCA"] = df["ItemNum"].map(map_to_upca)

df.to_csv(
  "SFT031_inventory.csv",
  index=False,
)
