if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from logging import getLogger
from pathlib import Path
from threading import Lock

from pandas import DataFrame, concat
from pypika.queries import Database, Query, QueryBuilder, Schema, Table
from rich.progress import (
  BarColumn,
  MofNCompleteColumn,
  Progress,
  TaskProgressColumn,
  TextColumn,
)

from logging_config import rich_console
from sql_querying import StoreSQLConn
from types_custom import (
  QueryDict,
  QueryResultsDict,
  StoreNum,
  StoreScanData,
)
from utils import cached_for_testing

logger = getLogger(__name__)


CWD = Path.cwd()


LAST_USED_DB_QUERYFILE = (CWD / __file__).with_name("last used database name.sql")


_DATABASE_CRESQL = Database("cresql")
_schema: Schema = _DATABASE_CRESQL.dbo
_table_depts: Table = _schema.Departments


def build_depts_query() -> QueryBuilder:
  """Build a query to retrieve itemized invoices between two dates.

  :param start_date: Start date to filter invoices. Inclusive
  :type start_date: date | datetime
  :param end_date: End date to filter invoices. Exclusive
  :type end_date: date | datetime
  :return: QueryBuilder to retrieve itemized invoices between two dates.
  :rtype: QueryBuilder
  """

  return Query.from_(_table_depts).select(
    _table_depts.Dept_ID,
    _table_depts.Store_ID,
    _table_depts.Description,
  )


_QUERY_THREADING_LOCK = Lock()


def update_database_name(new_db_name: str) -> None:
  if new_db_name != _schema._parent._name:
    _schema._parent = Database(new_db_name)


@cached_for_testing
def query_store(storenum: StoreNum, queries: QueryDict) -> QueryResultsDict:
  static_queries = {}
  results: QueryResultsDict = {}
  with StoreSQLConn(storenum) as conn:
    with conn.cursor() as cursor:
      db_name = cursor.execute(LAST_USED_DB_QUERYFILE.read_text()).fetchone()[0]
      with _QUERY_THREADING_LOCK:
        update_database_name(db_name)

        for query_name, query in queries.items():
          static_queries[query_name] = query.get_sql()

      for query_name, query in static_queries.items():
        results[query_name] = cursor.execute(query).fetchall()

  return results


TEMP_STORES_LIST = [1, 3, 8, 9, 16, 17, 19, 29, 42]


queries = {
  "depts": build_depts_query(),
}

store_querying_futures: list[Future] = []

with (
  ThreadPoolExecutor(
    # max_workers=4,
  ) as executor
):
  store_querying_futures.extend(
    executor.submit(query_store, storenum, queries) for storenum in TEMP_STORES_LIST
  )

  depts_list = []

  with Progress(
    BarColumn(),
    TaskProgressColumn(),
    MofNCompleteColumn(),
    TextColumn("[progress.description]{task.description}"),
    console=rich_console,
  ) as pbar:
    store_querying_task = pbar.add_task(
      "Querying Stores for Scan Data", total=len(TEMP_STORES_LIST)
    )
    for future in as_completed(store_querying_futures):
      result: StoreScanData = future.result()
      depts_raw = result["depts"]
      depts_raw = [tuple(row) for row in depts_raw]

      depts = DataFrame(
        depts_raw,
        dtype=object,
        columns=[
          "Dept_ID",
          "Store_ID",
          "Description",
        ],
      )
      depts_list.append(depts)
      pbar.update(store_querying_task, advance=1)

depts: DataFrame = concat(depts_list, ignore_index=True)

depts.sort_values(by=["Dept_ID", "Store_ID"], inplace=True)

depts.drop_duplicates(subset=["Dept_ID", "Description"], keep="first", inplace=True)


depts = depts.loc[
  depts["Description"].str.contains(r"\*", case=False, regex="True")
  | depts["Dept_ID"].isin(
    [
      "PMCOUPON",
      "COUPON",
      "COUPON$1",
      "COUPONS",
    ]
  )
]


depts["Dept_ID"].to_csv("depts.csv", index=False)
