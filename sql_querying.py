if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import json
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from logging import getLogger
from pathlib import Path
from threading import Lock
from typing import Literal

from logging_config import rich_console
from pandas import DataFrame
from pyodbc import Connection, Error, OperationalError, connect
from rich.live import Live
from rich.progress import (
  BarColumn,
  MofNCompleteColumn,
  Progress,
  TaskProgressColumn,
  TextColumn,
)
from rich.table import Table
from sql_query_builders import (
  update_database_name,
)
from types_column_names import BulkRateCols, ItemizedInvoiceCols
from types_custom import (
  QueryDict,
  QueryResultsDict,
  SQLCreds,
  SQLHostName,
  StoreNum,
  StoreScanData,
)
from utils import TableColumn, cached_for_testing

logger = getLogger(__name__)


CWD = Path.cwd()


LAST_USED_DB_QUERYFILE = (CWD / __file__).with_name("last used database name.sql")
PCA_SQL_CREDS_PATH = (CWD / __file__).with_name("store_sql_creds.json")
STORE_SQL_CREDS_OVERRIDE_PATH = CWD / "store_sql_creds_override.json"

SQL_DRIVER_DEFAULT = "{ODBC Driver 18 for SQL Server}"


class NoConnectionError(Exception):
  pass


def load_sql_creds(sql_driver: str = SQL_DRIVER_DEFAULT) -> SQLCreds:
  creds = {"DRIVER": sql_driver}
  with PCA_SQL_CREDS_PATH.open("r") as file:
    creds |= json.load(file)

  if STORE_SQL_CREDS_OVERRIDE_PATH.exists():
    with STORE_SQL_CREDS_OVERRIDE_PATH.open("r") as file:
      creds |= json.load(file)

  return creds


SQL_HOSTNAME_METHOD_DEFAULT: Literal["DNS", "IP"] = 0
SQL_HOSTNAME_METHODS = ("DNS", "IP")

TAILSCALE_HOSTNAME_PATTERN = "pos-sft{storenum:0>3}.salamander-nunki.ts.net"
TAILSCALE_IP_ADDRESS_LOOKUP_JSON = (CWD / __file__).with_name("store_ip_address_lookup.json")


def get_store_sql_hostname(
  storenum: StoreNum,
  hostname_lookup_method: Literal["DNS", "IP"] = SQL_HOSTNAME_METHODS[SQL_HOSTNAME_METHOD_DEFAULT],
) -> SQLHostName:
  match hostname_lookup_method:
    case "DNS":
      return TAILSCALE_HOSTNAME_PATTERN.format(storenum=storenum)
    case "IP":
      with TAILSCALE_IP_ADDRESS_LOOKUP_JSON.open("r") as file:
        ip_address_lookup = json.load(file)
        if str(storenum) not in ip_address_lookup:
          raise ValueError(f"Store {storenum} IP address not found in lookup table")
      return ip_address_lookup[str(storenum)]


class StoreSQLConn:
  _conn_string_template = (
    "DRIVER={driver};"
    "SERVER={hostname};"
    # "SERVER={hostname},1433;"
    "UID={uid};"
    "PWD={pwd};"
    "TrustServerCertificate=yes;"
  )

  def __init__(self, storenum: StoreNum):
    self.storenum = storenum
    self.cred_data = load_sql_creds()
    self.hostname = get_store_sql_hostname(storenum)

  def establish_connection(self) -> Connection:
    try:
      conn = connect(
        self._conn_string_template.format(
          driver=self.cred_data["DRIVER"],
          hostname=self.hostname,
          uid=self.cred_data["UID"],
          pwd=self.cred_data["PWD"],
        ),
        readonly=True,
        timeout=10,
      )

      if conn is None:
        raise NoConnectionError(f"Failed to connect to store {self.storenum} SQL server")

    except Exception as e:
      exc_type, exc_val, exc_tb = type(e), e, e.__traceback__
      # if exc_type inhereits from Error or is Error
      sqlstate = exc_val.args[0]
      if (exc_type is OperationalError and sqlstate == "08001") or exc_type is NoConnectionError:
        logger.debug(
          f"SFT {self.storenum}: Connection to store  SQL server timed out",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )
        try:
          self.hostname = get_store_sql_hostname(self.storenum, hostname_lookup_method="IP")
        except ValueError as exc:
          raise NoConnectionError(f"Failed to connect to store {self.storenum} SQL server") from exc
        try:
          conn = connect(
            self._conn_string_template.format(
              driver=self.cred_data["DRIVER"],
              hostname=self.hostname,
              uid=self.cred_data["UID"],
              pwd=self.cred_data["PWD"],
            ),
            readonly=True,
            timeout=10,
          )
        except Exception as e:
          raise e

    return conn

  def __enter__(self) -> Connection:
    self.conn = self.establish_connection()

    if self.conn is None:
      raise NoConnectionError(f"Failed to connect to store {self.storenum} SQL server")

    return self.conn

  def __exit__(self, exc_type, exc_val, exc_tb):
    if exc_type is not None:
      # if exc_type inhereits from Error or is Error
      if issubclass(exc_type, Error):
        sqlstate = exc_val.args[0]
        if exc_type is OperationalError and sqlstate == "08001":
          logger.debug(f"SFT {self.storenum}: Connection to store SQL server timed out")
        else:
          logger.error(
            f"SFT {self.storenum}: Failed to connect to store SQL server",
            exc_info=(exc_type, exc_val, exc_tb),
            stack_info=True,
          )
        return True
      elif exc_type is NoConnectionError:
        logger.error(
          f"SFT {self.storenum}: Failed to connect to store SQL server",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )
        return True
      else:
        logger.error(
          f"SFT {self.storenum}: An unexpected exception occurred while connecting to store SQL server",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )

    if hasattr(self, "conn"):
      self.conn.close()


_QUERY_THREADING_LOCK = Lock()


# @cached_for_testing
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


def get_store_data(
  storenum: StoreNum,
  queries: QueryDict,
) -> StoreScanData:
  logger.info(
    f"SFT {storenum:0>3}: [bold blue]Started[/] querying store for scan data",
    extra={"markup": True},
  )
  try:
    results = query_store(storenum, queries)
    bulk_rate_raw = results["bulk_rate_data"]
    itemized_invoice_raw = results["itemized_invoice_data"]
    logger.debug(f"SFT {storenum:0>3}: Finished querying store")
  except NoConnectionError:
    logger.warning(f"SFT {storenum:0>3}: Failed to connect to store SQL server")
    return StoreScanData(storenum=storenum)

  except Exception as e:
    exc_type, exc_val, exc_tb = type(e), e, e.__traceback__
    # logger.error(
    #   f"SFT {storenum:0>3}: An exception occurred while querying store",
    #   exc_info=(type(e), e, e.__traceback__),
    #   stack_info=True,
    # )
    if exc_type is not None:
      # if exc_type inhereits from Error or is Error
      if issubclass(exc_type, Error):
        sqlstate = exc_val.args[0]
        if exc_type is OperationalError and sqlstate == "08001":
          logger.debug(f"SFT {storenum:0>3}: Connection to store SQL server timed out")
        else:
          logger.error(
            f"SFT {storenum:0>3}: Failed to connect to store SQL server",
            exc_info=(exc_type, exc_val, exc_tb),
            stack_info=True,
          )
        return True
      elif exc_type is NoConnectionError:
        logger.error(
          f"SFT {storenum:0>3}: Failed to connect to store SQL server",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )
        return True
      else:
        logger.error(
          f"SFT {storenum:0>3}: An unexpected exception occurred while connecting to store SQL server",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )
    return StoreScanData(storenum=storenum)

  bulk_rate_raw = [tuple(row) for row in bulk_rate_raw]
  itemized_invoice_raw = [tuple(row) for row in itemized_invoice_raw]

  bulk_rate_data = DataFrame(
    bulk_rate_raw,
    dtype=object,
    # dtype=str,
    columns=BulkRateCols.init_columns(),
  )

  itemized_invoice_data = DataFrame(
    itemized_invoice_raw,
    dtype=object,
    # dtype=str,
    columns=ItemizedInvoiceCols.init_columns(),
  )

  return StoreScanData(
    storenum=storenum,
    bulk_rate_data=bulk_rate_data,
    itemized_invoice_data=itemized_invoice_data,
  )


# TODO: Gather list of stores from google sheets
DEFAULT_STORES_LIST = [
  1,
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
  58,
  59,
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

# TEMP_STORES_LIST = [14]


# @cached_for_testing
def query_all_stores_multithreaded(
  queries: QueryDict, storenums: list[StoreNum] = DEFAULT_STORES_LIST
) -> list[StoreScanData]:
  store_querying_futures: list[Future] = []

  pbar = Progress(
    BarColumn(),
    TaskProgressColumn(),
    MofNCompleteColumn(),
    TextColumn("[progress.description]{task.description}"),
    console=rich_console,
  )
  items = {storenum: f"{storenum:0>3}" for storenum in storenums}
  remaining_pbar = Progress(
    TableColumn("Store Queries Remaining", items),
    console=rich_console,
  )

  remaining_task = remaining_pbar.add_task("", total=len(storenums))

  display_table = Table.grid()
  display_table.add_row(remaining_pbar, pbar)

  with Live(
    display_table,
    console=rich_console,
    transient=True,
  ) as _:
    with ThreadPoolExecutor(
      max_workers=len(storenums),
    ) as executor:
      store_querying_futures.extend(
        executor.submit(
          get_store_data,
          storenum=storenum,
          queries=queries,
        )
        for storenum in storenums
      )
      scan_data = []

      store_querying_task = pbar.add_task("Querying Stores for Scan Data", total=len(storenums))
      for future in as_completed(store_querying_futures):
        try:
          result: StoreScanData = future.result()
          logger.debug(f"SFT {result.storenum:0>3}: {result}")

          bulk_found = result.bulk_rate_data is not None
          itemized_found = result.itemized_invoice_data is not None

          if not bulk_found:
            logger.warning(f"SFT {result.storenum:0>3}: Store bulk rate data is None")
          if not itemized_found:
            logger.warning(f"SFT {result.storenum:0>3}: Store itemized invoice data is None")

          if bulk_found and itemized_found:
            scan_data.append(result)
            remaining_pbar.update(remaining_task, advance=1, description=result.storenum)
            logger.info(
              f"SFT {result.storenum:0>3}: [bold green]Finished[/] getting store data",
              extra={"markup": True},
            )

        except Exception as e:
          logger.error(
            "An exception occurred while querying a store",
            exc_info=(type(e), e, e.__traceback__),
            stack_info=True,
          )
        pbar.update(store_querying_task, advance=1)

  return scan_data


if __name__ == "__main__":
  from datetime import timedelta

  from sql_query_builders import build_bulk_info_query, build_itemized_invoice_query
  from utils import get_last_sun

  last_sun = get_last_sun()
  start_date = last_sun - timedelta(days=8)
  queries: QueryDict = {
    "bulk_rate_data": build_bulk_info_query(),
    "itemized_invoice_data": build_itemized_invoice_query(start_date, last_sun),
  }

  with Progress(
    BarColumn(),
    TaskProgressColumn(),
    MofNCompleteColumn(),
    TextColumn("[progress.description]{task.description}"),
    console=rich_console,
  ) as pbar:
    get_store_data(
      storenum=14,
      queries=queries,
    )
