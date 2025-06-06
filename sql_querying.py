if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import inspect
import json
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from itertools import product
from logging import INFO, getLogger
from pathlib import Path
from threading import Lock
from typing import Literal, cast

from config import SETTINGS
from logging_config import RICH_CONSOLE
from pandas import DataFrame
from pyodbc import Connection, Error, OperationalError, connect
from rich_custom import LiveCustom
from sql_query_builders import update_database_name
from types_custom import (
  ColNameEnum,
  QueryDict,
  QueryName,
  QueryPackage,
  QueryResultsPackage,
  SQLCreds,
  SQLHostName,
  StoreNum,
  StoreResultsPackage,
)
from utils import DoNotCacheException, cached_for_testing, get_week_of

logger = getLogger(__name__)
logger.setLevel(INFO)


CWD = Path.cwd()


CUR_WEEK = get_week_of(SETTINGS.week_shift)

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
DIRECT_IP_ADDRESS_LOOKUP_JSON = (CWD / __file__).with_name("store_ip_address_lookup.json")


def get_store_sql_hostname(
  storenum: StoreNum,
  hostname_lookup_method: Literal["DNS", "IP"] = SQL_HOSTNAME_METHODS[SQL_HOSTNAME_METHOD_DEFAULT],
) -> SQLHostName:
  match hostname_lookup_method:
    case "DNS":
      return TAILSCALE_HOSTNAME_PATTERN.format(storenum=storenum)
    case "IP":
      with DIRECT_IP_ADDRESS_LOOKUP_JSON.open("r") as file:
        ip_address_lookup = json.load(file)
        if str(storenum) not in ip_address_lookup:
          raise ValueError(f"Store {storenum} IP address not found in lookup table")
      return ip_address_lookup[str(storenum)]["ip"]


def get_db_name_override(storenum: StoreNum) -> str | Literal[False]:
  with DIRECT_IP_ADDRESS_LOOKUP_JSON.open("r") as file:
    db_name = json.load(file).get(str(storenum), None)

  return False if db_name is None else db_name["dbname"]


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
    try:
      self.hostname = get_store_sql_hostname(storenum, hostname_lookup_method="IP")
    except ValueError:
      self.hostname = get_store_sql_hostname(storenum, hostname_lookup_method="DNS")

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
          self.hostname = get_store_sql_hostname(self.storenum, hostname_lookup_method="DNS")
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
def query_store(storenum: StoreNum, queries: QueryDict) -> QueryResultsPackage:
  static_queries = {}
  results = QueryResultsPackage(storenum=storenum)
  with StoreSQLConn(storenum) as conn:
    with conn.cursor() as cursor:
      if not (db_name := get_db_name_override(storenum)):
        db_name = cursor.execute(LAST_USED_DB_QUERYFILE.read_text()).fetchone()[0]
      with _QUERY_THREADING_LOCK:
        update_database_name(db_name)

        for query_name, (query, _) in queries.items():
          static_queries[query_name] = query.get_sql()

      for query_name, query in static_queries.items():
        logger.info(f"SFT {storenum:0>3}: Querying {query_name} data")
        results[query_name] = cursor.execute(query).fetchall()

  return results


@cached_for_testing(date_for_sig=CUR_WEEK)
def get_store_data(
  storenum: StoreNum,
  queries: QueryDict,
) -> StoreResultsPackage:  # sourcery skip: raise-from-previous-error
  is_caching = inspect.stack()[1][3] == "caching_wrapper"
  empty_return = StoreResultsPackage(storenum=storenum)

  logger.debug(
    f"SFT {storenum:0>3}: Getting Store Data",
  )
  try:
    query_results = query_store(storenum, queries)
    logger.debug(f"SFT {storenum:0>3}: Finished querying store")
  except NoConnectionError:
    logger.warning(f"SFT {storenum:0>3}: Failed to connect to store SQL server")
    logger.info(f"SFT {storenum:0>3}: Retrying connection to store SQL server")
    try:
      query_results = query_store(storenum, queries)
    except NoConnectionError:
      logger.warning(f"SFT {storenum:0>3}: Connection failed on retry")
      if is_caching:
        raise DoNotCacheException(f"Failed to connect to store {storenum} SQL server", intended_return=empty_return)
      else:
        return empty_return

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
      else:
        logger.error(
          f"SFT {storenum:0>3}: An unexpected exception occurred while connecting to store SQL server",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )
    if is_caching:
      raise DoNotCacheException(f"Failed to connect to store {storenum} SQL server", intended_return=empty_return)
    else:
      return empty_return

  store_data = {}

  for query_name, query_result in query_results.items():
    if not query_result:
      logger.warning(f"SFT {storenum:0>3}: No results found for {query_name} query")
      if is_caching:
        raise DoNotCacheException(f"Failed to connect to store {storenum} SQL server", intended_return=empty_return)
      else:
        return empty_return

    raw = [tuple(row) for row in query_result]

    if not (cols := queries.get(query_name).cols):
      logger.warning(f"SFT {storenum:0>3}: No columns found for {query_name} query")

      # Do not cache if query fails to return a result. This allows for queries failed
      # due to unstable connections to be retried without needing to requery ALL stores
      if is_caching:
        raise DoNotCacheException(f"Failed to connect to store {storenum} SQL server", intended_return=empty_return)
      else:
        return empty_return

    if not isinstance(cols, list) and issubclass(cols, ColNameEnum):
      cols = cols.init_columns()

    store_data[query_name] = DataFrame(
      raw,
      dtype=object,
      # dtype=str,
      columns=cols,
    )

  return StoreResultsPackage(
    storenum=storenum,
    data=store_data,
  )


DEFAULT_STORES_LIST = [
  # 1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  # 9,
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
  # 37,
  38,
  40,
  42,
  43,
  # 44,
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
  # 62,
  # 63,
  # 64,
  65,
  # 66,
  # 67,
  82,
  84,
  85,
  86,
  88,
]


# DEFAULT_STORES_LIST = [
#   44,
#   59,
#   66,
# ]


if SETTINGS.testing_stores:
  DEFAULT_STORES_LIST = SETTINGS.testing_stores


# @cached_for_testing(
#   pickle_path_override=f"query_all_stores_multithreaded_bulk_rates_invoices_{str(cur_week.date())}_{"_".join(str(x) for x in DEFAULT_STORES_LIST)}"
# )
def query_all_stores_multithreaded[q_name: QueryName](
  queries: dict[q_name, QueryPackage], storenums: list[StoreNum] = DEFAULT_STORES_LIST
) -> dict[q_name, dict[StoreNum, DataFrame]]:
  store_querying_futures: list[Future] = []

  items = {storenum: storenum for storenum in storenums}

  with LiveCustom(
    console=RICH_CONSOLE,
    transient=True,
  ) as live:
    pbar = live.pbar

    updaters = live.init_remaining(
      *((items, f"{query_name.capitalize().replace("_", " ")} Queries") for query_name in queries.keys())
    )

    updaters = updaters if isinstance(updaters, tuple) else (updaters,)

    remaining_updaters = {query_name: updater for query_name, updater in zip(queries.keys(), updaters)}
    with ThreadPoolExecutor(
      max_workers=len(storenums) * len(queries),
    ) as executor:
      for (query_name, query_package), storenum in product(queries.items(), storenums):
        future = executor.submit(
          get_store_data,
          storenum=storenum,
          queries={query_name: query_package},
        )
        store_querying_futures.append(future)

      query_results: dict[q_name, dict[StoreNum, DataFrame]] = {query_name: {} for query_name in queries.keys()}

      store_querying_task = pbar.add_task("Querying Stores for Scan Data", total=len(storenums))
      for future in as_completed(store_querying_futures):
        try:
          result = cast(StoreResultsPackage, future.result())
          storenum = result.storenum
          logger.debug(f"SFT {storenum:0>3}: {result}")

          if result:
            for query_name, query_result in result.items():
              container = query_results.setdefault(query_name, {})
              container[storenum] = query_result
              remaining_updaters[query_name](storenum)
              logger.info(
                f"SFT {storenum:0>3}: Finished getting {query_name}",
                extra={"markup": True},
              )

        except Exception as e:
          logger.error(
            "An exception occurred while querying a store",
            exc_info=(type(e), e, e.__traceback__),
            stack_info=True,
          )
        if all(storenum in container for container in query_results.values()):
          pbar.update(store_querying_task, advance=1)

  return query_results
