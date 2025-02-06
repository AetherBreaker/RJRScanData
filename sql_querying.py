if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import json
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from decimal import Decimal
from logging import getLogger
from pathlib import Path
from threading import Lock
from typing import Literal

from pandas import DataFrame, concat
from pyodbc import Connection, Error, OperationalError, connect
from rich.progress import (
  BarColumn,
  MofNCompleteColumn,
  Progress,
  TaskProgressColumn,
  TextColumn,
)

from dataframe_transformations import apply_addrinfo_and_initial_validation, apply_model_to_df
from dataframe_utils import NULL_VALUES
from logging_config import rich_console
from sql_query_builders import (
  update_database_name,
)
from types_column_names import BulkRateCols, ItemizedInvoiceCols
from types_custom import (
  AddressInfoType,
  DeptIDsEnum,
  QueryDict,
  QueryResultsDict,
  ScanDataPackage,
  SQLCreds,
  SQLHostName,
  StoreNum,
  StoreScanData,
)
from utils import cached_for_testing, convert_storenum_to_str, taskgen_whencalled
from validation_other import BulkRateModel, ItemizedInvoiceModel

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
SQL_HOSTNAME_METHODS = ["DNS", "IP"]

TAILSCALE_HOSTNAME_PATTERN = "sft{storenum:0>3}.woodpecker-roach.ts.net"
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
      return ip_address_lookup[str(storenum)]


class StoreSQLConn:
  _conn_string_template = (
    "DRIVER={driver};"
    "SERVER={hostname},1433;"
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
        timeout=5,
      )

      if conn is None:
        raise NoConnectionError(f"Failed to connect to store {self.storenum} SQL server")

    except Exception as e:
      exc_type, exc_val, exc_tb = type(e), e, e.__traceback__
      # if exc_type inhereits from Error or is Error
      sqlstate = exc_val.args[0]
      if (exc_type is OperationalError and sqlstate == "08001") or exc_type is NoConnectionError:
        logger.debug(
          f"Connection to store {self.storenum} SQL server timed out",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )
        self.hostname = get_store_sql_hostname(self.storenum, hostname_lookup_method="IP")
        try:
          conn = connect(
            self._conn_string_template.format(
              driver=self.cred_data["DRIVER"],
              hostname=self.hostname,
              uid=self.cred_data["UID"],
              pwd=self.cred_data["PWD"],
            ),
            readonly=True,
            timeout=5,
          )
        except Exception as e:
          raise NoConnectionError(f"Failed to connect to store {self.storenum} SQL server") from e

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
          logger.debug(f"Connection to store {self.storenum} SQL server timed out")
        else:
          logger.error(
            f"Failed to connect to store {self.storenum} SQL server",
            exc_info=(exc_type, exc_val, exc_tb),
            stack_info=True,
          )
        return True
      elif exc_type is NoConnectionError:
        logger.error(
          f"Failed to connect to store {self.storenum} SQL server",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )
        return True
      else:
        logger.error(
          f"An unexpected exception occurred while connecting to store {self.storenum} SQL server",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )

    if hasattr(self, "conn"):
      self.conn.close()


_QUERY_THREADING_LOCK = Lock()


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


def fix_decimals(x: Decimal) -> Decimal:
  return Decimal("0.00") if isinstance(x, Decimal) and str(x) == "0E-8" else x


def get_store_data(
  pbar: Progress,
  storenum: StoreNum,
  addr_info: AddressInfoType,
  errors: list,
  queries: QueryDict,
) -> StoreScanData:
  try:
    results = query_store(storenum, queries)
    bulk_rate_raw = results["bulk_rate_data"]
    itemized_invoice_raw = results["itemized_invoice_data"]
    logger.info(f"Finished querying store {storenum:0>3}")
  except NoConnectionError:
    logger.warning(f"Failed to connect to store {storenum} SQL server")
    return StoreScanData(storenum=storenum)

  except Exception as e:
    logger.error(
      f"An exception occurred while querying store {storenum}",
      exc_info=(type(e), e, e.__traceback__),
      stack_info=True,
    )
    return StoreScanData(storenum=storenum)

  bulk_rate_raw = [tuple(row) for row in bulk_rate_raw]
  itemized_invoice_raw = [tuple(row) for row in itemized_invoice_raw]

  bulk_rate_data = DataFrame(
    bulk_rate_raw,
    dtype=object,
    columns=BulkRateCols.init_columns(),
  )

  itemized_invoice_data = DataFrame(
    itemized_invoice_raw,
    dtype=object,
    columns=ItemizedInvoiceCols.init_columns(),
  )

  bulk_rate_data = bulk_rate_data.replace(NULL_VALUES, value=None)
  itemized_invoice_data = itemized_invoice_data.replace(NULL_VALUES, value=None)

  bulk_rate_data = bulk_rate_data.map(fix_decimals)
  itemized_invoice_data = itemized_invoice_data.map(fix_decimals)

  itemized_invoice_data[ItemizedInvoiceCols.Store_Number] = storenum
  itemized_invoice_data[ItemizedInvoiceCols.Store_Name] = convert_storenum_to_str(storenum)

  itemized_invoice_data.sort_values(ItemizedInvoiceCols.DateTime, inplace=True)

  # bulk_rate_data.to_csv(f"bulk_rate_data_{storenum}.csv", index=False)
  # itemized_invoice_data.to_csv(f"itemized_invoice_data_{storenum}.csv", index=False)

  bulk_rate_data = bulk_rate_data.apply(
    taskgen_whencalled(
      pbar,
      description=f"Validating {storenum:0>3} bulk rates",
      total=len(bulk_rate_data),
      clear_when_finished=True,
    )(apply_model_to_df)(),
    model=BulkRateModel,
    axis=1,
    result_type="broadcast",
  )
  logger.info(f"Finished validating {storenum:0>3} bulk rates")

  series_to_concat = []

  itemized_invoice_data.apply(
    taskgen_whencalled(
      pbar,
      description=f"Validating {storenum:0>3} itemized invoices",
      total=len(itemized_invoice_data),
      clear_when_finished=True,
    )(apply_addrinfo_and_initial_validation)(),
    axis=1,
    model=ItemizedInvoiceModel,
    errors=errors,
    new_rows=series_to_concat,
    addr_data=addr_info,
  )
  logger.info(f"Finished validating {storenum:0>3} itemized invoices")

  itemized_invoice_data = concat(series_to_concat, axis=1).T

  itemized_invoice_data = itemized_invoice_data[
    itemized_invoice_data[ItemizedInvoiceCols.Dept_ID].isin(DeptIDsEnum.all_columns())
  ]

  return StoreScanData(
    storenum=storenum,
    bulk_rate_data=bulk_rate_data,
    itemized_invoice_data=itemized_invoice_data,
  )


# TODO: Gather list of stores from google sheets
TEMP_STORES_LIST = [1, 3, 8, 9, 16, 17, 19, 29, 42]
# TEMP_STORES_LIST = [29]


def query_all_stores_multithreaded(
  queries: QueryDict,
  addr_info: AddressInfoType,
  errors: list,
  storenums: list[StoreNum] = TEMP_STORES_LIST,
) -> ScanDataPackage:
  store_querying_futures: list[Future] = []

  with Progress(
    BarColumn(),
    TaskProgressColumn(),
    MofNCompleteColumn(),
    TextColumn("[progress.description]{task.description}"),
    console=rich_console,
  ) as pbar:
    with (
      ThreadPoolExecutor(
        # max_workers=4,
      ) as executor
    ):
      store_querying_futures.extend(
        executor.submit(
          get_store_data,
          pbar=pbar,
          storenum=storenum,
          addr_info=addr_info,
          errors=errors,
          queries=queries,
        )
        for storenum in storenums
      )
      itemized_invoices = []
      bulk_rate_data = {}

      store_querying_task = pbar.add_task("Querying Stores for Scan Data", total=len(storenums))
      for future in as_completed(store_querying_futures):
        try:
          result: StoreScanData = future.result()
          logger.info(f"Finished getting store {result.storenum:0>3} data")
          logger.debug(f"{result}")
          if result.bulk_rate_data is not None:
            bulk_rate_data[result.storenum] = result.bulk_rate_data
          else:
            logger.warning(f"Store {result.storenum} bulk rate data is None")
          if result.itemized_invoice_data is not None:
            itemized_invoices.append(result.itemized_invoice_data)
          else:
            logger.warning(f"Store {result.storenum} itemized invoice data is None")
        except Exception as e:
          logger.error(
            "An exception occurred while querying a store",
            exc_info=(type(e), e, e.__traceback__),
            stack_info=True,
          )
        pbar.update(store_querying_task, advance=1)

  return ScanDataPackage(
    bulk_data=bulk_rate_data,
    itemized_invoice_data=concat(
      itemized_invoices,
      ignore_index=True,
    ),
  )


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
  get_store_data(1, queries)
