if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()


from datetime import datetime
from logging import getLogger
from pathlib import Path
from threading import RLock

from pandas import DataFrame, Index, to_datetime
from pydantic import ValidationError
from types_column_names import BadCustNumsCols, ItemizedInvoiceCols, LazyEmployeesCols
from types_custom import ColNameEnum, ModelContextType
from utils import get_last_sun

logger = getLogger(__name__)


now = get_last_sun()


CWD = Path.cwd()


REPORTING_PARENT_FOLDER = CWD / "Validation Reports"
REPORTING_PARENT_FOLDER.mkdir(exist_ok=True)


REPORTING_FOLDER = REPORTING_PARENT_FOLDER / f"Week Ending {now:%m-%d-%Y}"
REPORTING_FOLDER.mkdir(exist_ok=True)


BAD_CUST_ID_REPORTS = REPORTING_FOLDER / "Bad Cust ID.csv"
if BAD_CUST_ID_REPORTS.exists():
  BAD_CUST_ID_REPORTS.unlink()
LAZY_EMPLOYEE_REPORTS = REPORTING_FOLDER / "Lazy Employee.csv"
if LAZY_EMPLOYEE_REPORTS.exists():
  LAZY_EMPLOYEE_REPORTS.unlink()

type TypeData = dict[str, type]


REPORTING_FILES_SETUP: dict[str, tuple[Path, TypeData, type[ColNameEnum]]] = {
  "bad_cust_id": (
    BAD_CUST_ID_REPORTS,
    {
      "Ocurrences": int,
      "LastInfractionDate": datetime,
    },
    BadCustNumsCols,
  ),
  "lazy_employee": (
    LAZY_EMPLOYEE_REPORTS,
    {
      "InfractionCount": int,
      "LastInfractionDate": datetime,
      "FalseLoyaltyInfractioncount": object,
    },
    LazyEmployeesCols,
  ),
}

reporting_files: dict[str, tuple[RLock, DataFrame]] = {}

shared_file_lock = RLock()


class LoadReportingFiles:
  def __enter__(self) -> None:
    for report_key, (file_loc, type_key, enum_type) in REPORTING_FILES_SETUP.items():
      with shared_file_lock:
        if file_loc.exists():
          file_loc.unlink()
        data = DataFrame(
          columns=enum_type.init_columns(),
          index=Index([], name=enum_type._index_col, dtype=str),
          dtype=str,
        )
        temp_row = []
        for col_name in enum_type.init_columns():
          val_type = type_key.get(col_name)
          if val_type == datetime:
            none_val = datetime.now()
          elif val_type is object or not val_type:
            none_val = None
          else:
            none_val = val_type(0)
          temp_row.append(none_val)
        data.loc["TEMP_ROW"] = temp_row

        for key, type_desig in type_key.items():
          if type_desig == datetime:
            data[enum_type(key)] = to_datetime(data[enum_type(key)])
          else:
            data[enum_type(key)] = data[enum_type(key)].astype(type_desig)

        reporting_files[report_key] = (RLock(), data)

  def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    with shared_file_lock:
      for report_key, (lock, df) in reporting_files.items():
        file_location = REPORTING_FILES_SETUP[report_key][0]

        with lock:
          df.drop("TEMP_ROW", inplace=True)
          df.to_csv(file_location, index=True)


class AccessReportingFile:
  def __init__(self, report_key: str):
    self.locked = False
    self.report_key = report_key
    with shared_file_lock:
      result = reporting_files.get(report_key)
    if not result:
      # files were not loaded during execution. Raise an error
      raise ValueError(f"File {report_key} was not loaded during execution")

    self.lock, self.df = result

  def __enter__(self) -> DataFrame:
    self.lock.acquire()
    self.locked = True
    return self.df

  def __exit__(self, exc_type, exc_val, exc_tb):
    file_location = REPORTING_FILES_SETUP[self.report_key][0]

    report_df = self.df.drop("TEMP_ROW")
    report_df.to_csv(file_location, index=True)
    if self.locked:
      self.lock.release()
      self.locked = False
    else:
      logger.warning(f"Lock was not acquired for {self.report_key} on exit")

  def __del__(self):
    if self.locked:
      self.lock.release()


custnum_err_priority_list = [
  "string_pattern_mismatch",
  "string_too_short",
]
reported_invnum_lock = RLock()
reported_invoice_nums: set[int] = set()


def report_errors(context: ModelContextType):
  for field_name, (field_value, err) in context["row_err"].items():
    err: ValidationError
    skip_fields = context.get("skip_fields", {})
    test_func = skip_fields.get(field_name)

    if_field_name = field_name in skip_fields

    cond = if_field_name and test_func(field_value) if test_func else if_field_name

    if not cond:
      match field_name:
        case "CustNum" | "account_loyalty_id_number" | "LoyaltyIDNumber":
          err_details = err.errors()

          handle_type_index = 9999
          # handle type should be whatever error type is highest in the priority list chosen from the error details
          for details in err_details:
            if details["type"] in custnum_err_priority_list:
              handle_type_index = min(
                custnum_err_priority_list.index(details["type"]), handle_type_index
              )
            else:
              logger.error(f"New custnum error type found! {details['type']}")
              return

          handle_type = custnum_err_priority_list[handle_type_index]

          match handle_type:
            case _:
              report_lazy_employee(context)
              invoicenum = context["input"][ItemizedInvoiceCols.Invoice_Number]
              storenum = context["input"][ItemizedInvoiceCols.Store_Number]

              invoice_id = f"{storenum}_{invoicenum}"
              with reported_invnum_lock:
                if invoice_id in reported_invoice_nums:
                  return
              report_bad_custid(context)

          with reported_invnum_lock:
            reported_invoice_nums.add(invoice_id)

        case "upc_code" | "SKUCode" | "UPCCode":
          pass

        case "quantity" | "QtySold":
          pass

        case _:
          exc_type, exc_val, exc_tb = type(err), err, err.__traceback__
          logger.error(
            f"Error validating {field_name} in {context['model'].__name__}: {err}",
            exc_info=(exc_type, exc_val, exc_tb),
            stack_info=True,
          )


def report_lazy_employee(context: ModelContextType):
  with AccessReportingFile("lazy_employee") as lazy_employees:
    employee_id = str(context["input"][ItemizedInvoiceCols.Cashier_ID])
    infraction_date = context["input"][ItemizedInvoiceCols.DateTime]

    loyalty_disc = context["input"][ItemizedInvoiceCols.PID_Coupon_Discount_Amt]

    loyalty_infraction = loyalty_disc or 0

    # check if the empoyee id is in the lazy employees index
    if employee_id in lazy_employees.index:
      last_infaction_date = lazy_employees.loc[employee_id, LazyEmployeesCols.LastInfractionDate]
      lazy_employees.loc[employee_id, LazyEmployeesCols.InfractionCount] += 1
      lazy_employees.loc[employee_id, LazyEmployeesCols.FalseLoyaltyInfractioncount] += (
        loyalty_infraction
      )
      if infraction_date > last_infaction_date:
        lazy_employees.loc[employee_id, LazyEmployeesCols.LastInfractionDate] = infraction_date
    else:
      # if employee id doesn't exist in the index, we need to add a new row for them
      lazy_employees.loc[employee_id] = {
        LazyEmployeesCols.InfractionCount: 1,
        LazyEmployeesCols.LastInfractionDate: infraction_date,
        LazyEmployeesCols.StoreNum: context["input"][ItemizedInvoiceCols.Store_Number],
        LazyEmployeesCols.FalseLoyaltyInfractioncount: loyalty_infraction,
      }

    global reported_invoice_nums
    invoicenum = context["input"][ItemizedInvoiceCols.Invoice_Number]
    storenum = context["input"][ItemizedInvoiceCols.Store_Number]
    invoice_id = f"{storenum}_{invoicenum}"
    with reported_invnum_lock:
      if invoice_id in reported_invoice_nums:
        if loyalty_infraction != 0:
          lazy_employees.loc[employee_id, LazyEmployeesCols.FalseLoyaltyInfractioncount] += 1
        return


def report_bad_custid(context: ModelContextType):
  with AccessReportingFile("bad_cust_id") as bad_custids:
    custnum = str(context["input"][ItemizedInvoiceCols.CustNum])
    infraction_date = context["input"][ItemizedInvoiceCols.DateTime]

    if custnum in bad_custids.index:
      last_infraction_date = bad_custids.loc[custnum, BadCustNumsCols.LastInfractionDate]
      bad_custids.loc[custnum, BadCustNumsCols.Ocurrences] += 1
      if infraction_date > last_infraction_date:
        bad_custids.loc[custnum, BadCustNumsCols.LastInfractionDate] = infraction_date
    else:
      bad_custids.loc[custnum] = {
        BadCustNumsCols.Ocurrences: 1,
        BadCustNumsCols.LastInfractionDate: infraction_date,
      }
