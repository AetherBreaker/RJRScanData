if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()


from contextlib import contextmanager
from logging import getLogger
from pathlib import Path
from threading import Lock
from typing import Any, Generator

from pandas import DataFrame, read_csv, to_datetime
from types_column_names import ItemizedInvoiceCols, LazyEmployeesCols
from types_custom import ModelContextType
from utils import get_last_sun

logger = getLogger(__name__)


now = get_last_sun()


CWD = Path.cwd()


REPORTING_PARENT_FOLDER = CWD / "Validation Reports"
REPORTING_PARENT_FOLDER.mkdir(exist_ok=True)


REPORTING_FOLDER = REPORTING_PARENT_FOLDER / f"Week Ending {now:%m-%d-%Y}"
REPORTING_FOLDER.mkdir(exist_ok=True)


BAD_CUST_ID_REPORTS = REPORTING_FOLDER / "Bad Cust ID.csv"
LAZY_EMPLOYEE_REPORTS = REPORTING_FOLDER / "Lazy Employee.csv"


shared_file_lock = Lock()


@contextmanager
def load_lazy_employees() -> Generator[DataFrame, Any, None]:
  with shared_file_lock:
    if not LAZY_EMPLOYEE_REPORTS.exists():
      data = DataFrame(columns=LazyEmployeesCols.init_columns())
      data.set_index(LazyEmployeesCols.EmployeeID, inplace=True)
    else:
      data = read_csv(
        LAZY_EMPLOYEE_REPORTS,
        header=0,
        dtype=object,
        usecols=LazyEmployeesCols.init_columns(),
        index_col=LazyEmployeesCols.EmployeeID,
      )

    data[LazyEmployeesCols.InfractionCount] = data[LazyEmployeesCols.InfractionCount].astype(int)
    data[LazyEmployeesCols.LastInfractionDate] = to_datetime(
      data[LazyEmployeesCols.LastInfractionDate]
    )

    try:
      yield data
    finally:
      data.to_csv(
        LAZY_EMPLOYEE_REPORTS,
        index=True,
      )


def report_errors(context: ModelContextType):
  with load_lazy_employees() as lazy_employees:
    for field_name, (field_value, err) in context["row_err"].items():
      skip_fields = context.get("skip_fields", {})
      test_func = skip_fields.get(field_name)

      if_field_name = field_name in skip_fields

      cond = if_field_name and test_func(field_value) if test_func else if_field_name

      if not cond:
        match field_name:
          case "CustNum":
            employee_id = context["input"][ItemizedInvoiceCols.Cashier_ID]
            infraction_date = context["input"][ItemizedInvoiceCols.DateTime]
            # check if the empoyee id is in the lazy employees index

            if employee_id in lazy_employees.index:
              last_infaction_date = lazy_employees.loc[
                employee_id, LazyEmployeesCols.LastInfractionDate
              ]

              lazy_employees.loc[employee_id, LazyEmployeesCols.InfractionCount] += 1
              if infraction_date > last_infaction_date:
                lazy_employees.loc[employee_id, LazyEmployeesCols.LastInfractionDate] = (
                  infraction_date
                )
            else:
              # if employee id doesn't exist in the index, we need to add a new row for them
              lazy_employees.loc[employee_id] = {
                LazyEmployeesCols.InfractionCount: 1,
                LazyEmployeesCols.LastInfractionDate: infraction_date,
                LazyEmployeesCols.StoreNum: context["input"][ItemizedInvoiceCols.Store_Number],
              }

          case _:
            ...
  pass
