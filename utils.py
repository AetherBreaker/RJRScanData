if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import pickle
import sys
from datetime import date
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from ftplib import FTP
from functools import wraps
from hashlib import md5
from io import BufferedWriter
from json import load
from logging import getLogger
from os import get_terminal_size
from pathlib import Path
from re import match
from threading import Lock
from typing import Any, Callable, Sequence

from dateutil.relativedelta import MO, SU, relativedelta
from dateutil.utils import today
from numpy import nan
from rich.progress import Progress, TaskID
from types_custom import StoreNum

logger = getLogger(__name__)

CWD = Path.cwd()

FTP_CREDS_FILE = (CWD / __file__).with_name("ftp_creds.json")


# Runtime CONSTANTS
DECIMAL_MAX_DIGITS = Decimal("1.00")
TERMINAL_WIDTH = get_terminal_size().columns

if getattr(sys, "frozen", False):
  # clear pyinstaller deprecation warning
  sys.stdout.write("\033[1A\033[2K" * ((TERMINAL_WIDTH // 166) + 1))


def taskgen(progress: Progress, *args, **kwargs):
  def decorator(func: Callable):
    task = progress.add_task(*args, **kwargs)

    @wraps(wrapped=func)
    def wrapper(*args, **kwargs):
      result = func(*args, **kwargs)
      progress.update(task, advance=1)
      return result

    return wrapper

  return decorator


def taskgen_whencalled(
  progress: Progress,
  description: str,
  total: int,
  clear_when_finished=False,
  *args,
  **kwargs,
) -> Callable[[Callable], Callable]:
  def decorator[T: Callable[[Any], Any]](func: T) -> Callable[[Any], T]:
    def starts_task(*altargs, **altkwargs) -> T:
      kwargs.update(description=description, total=total)
      # If the task is started with different arguments, we override the original ones
      if altargs and args:
        final_args = (*altargs, *(args[len(altargs) :]))
      elif altargs:
        final_args = altargs
      else:
        final_args = args

      if altkwargs:
        kwargs.update(altkwargs)

      taskid = progress.add_task(*final_args, **kwargs)

      task = [task for task in progress.tasks if task.id == taskid][0]

      @wraps(wrapped=func)
      def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        progress.update(taskid, advance=1)
        if clear_when_finished and task.finished:
          progress.remove_task(taskid)
        return result

      return wrapper

    return starts_task

  return decorator


def decimal_converter[T: str](x: T) -> Decimal | T:
  try:
    return Decimal("".join(char for char in x if char.isdigit() or char in [".", "-"]))
  except Exception:
    return x


def unsafe_decimal_converter[T: str](x: T) -> Decimal | float | T:
  try:
    value = Decimal("".join(char for char in x if char.isdigit() or char in [".", "-"]))
  except InvalidOperation:
    value = nan
  finally:
    return value


def truncate_decimal(x: Decimal, exponent: Decimal = DECIMAL_MAX_DIGITS) -> Decimal:
  try:
    return Decimal(x).quantize(exponent, ROUND_FLOOR)
  except (InvalidOperation, TypeError):
    return x


def pbar_writer(
  pbar: Progress, task_id: TaskID, filestream: BufferedWriter
) -> Callable[[bytes], None]:
  def wrapped_writer(data: bytes):
    filestream.write(data)
    pbar.update(task_id, advance=len(data))

  return wrapped_writer


def advance_pbar(pbar: Progress, task_id: TaskID) -> Callable[[bytes], None]:
  def advance(data: bytes):
    pbar.update(task_id, advance=len(data))

  return advance


def login_ftp(ftp: FTP) -> None:
  with FTP_CREDS_FILE.open("r") as file:
    creds = load(file)
  ftp.connect(host=creds["FTP_HOST"], port=int(creds["FTP_PORT"]))
  ftp.login(user=creds["FTP_USER"], passwd=creds["FTP_PWD"])


def get_last_sun() -> date:
  now = today()
  return now + relativedelta(weekday=SU(-1))


def get_last_mon() -> date:
  now = today()
  return now + relativedelta(weekday=MO(-1))


def rjr_start_end_dates() -> tuple[date, date]:
  end_date = get_last_mon()
  start_date = end_date - relativedelta(weeks=1)
  return start_date, end_date


def pm_start_end_dates() -> tuple[date, date]:
  end_date = get_last_sun()
  start_date = end_date - relativedelta(weeks=1)
  return start_date, end_date


def get_full_dates() -> tuple[date, date]:
  rjr = rjr_start_end_dates()
  pm = pm_start_end_dates()

  start_date = min(rjr[0], pm[0])
  end_date = max(rjr[1], pm[1])
  return start_date, end_date


RESULTS_PICKLE_CACHE = CWD / "_testing_pickles"
if __debug__:  # noqa
  RESULTS_PICKLE_CACHE.mkdir(exist_ok=True)


def cached_for_testing(func: Callable):
  """
  decorator to pickle the results of a function to disk for testing purposes

  Arguments:
      func -- Func to cache the return of

  Returns:
      The unpickled result of the function if it exists, otherwise the result of the function
  """
  if not __debug__:
    return func

  @wraps(func)
  def wrapper(*args, **kwargs):
    # hash the function name and the arguments to get a unique filename
    arghash = md5(usedforsecurity=False)
    func_path = f"{func.__module__}.{func.__name__}"
    arghash.update(func_path.encode())
    for arg in args:
      if isinstance(arg, (str, int, float)):
        arghash.update(str(arg).encode())
      elif isinstance(arg, Sequence):
        for item in arg:
          if isinstance(item, (str, int, float)):
            arghash.update(str(item).encode())
          else:
            try:
              arghash.update(str(item).encode())
            except TypeError:
              pass
      else:
        try:
          arghash.update(str(arg).encode())
        except TypeError:
          pass

    for key, value in kwargs.items():
      arghash.update(key.encode())
      if isinstance(value, (str, int, float)):
        arghash.update(str(value).encode())
      elif isinstance(value, Sequence):
        for item in value:
          if isinstance(item, (str, int, float)):
            arghash.update(str(item).encode())
          else:
            try:
              arghash.update(str(item).encode())
            except TypeError:
              pass
      else:
        try:
          arghash.update(str(value).encode())
        except TypeError:
          pass

    filename = RESULTS_PICKLE_CACHE / f"{arghash.hexdigest()}.pickle"
    if filename.exists():
      with filename.open("rb") as file:
        return pickle.load(file)
    else:
      result = func(*args, **kwargs)
      with filename.open("wb") as file:
        pickle.dump(result, file)
      return result

  return wrapper


class SingletonType(type):
  def __new__(mcs, name, bases, attrs):
    cls = super(SingletonType, mcs).__new__(mcs, name, bases, attrs)
    cls.__shared_instance_lock__ = Lock()
    return cls

  def __call__(self, *args, **kwargs):
    with self.__shared_instance_lock__:
      try:
        return self.__shared_instance__
      except AttributeError:
        self.__shared_instance__ = super(SingletonType, self).__call__(*args, **kwargs)
        return self.__shared_instance__


def convert_storenum_to_str(storenum: StoreNum) -> str:
  return f"SFT{storenum:0>3}"


def convert_str_to_storenum(storenum: str) -> int:
  if matches := match(r"SFT(?P<StoreNum>\d{3})", storenum):
    return int(matches.group("StoreNum"))
  else:
    raise ValueError(f"Invalid Store Number: {storenum}")


def upce_to_upca(upce):
  """Test value 04182635 -> 041800000265"""
  if len(upce) == 6:
    middle_digits = upce  # assume we're getting just middle 6 digits
  elif len(upce) == 7:
    # truncate last digit, assume it is just check digit
    middle_digits = upce[:6]
  elif len(upce) == 8:
    # truncate first and last digit,
    # assume first digit is number system digit
    # last digit is check digit
    middle_digits = upce[1:7]
  else:
    return False
  d1, d2, d3, d4, d5, d6 = list(middle_digits)
  if d6 in ["0", "1", "2"]:
    mfrnum = d1 + d2 + d6 + "00"
    itemnum = f"00{d3}{d4}{d5}"
  elif d6 == "3":
    mfrnum = d1 + d2 + d3 + "00"
    itemnum = f"000{d4}{d5}"
  elif d6 == "4":
    mfrnum = d1 + d2 + d3 + d4 + "0"
    itemnum = f"0000{d5}"
  else:
    mfrnum = d1 + d2 + d3 + d4 + d5
    itemnum = f"0000{d6}"
  newmsg = f"0{mfrnum}{itemnum}"
  # calculate check digit, they are the same for both UPCA and UPCE
  check_digit = 0
  odd_pos = True
  for char in str(newmsg)[::-1]:
    check_digit += int(char) * 3 if odd_pos else int(char)
    odd_pos = not odd_pos  # alternate
  check_digit = check_digit % 10
  check_digit = 10 - check_digit
  check_digit = check_digit % 10
  return newmsg + str(check_digit)
