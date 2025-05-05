import contextlib

if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import pickle
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
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
from typing import Any

from dateutil.relativedelta import MO, SU, WE, relativedelta
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

RESULTS_PICKLE_CACHE = CWD / "_testing_pickles"
if __debug__:  # noqa
  RESULTS_PICKLE_CACHE.mkdir(exist_ok=True)

IGNORE_ARGTYPES = (Progress,)
IGNORE_KWARG_KEYS = ("errors", "buydowns_data", "vap_data", "live")

# if getattr(sys, "frozen", False):
#   # clear pyinstaller deprecation warning
#   sys.stdout.write("\033[1A\033[2K" * ((TERMINAL_WIDTH // 166) + 1))


def taskgen_whencalled(
  progress: Progress,
  description: str,
  total: int,
  clear_when_finished=False,
  *pbar_args,
  **pbar_kwargs,
):
  def decorator[**P, R, T: Callable[P, R]](func: T) -> Callable[[Any], T]:
    def starts_task(*altargs, **altkwargs) -> T:
      pbar_kwargs.update(description=description, total=total)
      # If the task is started with different arguments, we override the original ones
      if altargs and pbar_args:
        final_args = (*altargs, *(pbar_args[len(altargs) :]))
      elif altargs:
        final_args = altargs
      else:
        final_args = pbar_args

      if altkwargs:
        pbar_kwargs.update(altkwargs)

      taskid = progress.add_task(*final_args, **pbar_kwargs)

      task = [task for task in progress.tasks if task.id == taskid][0]

      @wraps(func)
      def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
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


def get_last_sun(additional_shift: int = 0) -> datetime:
  now = today()
  return now + relativedelta(weekday=SU(-1)) + relativedelta(weeks=additional_shift)


def get_last_mon(additional_shift: int = 0) -> datetime:
  now = today()
  return now + relativedelta(weekday=MO(-1)) + relativedelta(weeks=additional_shift)


def rjr_start_end_dates(additional_shift: int = 0) -> tuple[datetime, datetime]:
  end_date = get_last_mon(additional_shift)
  start_date = end_date - relativedelta(weeks=1)
  return start_date, end_date


def pm_start_end_dates(additional_shift: int = 0) -> tuple[datetime, datetime]:
  end_date = get_last_sun(additional_shift)
  start_date = end_date - relativedelta(weeks=1)
  return start_date, end_date


def get_full_dates(week_shift: int = 0) -> tuple[datetime, datetime]:
  rjr = rjr_start_end_dates(week_shift)
  pm = pm_start_end_dates(week_shift)

  start_date = min(rjr[0], pm[0])
  end_date = max(rjr[1], pm[1])
  return start_date, end_date


def get_week_of(week_shift: int = 0) -> datetime:
  """Returns the end date of the week for the current date"""
  now = today() + relativedelta(weeks=week_shift)
  return now + relativedelta(weekday=WE(0))  # Wednesday of target week


class DoNotCacheException[**P](Exception):
  """Exception to indicate that a function's return should not be cached by cached_for_testing"""

  def __init__(self, *args, intended_return: Any = None, **kwargs):
    self.__intended_return = intended_return
    super().__init__(*args)

  @property
  def intended_return(self) -> Any:
    """The intended return value of the function that raised this exception"""
    return self.__intended_return


def process_arg_signature(
  arg: Any, hash_list: list[Any], func: Callable, annotations: list | dict = None
):
  if func.__qualname__.split(".")[0] == arg.__class__.__qualname__:
    return
  if (
    annotations
    and hasattr(annotations, "__metadata__")
    and "ignore_for_sig" in annotations.__metadata__
  ):
    return
  if isinstance(arg, (str, int, float)):
    hash_list.append(arg)
  elif isinstance(arg, Mapping):
    for key, value in arg.items():
      if annotations and (anno := annotations.get(key)):
        if hasattr(anno, "__metadata__") and "ignore_for_sig" in anno.__metadata__:
          continue
      if (
        key in IGNORE_KWARG_KEYS
        or isinstance(key, IGNORE_ARGTYPES)
        or isinstance(value, IGNORE_ARGTYPES)
      ):
        continue
      process_arg_signature(key, hash_list, func)
      # else:
      #   with contextlib.suppress(TypeError):
      #     hash.update(str(key).encode())
      process_arg_signature(value, hash_list, func)
  elif isinstance(arg, Sequence):
    for index, item in enumerate(arg):
      if annotations:
        with contextlib.suppress(IndexError):
          anno = annotations[index]
          if (
            anno and hasattr(annotations, "__metadata__") and "ignore_for_sig" in anno.__metadata__
          ):
            continue
      if isinstance(item, IGNORE_ARGTYPES):
        continue
      process_arg_signature(item, hash_list, func)
  #     else:
  #       with contextlib.suppress(TypeError):
  #         hash.update(str(item).encode())
  # else:
  #   with contextlib.suppress(TypeError):
  #     hash.update(str(arg).encode())


def cached_for_testing[**TP, TR](
  _func: Callable[TP, TR] | None = None,
  *,
  pickle_path_override: str = None,
  hash_for_sig: bool = False,
  date_for_sig: datetime = None,
) -> Callable[TP, TR] | Callable[[Callable[TP, TR]], Callable[TP, TR]]:
  if pickle_path_override is not None:
    pickle_path_override = (
      pickle_path_override
      if pickle_path_override.endswith(".pickle")
      else pickle_path_override + ".pickle"
    )

  def cached_for_testing_under[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    """
    decorator to pickle the results of a function to disk for testing purposes

    Arguments:
        func -- Func to cache the return of

    Returns:
        The unpickled result of the function if it exists, otherwise the result of the function
    """
    # if not __debug__:
    #   return func

    pickle_path = RESULTS_PICKLE_CACHE / func.__module__
    pickle_path.mkdir(exist_ok=True)

    if hash_for_sig:
      arghash = md5(usedforsecurity=False)

    func_path = f"{func.__module__}.{func.__name__}"
    # arghash.update(func_path.encode())

    @wraps(func)
    def caching_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
      if pickle_path_override:
        filename = pickle_path / pickle_path_override
      else:
        # Process all args to form a unique function signature
        # hash the function name and the arguments to get a unique filename
        arg_anno = [v for k, v in func.__annotations__.items() if k not in kwargs and k != "return"]
        kwarg_anno = {
          k: v for k, v in func.__annotations__.items() if k in kwargs and k != "return"
        }

        hash_list = [func_path]

        process_arg_signature(args, hash_list, func, arg_anno)
        process_arg_signature(kwargs, hash_list, func, kwarg_anno)

        pass

        # create the filename from the hash
        if hash_for_sig:
          for item in hash_list:
            arghash.update(str(item).encode())
          filename = pickle_path / f"{func.__name__}_{arghash.hexdigest()}.pickle"
        else:
          strfixed = "_".join([str(item).replace(" ", "_").replace("-", "_") for item in hash_list])
          filename = (
            (pickle_path / f"{func.__name__}_{str(date_for_sig.date())}_{strfixed}.pickle")
            if date_for_sig is not None
            else pickle_path / f"{func.__name__}_{strfixed}.pickle"
          )
        pass

      # if the file exists, load it and return the result
      # otherwise, call the function and save the result to disk
      if filename.exists():
        with filename.open("rb") as file:
          return pickle.load(file)
      else:
        try:
          result = func(*args, **kwargs)
        except DoNotCacheException as e:
          return e.intended_return
        with filename.open("wb") as file:
          pickle.dump(result, file)
        return result

    return caching_wrapper

  return cached_for_testing_under if _func is None else cached_for_testing_under(_func)


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


def upce_to_upca(upce: str):
  """Test value 04182635 -> 041800000265"""
  if not upce.isdigit():
    return upce
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
    return upce
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
