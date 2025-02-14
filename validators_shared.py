from decimal import Decimal
from typing import Any

from types_custom import UnitsOfMeasureEnum
from utils import upce_to_upca


def validate_unit_type(v: Any):
  # TODO Implement validation error processing and stop suppressing unit type errors
  # check if value is a valid unit of measure
  try:
    return UnitsOfMeasureEnum(v.capitalize())
  except (ValueError, AttributeError):
    return UnitsOfMeasureEnum.EACH
  # if v is None:
  #   return UnitsOfMeasure.EACH
  # elif isinstance(v, str):
  #   return v.capitalize()
  # else:
  #   return v


def strip_string_to_digits(v: str) -> str:
  return "".join(filter(str.isdigit, v)) if isinstance(v, str) else v


def abs_decimal(v: Decimal) -> Decimal:
  return v.copy_abs()


def map_to_upca(x):
  """Apply function to each row"""
  itemnum = x

  if not isinstance(itemnum, str) or not itemnum.isdigit():
    return x

  itemnum = itemnum.zfill(8)

  return upce_to_upca(itemnum) if len(itemnum) == 8 else itemnum
