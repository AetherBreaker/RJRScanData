from decimal import Decimal
from typing import Any

from types_custom import UnitsOfMeasureEnum


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
