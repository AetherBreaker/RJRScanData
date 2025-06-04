from decimal import Decimal
from typing import Any, Optional

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


def clear_default_custnums(x: str) -> Optional[str]:
  return None if x in {"101", 101} else x


def validate_upc_checkdigit[UPCT: str](upc: UPCT) -> UPCT:
  if upc is None:
    return upc
  digits = [int(i) for i in upc][:-1]

  odd_pos_list = digits[0::2]  # 1st, 3rd, 5th, etc
  even_pos_list = digits[1::2]  # 2nd, 4th, 6th, etc

  n = (sum(odd_pos_list) * 3) + sum(even_pos_list)
  rmndr = n % 10
  result = (10 - rmndr) if rmndr > 0 else 0

  if result != int(upc[-1]):
    raise ValueError(f"Invalid UPC-A check digit: {upc[-1]} != {result}")
  return upc


def pad_to_length[UPCT: str](upc: UPCT, length: int) -> UPCT:
  """Pads UPC-E to UPC-A format by adding leading zeros."""
  if upc is None:
    return upc
  if len(upc) == length:
    return upc
  elif len(upc) > length:
    # raise ValueError(f"UPC must be {length} characters long, got {len(upc)}")
    return upc
  else:
    return upc.zfill(length)


def check_num_sys_digit[UPCT: str](upc: UPCT) -> UPCT:
  if upc is None:
    return upc
  num_sys_digit = int(upc[0])
  match num_sys_digit:
    case 2:
      # if Confirm.ask(
      #   f"UPC-A with a leading 2 is reserved for items sold by variable weight.\n"
      #   f"Are you sure {upc} is the correct upc?"
      # ):
      #   return upc
      # else:
      raise ValueError("UPC-A with a leading 2 is reserved for items sold by variable weight.")
    case 3:
      # if Confirm.ask(
      #   f"UPC-A with a leading 3 is reserved for pharmaceuticals.\n"
      #   f"Are you sure {upc} is the correct upc?"
      # ):
      #   return upc
      # else:
      raise ValueError("UPC-A with a leading 3 is reserved for pharmaceuticals.")
    case 4:
      # if Confirm.ask(
      #   f"UPC-A with a leading 4 is usually used for loyalty cards or store coupons.\n"
      #   f"Are you sure {upc} is the correct upc?"
      # ):
      #   return upc
      # else:
      raise ValueError("UPC-A with a leading 4 is reserved for loyalty cards or store coupons.")
    case 5:
      # if Confirm.ask(
      #   f"UPC-A with a leading 5 is reserved for coupons.\n"
      #   f"Are you sure {upc} is the correct upc?"
      # ):
      #   return upc
      # else:
      raise ValueError("UPC-A with a leading 5 is reserved for coupons.")
    case _:
      return upc  # For other leading digits, no special handling is needed


def upc_isdigit[UPCT: str](upc: UPCT) -> UPCT:
  if upc is None:
    return upc
  if not upc.isdigit():
    raise ValueError(f"UPC must be numeric, got {upc}")
  return upc


def validate_ean[EANT: str](ean: EANT) -> EANT:
  check_digit = ean[-1]
  x = sum((3, 1)[i % 2] * int(n) for i, n in enumerate(reversed(ean[:-1])))
  result = (10 - x) % 10

  if str(result) != check_digit:
    raise ValueError(f"Invalid EAN check digit: {check_digit} != {result}")
  return ean
