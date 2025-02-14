if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from decimal import Decimal
from logging import getLogger
from typing import Annotated, Literal, Optional

from pydantic import BeforeValidator, ValidationInfo, field_validator
from types_custom import DiscountTypesEnum, StatesEnum, StoreNum, UnitsOfMeasureEnum
from validation_config import CustomBaseModel
from validators_shared import map_to_upca, strip_string_to_digits, validate_unit_type

logger = getLogger(__name__)


class StoreInfoModel(CustomBaseModel):
  StoreNum: StoreNum
  Phone: Annotated[Optional[int], BeforeValidator(strip_string_to_digits)]
  Address: Optional[str]
  Address2: Optional[str]
  City: Optional[str]
  State: Optional[StatesEnum]
  Zip: Optional[str]
  Email: Optional[str]


class UnitsOfMeasureModel(CustomBaseModel):
  UPC: Annotated[str, BeforeValidator(map_to_upca)]
  Item_Name: str
  Manufacturer: str
  Unit_of_Measure: Annotated[UnitsOfMeasureEnum, BeforeValidator(validate_unit_type)]
  Quantity: str


class VAPDiscountsModel(CustomBaseModel):
  UPC: Annotated[str, BeforeValidator(map_to_upca)]
  Item_Name: str
  Manufacturer: str
  Discount_Amt: Decimal
  Discount_Type: DiscountTypesEnum  # TODO Evaluate whether this should be string Literals


class BuydownsModel(CustomBaseModel):
  UPC: Annotated[str, BeforeValidator(map_to_upca)]
  State: str
  Item_Name: str
  Manufacturer: str
  Buydown_Desc: Optional[str]
  Buydown_Amt: Optional[Decimal]


class ScannableCouponsModel(CustomBaseModel):
  Coupon_UPC: Annotated[str, BeforeValidator(map_to_upca)]
  Coupon_Description: str
  Coupon_Provider: str
  Applicable_Departments: list[str]
  Applicable_UPCs: list[str]

  @field_validator("Applicable_Departments", "Applicable_UPCs", mode="before")
  @classmethod
  def str_to_list(
    cls, value: str, info: ValidationInfo, delimiter: Literal[",", "|", "\t"] = "|"
  ) -> list[str]:
    try:
      return [item.strip() for item in value.split(delimiter)]
    except AttributeError as e:
      raise ValueError(
        f"Value {value} is not a string and cannot be split into a list.", info
      ) from e


# buydown_field_pattern = compile(r"(?:Buydown_(?P<FieldType>Type|Amt)_)(?P<FieldNum>\d{1,2})")


# @field_validator(
#   "Buydown_Type_1",
#   "Buydown_Amt_1",
#   "Buydown_Type_2",
#   "Buydown_Amt_2",
#   "Buydown_Type_3",
#   "Buydown_Amt_3",
#   "Buydown_Type_4",
#   "Buydown_Amt_4",
#   "Buydown_Type_5",
#   "Buydown_Amt_5",
#   "Buydown_Type_6",
#   "Buydown_Amt_6",
#   "Buydown_Type_7",
#   "Buydown_Amt_7",
#   "Buydown_Type_8",
#   "Buydown_Amt_8",
#   "Buydown_Type_9",
#   "Buydown_Amt_9",
#   "Buydown_Type_10",
#   "Buydown_Amt_10",
#   mode="after",
# )
# @classmethod
# def validate_buydowns_paired[T: Optional[DiscountTypesEnum | Decimal]](
#   cls, value: T, info: ValidationInfo
# ) -> T:
#   field_match = buydown_field_pattern.match(info.field_name)
#   buydown_field_type = field_match.group("FieldType")
#   buydown_field_number = int(field_match.group("FieldNum"))

#   paired_field_name = (
#     f"Buydown_{'Amt' if buydown_field_type == 'Type' else 'Type'}_{buydown_field_number}"
#   )

#   if paired_field_name in info.data:
#     paired_field_value = info.data[paired_field_name]
#     # if value is not None, we must verify that it's paired field is ALSO not None
#     if value is not None and paired_field_value is None:
#       raise ValueError(
#         f"Buydown_{buydown_field_type}_{buydown_field_number} is set, but paired field {paired_field_name} is not set.\n"
#         " Both fields must be set together.\n"
#       )
#     elif value is None and paired_field_value is not None:
#       raise ValueError(
#         f"Buydown_{buydown_field_type}_{buydown_field_number} is not set, but paired field {paired_field_name} is set.\n"
#         " Both fields must be set together.\n"
#       )

#   return value
