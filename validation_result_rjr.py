if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import datetime
from decimal import Decimal
from logging import getLogger
from typing import ClassVar, Literal, Optional

from pydantic import AfterValidator, AliasChoices, BeforeValidator, Field, ValidationInfo, computed_field, field_validator
from types_custom import PromoFlag, StatesEnum, StoreNum, UnitsOfMeasureEnum
from typing_extensions import Annotated
from utils import is_not_integer, truncate_decimal
from validation_config import CustomBaseModel, ReportingFieldInfo
from validators_shared import validate_unit_type

logger = getLogger(__name__)


PROMO_FIELDS = [
  "outlet_multipack_quantity",
  "outlet_multipack_discount_amt",
  "acct_promo_name",
  "acct_discount_amt",
  "manufacturer_discount_amt",
  "pid_coupon",
  "pid_coupon_discount_amt",
  "manufacturer_multipack_quantity",
  "manufacturer_multipack_discount_amt",
  "manufacturer_promo_desc",
  "manufacturer_multipack_desc",
  "coupon_desc",
]


class RJRValidationModel(CustomBaseModel):
  outlet_number: Annotated[StoreNum, Field(alias=AliasChoices("Store_ID", "Store_Number"))]
  address_1: Annotated[str, Field(alias="Store_Address")]
  address_2: Annotated[Optional[str], Field(alias="Store_Address2")] = None
  city: Annotated[str, Field(alias="Store_City")]
  state: Annotated[StatesEnum, Field(alias="Store_State")]
  zip: Annotated[str, Field(alias="Store_Zip")]
  transaction_date: Annotated[datetime, Field(alias="DateTime")]
  market_basket_id: Annotated[int, Field(alias="Invoice_Number")]
  scan_id: Annotated[int, Field(alias="LineNum")]
  register_id: Annotated[int, Field(alias="Station_ID")]
  quantity: Annotated[
    int,
    Field(alias="Quantity", le=100),
    ReportingFieldInfo(dont_report_if=is_not_integer, dont_remove_if=is_not_integer),
  ]
  # upc_code: Annotated[str, Field(alias="ItemNum", min_length=6, max_length=14, pattern=r"^[0-9]{6,14}$")]
  upc_code: Annotated[
    str,
    # Annotated[
    #   Annotated[str, BeforeValidator(partial(pad_to_length, length=12))]  # UPC-A must be exactly 12 characters
    #   | Annotated[str, BeforeValidator(partial(pad_to_length, length=8))],
    #   AfterValidator(validate_upc_checkdigit),
    # ]  # UPC-E must be exactly 8 characters
    # | Annotated[
    #   str,
    #   BeforeValidator(partial(pad_to_length, length=13)),
    #   AfterValidator(validate_ean),
    # ],  # EAN must be exactly 13 characters
    # BeforeValidator(upc_isdigit),
    # AfterValidator(check_num_sys_digit),
    # AfterValidator(validate_upc_checkdigit),
    Field(alias="ItemNum", pattern=r"^[0-9]+$"),
    ReportingFieldInfo(report_field=False),
  ]
  upc_description: Annotated[str, Field(alias="ItemName")]
  unit_of_measure: Annotated[UnitsOfMeasureEnum, BeforeValidator(validate_unit_type), Field(alias="Unit_Type")]
  outlet_multipack_quantity: Annotated[Annotated[int, Field(ge=1)] | None, Field(alias="Retail_Multipack_Quantity")] = None
  outlet_multipack_discount_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(gt=0, le=30)] | None,
    Field(alias="Retail_Multipack_Disc_Amt"),
  ] = None
  acct_promo_name: Annotated[Optional[str], Field(alias="Acct_Promo_Name")] = None
  acct_discount_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(le=30)] | None, Field(alias="Acct_Discount_Amt")
  ] = None
  manufacturer_discount_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(gt=0, le=30)] | None,
    Field(alias="Manufacturer_Discount_Amt"),
  ] = None
  pid_coupon: Annotated[Optional[str], Field(alias="PID_Coupon")] = None
  pid_coupon_discount_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(gt=0, le=30)] | None,
    Field(alias="PID_Coupon_Discount_Amt"),
  ] = None
  manufacturer_multipack_quantity: Annotated[
    Annotated[int, Field(ge=1)] | None,
    Field(alias=AliasChoices("Manufacturer_Multipack_Quantity", "Altria_Manufacturer_Multipack_Quantity")),
  ] = None
  manufacturer_multipack_discount_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(gt=0, le=30)] | None,
    Field(alias=AliasChoices("Manufacturer_Multipack_Discount_Amt", "Altria_Manufacturer_Multipack_Discount_Amt")),
  ] = None
  manufacturer_promo_desc: Annotated[Optional[str], Field(alias="Manufacturer_Promo_Desc")] = None
  manufacturer_buydown_desc: Annotated[Optional[str], Field(alias="Manufacturer_Buydown_Desc")] = None
  manufacturer_buydown_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(gt=0, le=30)] | None,
    Field(alias="Manufacturer_Buydown_Amt"),
  ] = None
  manufacturer_multipack_desc: Annotated[Optional[str], Field(alias="Manufacturer_Multipack_Desc")] = None
  account_loyalty_id_number: Annotated[
    Annotated[str, Field(pattern=r"^[0-9a-zA-Z]*$")] | None,
    Field(alias="CustNum"),
    ReportingFieldInfo(report_field=False),
  ] = None
  coupon_desc: Annotated[Optional[str], Field(alias="loyalty_disc_desc")] = None

  price: Annotated[Decimal, Field(alias="Inv_Price"), AfterValidator(truncate_decimal)]

  remove_bad_rows: ClassVar[bool] = True

  @field_validator("price", mode="before")
  @classmethod
  def discount_reported_if_noprice[InputT: str](cls, input: InputT, info: ValidationInfo) -> InputT:
    """Ensure that if a discount is reported, the price is also reported."""
    if input <= 0 and all(info.data.get(field) is None for field in PROMO_FIELDS):
      raise ValueError("Price must be greater than 0 if no discounts are reported.")
    return input

  @field_validator("manufacturer_multipack_desc", mode="before")
  @classmethod
  def multipack_desc_required_check[InputT: str](cls, input: InputT, info: ValidationInfo) -> InputT:
    """Ensure that if manufacturer multipack discount is present, the description is also provided."""
    if not input and info.data.get("manufacturer_multipack_discount_amt"):
      raise ValueError(
        "Manufacturer Multipack Description is required when Manufacturer Multipack Quantity or Discount is present."
      )
    return input

  @computed_field
  @property
  def outlet_name(self) -> str:
    return "Sweet Fire Tobacco Inc."

  @computed_field
  @property
  def promotion_flag(self) -> PromoFlag:
    # Y if any of the following attributes are not None
    promo_flags = [
      self.outlet_multipack_flag,
      self.manufacturer_multipack_flag,
    ]
    promo_fields = [
      self.outlet_multipack_quantity,
      self.outlet_multipack_discount_amt,
      self.acct_promo_name,
      self.acct_discount_amt,
      self.manufacturer_discount_amt,
      self.pid_coupon,
      self.pid_coupon_discount_amt,
      self.manufacturer_multipack_quantity,
      self.manufacturer_multipack_discount_amt,
      self.manufacturer_promo_desc,
      self.manufacturer_multipack_desc,
      self.coupon_desc,
    ]

    return "Y" if "Y" in promo_flags or any(field is not None for field in promo_fields) else "N"

  @computed_field
  @property
  def outlet_multipack_flag(self) -> PromoFlag:
    return "Y" if self.outlet_multipack_quantity is not None or self.outlet_multipack_discount_amt is not None else "N"

  @computed_field
  @property
  def manufacturer_multipack_flag(self) -> PromoFlag:
    return (
      "Y" if self.manufacturer_multipack_quantity is not None or self.manufacturer_multipack_discount_amt is not None else "N"
    )


class FTXRJRValidationModel(CustomBaseModel):
  """FTX RJR Validation Model."""

  outlet_number: Annotated[StoreNum, Field(alias=AliasChoices("Store_ID", "Store_Number"))]
  address_1: Annotated[str, Field(alias="Store_Address")]
  address_2: Annotated[Optional[str], Field(alias="Store_Address2")] = None
  city: Annotated[str, Field(alias="Store_City")]
  state: Annotated[StatesEnum, Field(alias="Store_State")]
  zip: Annotated[str, Field(alias="Store_Zip")]
  transaction_date: Annotated[datetime, Field(alias="DateTime")]
  market_basket_id: Annotated[int, Field(alias="Invoice_Number")]
  scan_id: Annotated[int, Field(alias="LineNum")]
  register_id: Annotated[int, Field(alias="Station_ID")]
  quantity: Annotated[
    int,
    Field(alias="Quantity", le=100),
    ReportingFieldInfo(dont_report_if=is_not_integer, dont_remove_if=is_not_integer),
  ]
  # upc_code: Annotated[str, Field(alias="ItemNum", min_length=6, max_length=14, pattern=r"^[0-9]{6,14}$")]
  upc_code: Annotated[
    str,
    # Annotated[
    #   Annotated[str, BeforeValidator(partial(pad_to_length, length=12))]  # UPC-A must be exactly 12 characters
    #   | Annotated[str, BeforeValidator(partial(pad_to_length, length=8))],
    #   AfterValidator(validate_upc_checkdigit),
    # ]  # UPC-E must be exactly 8 characters
    # | Annotated[
    #   str,
    #   BeforeValidator(partial(pad_to_length, length=13)),
    #   AfterValidator(validate_ean),
    # ],  # EAN must be exactly 13 characters
    # BeforeValidator(upc_isdigit),
    # AfterValidator(check_num_sys_digit),
    # AfterValidator(validate_upc_checkdigit),
    Field(alias="ItemNum", pattern=r"^[0-9]+$"),
    ReportingFieldInfo(report_field=False),
  ]
  upc_description: Annotated[str, Field(alias="ItemName")]
  unit_of_measure: Annotated[UnitsOfMeasureEnum, BeforeValidator(validate_unit_type), Field(alias="Unit_Type")]
  promotion_flag: Literal["Y", "N"]
  outlet_multipack_flag: Literal["Y", "N"]
  outlet_multipack_quantity: Annotated[Annotated[int, Field(ge=0)] | None, Field(alias="Retail_Multipack_Quantity")] = None
  outlet_multipack_discount_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(ge=0, le=30)] | None,
    Field(alias="Retail_Multipack_Disc_Amt"),
  ] = None
  acct_promo_name: Annotated[Optional[str], Field(alias="Acct_Promo_Name")] = None
  acct_discount_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(ge=0, le=30)] | None, Field(alias="Acct_Discount_Amt")
  ] = None
  manufacturer_discount_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(ge=0, le=30)] | None,
    Field(alias="Manufacturer_Discount_Amt"),
  ] = None
  pid_coupon: Annotated[Optional[str], Field(alias="PID_Coupon")] = None
  pid_coupon_discount_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(ge=0, le=30)] | None,
    Field(alias="PID_Coupon_Discount_Amt"),
  ] = None
  manufacturer_multipack_flag: Literal["Y", "N"]
  manufacturer_multipack_quantity: Annotated[
    Annotated[int, Field(ge=0)] | None,
    Field(alias=AliasChoices("Manufacturer_Multipack_Quantity", "Altria_Manufacturer_Multipack_Quantity")),
  ] = None
  manufacturer_multipack_discount_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(ge=0, le=30)] | None,
    Field(alias=AliasChoices("Manufacturer_Multipack_Discount_Amt", "Altria_Manufacturer_Multipack_Discount_Amt")),
  ] = None
  manufacturer_promo_desc: Annotated[Optional[str], Field(alias="Manufacturer_Promo_Desc")] = None
  manufacturer_buydown_desc: Annotated[Optional[str], Field(alias="Manufacturer_Buydown_Desc")] = None
  manufacturer_buydown_amt: Annotated[
    Annotated[Decimal, AfterValidator(truncate_decimal), Field(ge=0, le=100)] | None,
    Field(alias="Manufacturer_Buydown_Amt"),
  ] = None
  manufacturer_multipack_desc: Annotated[Optional[str], Field(alias="Manufacturer_Multipack_Desc")] = None
  account_loyalty_id_number: Annotated[
    Annotated[str, Field(pattern=r"^[0-9a-zA-Z]*$")] | None, Field(alias="CustNum"), ReportingFieldInfo(report_field=False)
  ] = None
  coupon_desc: Annotated[Optional[str], Field(alias="loyalty_disc_desc")] = None

  price: Annotated[Decimal, Field(alias="Inv_Price"), AfterValidator(truncate_decimal)]

  remove_bad_rows: ClassVar[bool] = False

  @field_validator("price", mode="after")
  @classmethod
  def discount_reported_if_noprice[InputT: str](cls, input: InputT, info: ValidationInfo) -> InputT:
    """Ensure that if a discount is reported, the price is also reported."""
    if input <= 0 and all(info.data.get(field) is None for field in PROMO_FIELDS):
      raise ValueError("Price must be greater than 0 if no discounts are reported.")
    return input

  @field_validator("manufacturer_multipack_desc", mode="before")
  @classmethod
  def multipack_desc_required_check[InputT: str](cls, input: InputT, info: ValidationInfo) -> InputT:
    """Ensure that if manufacturer multipack discount is present, the description is also provided."""
    if not input and info.data.get("manufacturer_multipack_discount_amt"):
      raise ValueError(
        "Manufacturer Multipack Description is required when Manufacturer Multipack Quantity or Discount is present."
      )
    return input

  @computed_field
  @property
  def outlet_name(self) -> str:
    return "Sweet Fire Tobacco Inc."
