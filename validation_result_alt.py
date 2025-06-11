if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import date, datetime, time
from decimal import Decimal
from functools import partial
from logging import getLogger
from typing import Annotated, ClassVar, Literal, Optional

from dateutil.relativedelta import SA, relativedelta
from pydantic import (
  AfterValidator,
  AliasChoices,
  BeforeValidator,
  Field,
  SkipValidation,
  ValidationInfo,
  computed_field,
  field_serializer,
  field_validator,
)
from types_custom import AltriaDeptsEnum, FTXDeptIDsEnum, ModelContextType, StatesEnum, StoreNum, UnitsOfMeasureEnum
from utils import is_not_integer, truncate_decimal
from validation_config import CustomBaseModel, ReportingFieldInfo
from validators_shared import pad_to_length, validate_ean, validate_unit_type, validate_upc_checkdigit

logger = getLogger(__name__)


ALTRIA_ACCOUNT_NUMBER = "77412"


def truncate_phonenum(phonenum: str) -> str:
  """Truncate phone number to last 6 digits."""
  if phonenum is None or not isinstance(phonenum, str):
    return None
  if len(phonenum) > 6 and phonenum.isdigit():
    return phonenum[-6:]
  elif len(phonenum) == 6 and phonenum.isdigit():
    return phonenum
  else:
    return None


def default_consumerunits(x: str) -> int:
  """Default ConsumerUnits to 1 if not provided."""
  return 1 if x is None or x == 0 else x


def fix_date(date: str) -> Optional[date]:
  """Fix date format."""
  if date is None or not date:
    return None
  try:
    return datetime.strptime(date, "%Y%m%d").date()
  except ValueError:
    return None


def map_age_validation_method(method: str) -> Optional[str]:
  """Map age validation method to a standard format."""
  return "Scanned ID" if method in {"1", 1} else None


class AltriaValidationModel(CustomBaseModel):
  TransactionID: Annotated[int, Field(alias="Invoice_Number")]
  StoreNumber: Annotated[StoreNum, Field(alias=AliasChoices("Store_ID", "Store_Number"))]
  StoreName: Annotated[str, Field(alias="Store_Name")]
  StoreAddress: Annotated[Optional[str], Field(alias="Store_Address")]
  StoreCity: Annotated[Optional[str], Field(alias="Store_City")]
  StoreState: Annotated[Optional[StatesEnum], Field(alias="Store_State")]
  StoreZip: Annotated[Optional[str], Field(alias="Store_Zip")]
  Category: Annotated[AltriaDeptsEnum, Field(alias="Dept_ID"), ReportingFieldInfo(report_field=False)]
  ManufacturerName: Annotated[Optional[str], Field(alias="ItemName_Extra")] = None
  SKUCode: Annotated[SkipValidation[str], Field(alias="ItemNum")]
  # UPCCode: Annotated[str, Field(alias="ItemNum", min_length=6, max_length=14, pattern=r"^[0-9]+$")]
  UPCCode: Annotated[
    Annotated[
      Annotated[str, BeforeValidator(partial(pad_to_length, length=12))]  # UPC-A must be exactly 12 characters
      | Annotated[str, BeforeValidator(partial(pad_to_length, length=8))],
      AfterValidator(validate_upc_checkdigit),
    ]  # UPC-E must be exactly 8 characters
    | Annotated[
      str,
      BeforeValidator(partial(pad_to_length, length=13)),
      AfterValidator(validate_ean),
    ],  # EAN must be exactly 13 characters
    # BeforeValidator(upc_isdigit),
    # AfterValidator(check_num_sys_digit),
    # AfterValidator(validate_upc_checkdigit),
    Field(alias="ItemNum", pattern=r"^[0-9]+$"),
    ReportingFieldInfo(report_field=False),
  ]
  ItemDescription: Annotated[str, Field(alias="ItemName")]
  UnitMeasure: Annotated[UnitsOfMeasureEnum, BeforeValidator(validate_unit_type), Field(alias="Unit_Type")]
  QtySold: Annotated[
    int,
    Field(alias="Quantity", le=100),
    ReportingFieldInfo(dont_report_if=is_not_integer, dont_remove_if=is_not_integer),
  ]
  ConsumerUnits: Annotated[int, BeforeValidator(default_consumerunits), Field(alias="Unit_Size", gt=0)] = 1
  TotalMultiUnitDiscountQty: Annotated[Optional[int], Field(alias="Altria_Manufacturer_Multipack_Quantity")] = None
  TotalMultiUnitDiscountAmt: Annotated[Optional[Decimal], Field(alias="Altria_Manufacturer_Multipack_Discount_Amt")] = None
  RetailerFundedDiscountName: Annotated[Optional[str], Field(alias="Acct_Promo_Name")] = None
  RetailerFundedDiscountAmt: Annotated[Optional[Decimal], Field(alias="Acct_Discount_Amt")] = None
  CouponDiscountName: Optional[str] = None
  # CouponDiscountName: Annotated[str, Field(alias="Acct_Promo_Name")]
  CouponDiscountAmt: Optional[Decimal] = None
  # CouponDiscountAmt: Annotated[Decimal, Field(alias="Acct_Discount_Amt")]
  OtherManufacturerDiscountName: Optional[str] = None
  OtherManufacturerDiscountAmt: Optional[Decimal] = None
  LoyaltyDiscountName: Annotated[Optional[str], Field(alias="loyalty_disc_desc")] = None
  StoreTelephone: Annotated[Optional[int], Field(alias="Store_Telephone")]
  StoreContactName: Optional[str] = None
  StoreContactEmail: Annotated[Optional[str], Field(alias="Store_ContactEmail")]
  ProductGroupingCode: Optional[str] = None
  ProductGroupingName: Optional[str] = None
  LoyaltyIDNumber: Annotated[
    Optional[str],
    Field(pattern=r"^[0-9a-zA-Z]*$", alias="CustNum"),
    ReportingFieldInfo(report_field=False, remove_row_if_error=False),
  ] = None
  AdultTobConsumerPhoneNum: Annotated[Optional[str], BeforeValidator(truncate_phonenum), Field(alias="Phone_1")] = None
  AgeValidationMethod: Annotated[Optional[str], AfterValidator(map_age_validation_method), Field(alias="AgeVerificationMethod")]
  ManufacturerOfferName: Optional[str] = None
  ManufacturerOfferAmt: Optional[str] = None
  PurchaseType: Optional[str] = None
  ReservedField43: Optional[str] = None
  ReservedField44: Optional[str] = None
  ReservedField45: Optional[str] = None
  LoyaltyDiscountAmt: Annotated[Optional[Decimal], Field(alias="PID_Coupon_Discount_Amt")] = None

  DateTime: Annotated[Optional[datetime], Field(alias="DateTime", exclude=True)]
  Price_at_sale: Annotated[
    Decimal,
    AfterValidator(abs),
    Field(alias=AliasChoices("PricePer"), exclude=True),
  ]

  remove_bad_rows: ClassVar[bool] = True

  @field_validator("LoyaltyDiscountAmt", mode="after")
  @classmethod
  def find_removed_loyalty[InputT: str](cls, input: InputT, info: ValidationInfo) -> InputT:
    context: ModelContextType = info.context

    if input is not None and any(context["remove_row"].values()):
      reason_fields = ", ".join(field_name for field_name in context["row_err"].keys())
      raise ValueError(f"A row with valid loyalty discounts is being removed! Field(s) Causing Removal: [{reason_fields}]")
    return input

  @computed_field
  @property
  def AccountNumber(self) -> str:
    return ALTRIA_ACCOUNT_NUMBER

  @computed_field
  @property
  def WeekEndDate(self) -> date:
    return self.TransactionDate + relativedelta(weekday=SA(1))

  @field_serializer("WeekEndDate")
  def serialize_week_end_date(self, WeekEndDate: date) -> Optional[str]:
    return WeekEndDate.strftime("%Y%m%d")

  @computed_field
  @property
  def TransactionDate(self) -> date:
    if self.DateTime:
      return self.DateTime.date()
    elif self.special_TransactionDate:
      return self.special_TransactionDate

  @field_serializer("TransactionDate")
  def serialize_transaction_date(self, TransactionDate: date) -> Optional[str]:
    return TransactionDate.strftime("%Y%m%d")

  @computed_field
  @property
  def TransactionTime(self) -> time:
    if self.DateTime:
      return self.DateTime.time()
    elif self.special_TransactionTime:
      return self.special_TransactionTime

  @computed_field
  @property
  def MultiUnitIndicator(self) -> str:
    return "Y" if any((self.TotalMultiUnitDiscountQty is not None, self.TotalMultiUnitDiscountAmt is not None)) else "N"

  @computed_field
  @property
  def FinalSalesPrice(self) -> Decimal:
    """Calculate the final price."""
    return truncate_decimal(self.QtySold * self.Price_at_sale)


class FTXPMUSAValidationModel(CustomBaseModel):
  """FTX PMUSA Validation Model."""

  AccountNumber: Literal["77412"]
  WeekEndDate: Annotated[date, BeforeValidator(fix_date)]
  TransactionDate: Annotated[date, BeforeValidator(fix_date)]
  TransactionTime: time
  TransactionID: int
  StoreNumber: StoreNum
  StoreName: str
  StoreAddress: Optional[str]
  StoreCity: Optional[str]
  StoreState: Optional[StatesEnum]
  StoreZip: Optional[str]
  Category: FTXDeptIDsEnum
  ManufacturerName: Optional[str] = None
  SKUCode: str
  UPCCode: Annotated[
    Annotated[
      Annotated[str, BeforeValidator(partial(pad_to_length, length=12))]  # UPC-A must be exactly 12 characters
      | Annotated[str, BeforeValidator(partial(pad_to_length, length=8))],  # UPC-E must be exactly 8 characters
      AfterValidator(validate_upc_checkdigit),
    ]
    | Annotated[
      str, BeforeValidator(partial(pad_to_length, length=13)), AfterValidator(validate_ean)
    ],  # EAN-13 must be exactly 13 characters
    Field(pattern=r"^[0-9]+$"),
    ReportingFieldInfo(report_field=False),
  ]
  ItemDescription: str
  UnitMeasure: UnitsOfMeasureEnum
  QtySold: Annotated[int, Field(alias="Quantity", le=100)]
  ConsumerUnits: Annotated[int, Field(gt=0)] = 1
  MultiUnitIndicator: Literal["Y", "N"]
  TotalMultiUnitDiscountQty: Optional[int] = None
  TotalMultiUnitDiscountAmt: Optional[Decimal] = None
  RetailerFundedDiscountName: Optional[str] = None
  RetailerFundedDiscountAmt: Optional[Decimal] = None
  CouponDiscountName: Optional[str] = None
  CouponDiscountAmt: Optional[Decimal] = None
  OtherManufacturerDiscountName: Optional[str] = None
  OtherManufacturerDiscountAmt: Optional[Decimal] = None
  LoyaltyDiscountName: Optional[str] = None
  LoyaltyDiscountAmt: Optional[Decimal] = None
  FinalSalesPrice: Decimal
  StoreTelephone: Optional[int]
  StoreContactName: Optional[str] = None
  StoreContactEmail: Optional[str]
  ProductGroupingCode: Optional[str] = None
  ProductGroupingName: Optional[str] = None
  LoyaltyIDNumber: Optional[str] = None
  AdultTobConsumerPhoneNum: Optional[str] = None
  AgeValidationMethod: Optional[str]
  ManufacturerOfferName: Optional[str] = None
  ManufacturerOfferAmt: Optional[str] = None
  PurchaseType: Optional[str] = None
  ReservedField43: Optional[str] = None
  ReservedField44: Optional[str] = None
  ReservedField45: Optional[str] = None

  remove_bad_rows: ClassVar[bool] = True

  @field_serializer("WeekEndDate")
  def serialize_week_end_date(self, WeekEndDate: date) -> Optional[str]:
    return WeekEndDate.strftime("%Y%m%d")

  @field_serializer("TransactionDate")
  def serialize_transaction_date(self, TransactionDate: date) -> Optional[str]:
    return TransactionDate.strftime("%Y%m%d")
