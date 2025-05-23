if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import date, datetime, time
from decimal import Decimal
from logging import getLogger
from typing import Annotated, ClassVar, Optional

from dateutil.relativedelta import SA, relativedelta
from pydantic import (
  AfterValidator,
  AliasChoices,
  BeforeValidator,
  Field,
  FieldSerializationInfo,
  computed_field,
  field_serializer,
)
from types_column_names import ItemizedInvoiceCols, PMUSAScanHeaders
from types_custom import DeptIDsEnum, FTXDeptIDsEnum, StatesEnum, StoreNum, UnitsOfMeasureEnum
from utils import truncate_decimal
from validation_config import CustomBaseModel
from validators_shared import validate_unit_type

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


class PMUSAValidationModel(CustomBaseModel):
  TransactionID: Annotated[int, Field(alias="Invoice_Number")]
  StoreNumber: Annotated[StoreNum, Field(alias=AliasChoices("Store_ID", "Store_Number"))]
  StoreName: Annotated[str, Field(alias="Store_Name")]
  StoreAddress: Annotated[Optional[str], Field(alias="Store_Address")]
  StoreCity: Annotated[Optional[str], Field(alias="Store_City")]
  StoreState: Annotated[Optional[StatesEnum], Field(alias="Store_State")]
  StoreZip: Annotated[Optional[str], Field(alias="Store_Zip")]
  Category: Annotated[DeptIDsEnum, Field(alias="Dept_ID")]
  ManufacturerName: Annotated[Optional[str], Field(alias="ItemName_Extra")] = None
  SKUCode: Annotated[
    str, Field(alias="ItemNum", min_length=6, max_length=14, pattern=r"^[0-9]{6,14}$")
  ]
  UPCCode: Annotated[
    str, Field(alias="ItemNum", min_length=6, max_length=14, pattern=r"^[0-9]{6,14}$")
  ]
  ItemDescription: Annotated[str, Field(alias="ItemName")]
  UnitMeasure: Annotated[
    UnitsOfMeasureEnum, BeforeValidator(validate_unit_type), Field(alias="Unit_Type")
  ]
  QtySold: Annotated[int, Field(alias="Quantity", le=100)]
  ConsumerUnits: Annotated[
    int, BeforeValidator(default_consumerunits), Field(alias="Unit_Size", gt=0)
  ] = 1
  TotalMultiUnitDiscountQty: Annotated[
    Optional[int], Field(alias="Altria_Manufacturer_Multipack_Quantity")
  ] = None
  TotalMultiUnitDiscountAmt: Annotated[
    Optional[Decimal], Field(alias="Altria_Manufacturer_Multipack_Discount_Amt")
  ] = None
  RetailerFundedDiscountName: Annotated[Optional[str], Field(alias="Acct_Promo_Name")] = None
  RetailerFundedDiscountAmt: Annotated[Optional[Decimal], Field(alias="Acct_Discount_Amt")] = None
  CouponDiscountName: Optional[str] = None
  # CouponDiscountName: Annotated[str, Field(alias="Acct_Promo_Name")]
  CouponDiscountAmt: Optional[Decimal] = None
  # CouponDiscountAmt: Annotated[Decimal, Field(alias="Acct_Discount_Amt")]
  OtherManufacturerDiscountName: Optional[str] = None
  OtherManufacturerDiscountAmt: Optional[Decimal] = None
  LoyaltyDiscountName: Annotated[Optional[str], Field(alias="loyalty_disc_desc")] = None
  LoyaltyDiscountAmt: Annotated[Optional[Decimal], Field(alias="PID_Coupon_Discount_Amt")] = None
  StoreTelephone: Annotated[Optional[int], Field(alias="Store_Telephone")]
  StoreContactName: Optional[str] = None
  StoreContactEmail: Annotated[Optional[str], Field(alias="Store_ContactEmail")]
  ProductGroupingCode: Optional[str] = None
  ProductGroupingName: Optional[str] = None
  LoyaltyIDNumber: Annotated[Optional[str], Field(pattern=r"^[0-9a-zA-Z]*$", alias="CustNum")] = (
    None
  )
  AdultTobConsumerPhoneNum: Annotated[
    Optional[str], BeforeValidator(truncate_phonenum), Field(alias="Phone_1")
  ] = None
  AgeValidationMethod: Annotated[
    Optional[str], AfterValidator(map_age_validation_method), Field(alias="AgeVerificationMethod")
  ]
  ManufacturerOfferName: Optional[str] = None
  ManufacturerOfferAmt: Optional[str] = None
  PurchaseType: Optional[str] = None
  ReservedField43: Optional[str] = None
  ReservedField44: Optional[str] = None
  ReservedField45: Optional[str] = None

  DateTime: Annotated[Optional[datetime], Field(alias="DateTime", exclude=True)]
  Price_at_sale: Annotated[
    Decimal,
    AfterValidator(abs),
    Field(alias=AliasChoices("PricePer", "FinalSalesPrice"), exclude=True),
  ]
  special_TransactionDate: Annotated[
    Optional[date], BeforeValidator(fix_date), Field(alias="TransactionDate", exclude=True)
  ] = None
  special_TransactionTime: Annotated[
    Optional[time], Field(alias="TransactionTime", exclude=True)
  ] = None

  field_name_lookup: ClassVar[dict[ItemizedInvoiceCols, PMUSAScanHeaders]] = {
    ItemizedInvoiceCols.Invoice_Number: PMUSAScanHeaders.TransactionID,
    ItemizedInvoiceCols.Store_Number: PMUSAScanHeaders.StoreNumber,
    ItemizedInvoiceCols.Store_Name: PMUSAScanHeaders.StoreName,
    ItemizedInvoiceCols.Store_Address: PMUSAScanHeaders.StoreAddress,
    ItemizedInvoiceCols.Store_City: PMUSAScanHeaders.StoreCity,
    ItemizedInvoiceCols.Store_State: PMUSAScanHeaders.StoreState,
    ItemizedInvoiceCols.Store_Zip: PMUSAScanHeaders.StoreZip,
    ItemizedInvoiceCols.Dept_ID: PMUSAScanHeaders.Category,
    ItemizedInvoiceCols.ItemName_Extra: PMUSAScanHeaders.ManufacturerName,
    ItemizedInvoiceCols.ItemNum: PMUSAScanHeaders.UPCCode,
    ItemizedInvoiceCols.ItemName: PMUSAScanHeaders.ItemDescription,
    ItemizedInvoiceCols.Unit_Type: PMUSAScanHeaders.UnitMeasure,
    ItemizedInvoiceCols.Quantity: PMUSAScanHeaders.QtySold,
    ItemizedInvoiceCols.Unit_Size: PMUSAScanHeaders.ConsumerUnits,
    ItemizedInvoiceCols.Altria_Manufacturer_Multipack_Quantity: PMUSAScanHeaders.TotalMultiUnitDiscountQty,
    ItemizedInvoiceCols.Altria_Manufacturer_Multipack_Discount_Amt: PMUSAScanHeaders.TotalMultiUnitDiscountAmt,
    ItemizedInvoiceCols.Acct_Promo_Name: PMUSAScanHeaders.RetailerFundedDiscountName,
    ItemizedInvoiceCols.Acct_Discount_Amt: PMUSAScanHeaders.RetailerFundedDiscountAmt,
    ItemizedInvoiceCols.loyalty_disc_desc: PMUSAScanHeaders.LoyaltyDiscountName,
    ItemizedInvoiceCols.PID_Coupon_Discount_Amt: PMUSAScanHeaders.LoyaltyDiscountAmt,
    ItemizedInvoiceCols.Store_Telephone: PMUSAScanHeaders.StoreTelephone,
    ItemizedInvoiceCols.Store_ContactEmail: PMUSAScanHeaders.StoreContactEmail,
    ItemizedInvoiceCols.CustNum: PMUSAScanHeaders.LoyaltyIDNumber,
    ItemizedInvoiceCols.Phone_1: PMUSAScanHeaders.AdultTobConsumerPhoneNum,
    ItemizedInvoiceCols.AgeVerificationMethod: PMUSAScanHeaders.AgeValidationMethod,
  }

  # @field_validator("ConsumerUnits", mode="wrap")
  # @classmethod
  # def test_consumerunits_validation(
  #   cls, data: str, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
  # ):
  #   results = data
  #   try:
  #     results = handler(data)
  #   except Exception as e:
  #     exc_type, exc_val, exc_tb = type(e), e, e.__traceback__
  #     pass

  #   return results

  @computed_field
  @property
  def TransactionDate(self) -> date:
    if self.DateTime:
      return self.DateTime.date()
    elif self.special_TransactionDate:
      return self.special_TransactionDate

  @field_serializer("TransactionDate")
  def serialize_transaction_date(
    self, TransactionDate: date, info: FieldSerializationInfo
  ) -> Optional[str]:
    return TransactionDate.strftime("%Y%m%d") if TransactionDate else None

  @computed_field
  @property
  def TransactionTime(self) -> time:
    if self.DateTime:
      return self.DateTime.time()
    elif self.special_TransactionTime:
      return self.special_TransactionTime

  @computed_field
  @property
  def FinalSalesPrice(self) -> Decimal:
    """Calculate the final price."""
    return truncate_decimal(self.QtySold * self.Price_at_sale)

  @computed_field
  @property
  def AcountNumber(self) -> str:
    return ALTRIA_ACCOUNT_NUMBER

  @computed_field
  @property
  def WeekEndDate(self) -> date:
    return self.TransactionDate + relativedelta(weekday=SA(1))

  @field_serializer("WeekEndDate")
  def serialize_week_end_date(
    self, WeekEndDate: date, info: FieldSerializationInfo
  ) -> Optional[str]:
    return WeekEndDate.strftime("%Y%m%d") if WeekEndDate else None

  @computed_field
  @property
  def MultiUnitIndicator(self) -> str:
    return (
      "Y"
      if any(
        (self.TotalMultiUnitDiscountQty is not None, self.TotalMultiUnitDiscountAmt is not None)
      )
      else "N"
    )


class FTXPMUSAValidationModel(PMUSAValidationModel):
  """FTX PMUSA Validation Model."""

  Category: Annotated[FTXDeptIDsEnum, Field(alias="Dept_ID")]
  DateTime: Annotated[Optional[datetime], Field(default=None, alias="DateTime", exclude=True)]

  @computed_field
  @property
  def FinalSalesPrice(self) -> Decimal:
    """Calculate the final price."""
    return truncate_decimal(self.Price_at_sale)
