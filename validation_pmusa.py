if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import date, datetime, time
from decimal import Decimal
from logging import getLogger
from typing import Annotated, Optional

from dateutil.relativedelta import SA, relativedelta
from pydantic import (
  AfterValidator,
  AliasChoices,
  BeforeValidator,
  Field,
  computed_field,
)
from types_custom import DeptIDsEnum, StatesEnum, StoreNum, UnitsOfMeasureEnum
from utils import truncate_decimal
from validation_config import CustomBaseModel
from validators_shared import validate_unit_type

logger = getLogger(__name__)


ALTRIA_ACCOUNT_NUMBER = "77412"


def truncate_phonenum(phonenum: str) -> str:
  """Truncate phone number to last 6 digits."""
  if phonenum is not None:
    return phonenum[-6:] if len(phonenum) > 6 else phonenum
  else:
    return phonenum


class PMUSAValidationModel(CustomBaseModel):
  TransactionID: Annotated[int, Field(alias="Invoice_Number")]
  StoreNumber: Annotated[
    StoreNum,
    Field(
      alias=AliasChoices(
        "Store_ID",
        "Store_Number",
      )
    ),
  ]
  StoreName: Annotated[str, Field(alias="Store_Name")]
  StoreAddress: Annotated[Optional[str], Field(alias="Store_Address")]
  StoreCity: Annotated[Optional[str], Field(alias="Store_City")]
  StoreState: Annotated[Optional[StatesEnum], Field(alias="Store_State")]
  StoreZip: Annotated[Optional[str], Field(alias="Store_Zip")]
  Category: Annotated[DeptIDsEnum, Field(alias="Dept_ID")]
  ManufacturerName: Annotated[Optional[str], Field(alias="ItemName_Extra")] = None
  SKUCode: Annotated[
    str,
    Field(
      alias="ItemNum",
      min_length=6,
      max_length=14,
      # UPC codes must be numeric only
      pattern=r"^[0-9]{6,14}$",
    ),
  ]
  UPCCode: Annotated[
    str,
    Field(
      alias="ItemNum",
      min_length=6,
      max_length=14,
      # UPC codes must be numeric only
      pattern=r"^[0-9]{6,14}$",
    ),
  ]
  ItemDescription: Annotated[str, Field(alias="ItemName")]
  UnitMeasure: Annotated[
    UnitsOfMeasureEnum, BeforeValidator(validate_unit_type), Field(alias="Unit_Type")
  ]
  QtySold: Annotated[int, Field(alias="Quantity", le=100)]
  ConsumerUnits: Annotated[int, Field(alias="Unit_Size", gt=0)] = 1
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
  FinalSalesPrice: Annotated[Decimal, Field(alias="Inv_Price")]
  StoreTelephone: Annotated[Optional[int], Field(alias="Store_Telephone")]
  StoreContactName: Optional[str] = None
  StoreContactEmail: Annotated[Optional[str], Field(alias="Store_ContactEmail")]
  ProductGroupingCode: Optional[str] = None
  ProductGroupingName: Optional[str] = None
  LoyaltyIDNumber: Annotated[
    Optional[str],
    Field(pattern=r"^[0-9a-zA-Z]*$", alias="CustNum"),
  ]
  AdultTobConsumerPhoneNum: Annotated[
    Optional[str], AfterValidator(truncate_phonenum), Field(alias="Phone_1")
  ]
  AgeValidationMethod: Annotated[Optional[str], Field(alias="AgeVerificationMethod")]
  ManufacturerOfferName: Optional[str] = None
  ManufacturerOfferAmt: Optional[str] = None
  PurchaseType: Optional[str] = None
  ReservedField43: Optional[str] = None
  ReservedField44: Optional[str] = None
  ReservedField45: Optional[str] = None

  DateTime: Annotated[datetime, Field(alias="DateTime")]
  Inv_Price: Annotated[Decimal, Field(alias="Inv_Price", exclude=True)]

  @computed_field
  @property
  def TransactionDate(self) -> date:
    return self.DateTime.date()

  @computed_field
  @property
  def TransactionTime(self) -> time:
    return self.DateTime.time()

  @computed_field
  @property
  def calc_final_price(self) -> Decimal:
    """Calculate the final price."""
    return truncate_decimal(self.QtySold * self.Inv_Price)

  @computed_field
  @property
  def AcountNumber(self) -> str:
    return ALTRIA_ACCOUNT_NUMBER

  @computed_field
  @property
  def WeekEndDate(self) -> date:
    return self.TransactionDate + relativedelta(weekday=SA(1))

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
