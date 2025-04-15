if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import datetime
from decimal import Decimal
from logging import getLogger
from typing import Annotated, Literal, Optional

from pydantic import AfterValidator, BeforeValidator, Field
from types_custom import DeptIDsEnum, StatesEnum, StoreNum, UnitsOfMeasureEnum
from utils import truncate_decimal
from validation_config import CustomBaseModel
from validators_shared import (
  abs_decimal,
  clear_default_custnums,
  map_to_upca,
  strip_string_to_digits,
  validate_unit_type,
)

logger = getLogger(__name__)


class ItemizedInvoiceModel(CustomBaseModel):
  Invoice_Number: int
  CustNum: Annotated[
    Optional[str],
    # Field(pattern=r"^[0-9a-zA-Z]*$"),
    BeforeValidator(clear_default_custnums),
  ]
  Phone_1: Annotated[Optional[int], BeforeValidator(strip_string_to_digits)]
  AgeVerificationMethod: str
  AgeVerification: str
  LineNum: int
  Cashier_ID: str
  Station_ID: int
  ItemNum: Annotated[str, BeforeValidator(map_to_upca)]
  ItemName: str
  ItemName_Extra: Optional[str]
  DiffItemName: str
  Dept_ID: DeptIDsEnum
  Unit_Type: Annotated[UnitsOfMeasureEnum, BeforeValidator(validate_unit_type)]
  Unit_Size: int
  DateTime: datetime
  Quantity: int
  CostPer: Decimal
  PricePer: Decimal
  Tax1Per: Decimal
  Inv_Cost: Decimal
  Inv_Price: Annotated[Decimal, AfterValidator(abs_decimal)]
  Inv_Retail_Price: Decimal
  Coupon_Flat_Percent: Optional[Literal[0, 1]]
  origPricePer: Decimal
  MixNMatchRate: Optional[str]
  SalePricePer: Decimal
  PricePerBeforeDiscount: Decimal
  PriceChangedBy: str
  Store_Number: StoreNum
  Store_Name: str
  Store_Address: Annotated[Optional[str], Field(alias="Address")]
  Store_Address2: Annotated[Optional[str], Field(alias="Address2")]
  Store_City: Annotated[Optional[str], Field(alias="City")]
  Store_State: Annotated[Optional[StatesEnum], Field(alias="State")]
  Store_Zip: Annotated[Optional[str], Field(alias="Zip")]
  Store_Telephone: Annotated[
    Optional[int], BeforeValidator(strip_string_to_digits), Field(alias="Phone")
  ]
  Store_ContactName: Optional[str] = None
  Store_ContactEmail: Annotated[Optional[str], Field(alias="Email")]
  Retail_Multipack_Quantity: Optional[int] = None
  Retail_Multipack_Disc_Amt: Optional[Decimal] = None
  Acct_Promo_Name: Optional[str] = None
  Acct_Discount_Amt: Optional[Decimal] = None
  PID_Coupon: Optional[str] = None
  PID_Coupon_Discount_Amt: Annotated[Optional[Decimal], AfterValidator(truncate_decimal)] = None
  Manufacturer_Multipack_Quantity: Optional[int] = None
  Manufacturer_Multipack_Discount_Amt: Optional[Decimal] = None
  Manufacturer_Multipack_Desc: Optional[str] = None
  Altria_Manufacturer_Multipack_Quantity: Optional[int] = None
  Altria_Manufacturer_Multipack_Discount_Amt: Optional[Decimal] = None
  Manufacturer_Promo_Desc: Optional[str] = None
  Manufacturer_Discount_Amt: Optional[Decimal] = None
  Manufacturer_Buydown_Desc: Optional[str] = None
  Manufacturer_Buydown_Amt: Optional[Decimal] = None
  loyalty_disc_desc: Optional[str] = None
