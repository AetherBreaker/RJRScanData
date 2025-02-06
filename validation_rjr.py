if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import datetime
from decimal import Decimal
from logging import getLogger

from pydantic import AfterValidator, AliasChoices, BeforeValidator, Field, computed_field
from typing_extensions import Annotated

from sql_querying import StoreNum
from types_custom import Optional, PromoFlag, StatesEnum, UnitsOfMeasureEnum
from utils import truncate_decimal
from validation_config import CustomBaseModel
from validators_shared import validate_unit_type

logger = getLogger(__name__)


class RJRValidationModel(CustomBaseModel):
  outlet_number: Annotated[
    StoreNum, Field(alias=AliasChoices("Store_ID", "Store_Number"), pattern=r"[\w\d]+")
  ]
  address_1: Annotated[Optional[str], Field(alias="Store_Address")]
  address_2: Annotated[Optional[str], Field(alias="Store_Address2")]
  city: Annotated[Optional[str], Field(alias="Store_City")]
  state: Annotated[Optional[StatesEnum], Field(alias="Store_State")]
  zip: Annotated[Optional[str], Field(alias="Store_Zip")]
  transaction_date: Annotated[datetime, Field(alias="DateTime")]
  market_basket_id: Annotated[int, Field(alias="Invoice_Number")]
  scan_id: Annotated[int, Field(alias="LineNum")]
  register_id: Annotated[int, Field(alias="Station_ID")]
  quantity: Annotated[int, Field(alias="Quantity")]
  price: Annotated[Decimal, Field(alias="Inv_Price"), AfterValidator(truncate_decimal)]
  upc_code: Annotated[str, Field(alias="ItemNum")]
  upc_description: Annotated[str, Field(alias="ItemName")]
  unit_of_measure: Annotated[
    UnitsOfMeasureEnum, BeforeValidator(validate_unit_type), Field(alias="Unit_Type")
  ]
  outlet_multipack_quantity: Annotated[Optional[int], Field(alias="Retail_Multipack_Quantity")] = (
    None
  )
  outlet_multipack_discount_amt: Annotated[
    Optional[Decimal], Field(alias="Retail_Multipack_Disc_Amt")
  ] = None
  acct_promo_name: Annotated[Optional[str], Field(alias="Acct_Promo_Name")] = None
  acct_discount_amt: Annotated[Optional[Decimal], Field(alias="Acct_Discount_Amt")] = None
  manufacturer_discount_amt: Annotated[
    Optional[Decimal], Field(alias="Manufacturer_Discount_Amt")
  ] = None
  pid_coupon: Annotated[Optional[str], Field(alias="PID_Coupon")] = None
  pid_coupon_discount_amt: Annotated[Optional[Decimal], Field(alias="PID_Coupon_Discount_Amt")] = (
    None
  )
  manufacturer_multipack_quantity: Annotated[
    Optional[int], Field(alias="Manufacturer_Multipack_Quantity")
  ] = None
  manufacturer_multipack_discount_amt: Annotated[
    Optional[Decimal], Field(alias="Manufacturer_Multipack_Discount_Amt")
  ] = None
  manufacturer_promo_desc: Annotated[Optional[str], Field(alias="Manufacturer_Promo_Desc")] = None
  manufacturer_buydown_desc: Annotated[Optional[str], Field(alias="Manufacturer_Buydown_Desc")] = (
    None
  )
  manufacturer_buydown_amt: Annotated[
    Optional[Decimal], Field(alias="Manufacturer_Buydown_Amt")
  ] = None
  manufacturer_multipack_desc: Annotated[
    Optional[str], Field(alias="Manufacturer_Multipack_Desc")
  ] = None
  account_loyalty_id_number: Annotated[str, Field(alias="CustNum")]
  coupon_desc: Annotated[Optional[str], Field(alias="loyalty_disc_desc")] = None

  @computed_field
  @property
  def outlet_name(self) -> str:
    return f"SFT{self.outlet_number:0>3}"

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
    return (
      "Y"
      if self.outlet_multipack_quantity is not None
      or self.outlet_multipack_discount_amt is not None
      else "N"
    )

  @computed_field
  @property
  def manufacturer_multipack_flag(self) -> PromoFlag:
    return (
      "Y"
      if self.manufacturer_multipack_quantity is not None
      or self.manufacturer_multipack_discount_amt is not None
      else "N"
    )
