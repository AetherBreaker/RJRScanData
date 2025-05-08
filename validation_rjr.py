if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import datetime
from decimal import Decimal
from logging import getLogger
from typing import Optional

from pydantic import (
  AfterValidator,
  AliasChoices,
  BeforeValidator,
  Field,
  computed_field,
  create_model,
)
from types_column_names import ItemizedInvoiceCols, RJRScanHeaders
from types_custom import PromoFlag, StatesEnum, StoreNum, UnitsOfMeasureEnum
from typing_extensions import Annotated
from utils import truncate_decimal
from validation_config import CustomBaseModel
from validators_shared import validate_unit_type

logger = getLogger(__name__)


class RJRValidationModel(CustomBaseModel):
  outlet_number: Annotated[
    Optional[StoreNum], Field(alias=AliasChoices("Store_ID", "Store_Number"))
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
  quantity: Annotated[int, Field(alias="Quantity", le=100)]
  price: Annotated[Decimal, Field(alias="Inv_Price"), AfterValidator(truncate_decimal)]
  upc_code: Annotated[
    str, Field(alias="ItemNum", min_length=6, max_length=14, pattern=r"^[0-9]{6,14}$")
  ]
  upc_description: Annotated[str, Field(alias="ItemName")]
  unit_of_measure: Annotated[
    UnitsOfMeasureEnum, BeforeValidator(validate_unit_type), Field(alias="Unit_Type")
  ]
  outlet_multipack_quantity: Annotated[Optional[int], Field(alias="Retail_Multipack_Quantity")] = (
    None
  )
  outlet_multipack_discount_amt: Annotated[
    Optional[Decimal], Field(alias="Retail_Multipack_Disc_Amt"), AfterValidator(truncate_decimal)
  ] = None
  acct_promo_name: Annotated[Optional[str], Field(alias="Acct_Promo_Name")] = None
  acct_discount_amt: Annotated[
    Optional[Decimal], Field(alias="Acct_Discount_Amt"), AfterValidator(truncate_decimal)
  ] = None
  manufacturer_discount_amt: Annotated[
    Optional[Decimal], Field(alias="Manufacturer_Discount_Amt"), AfterValidator(truncate_decimal)
  ] = None
  pid_coupon: Annotated[Optional[str], Field(alias="PID_Coupon")] = None
  pid_coupon_discount_amt: Annotated[
    Optional[Decimal], Field(alias="PID_Coupon_Discount_Amt"), AfterValidator(truncate_decimal)
  ] = None
  manufacturer_multipack_quantity: Annotated[
    Optional[int],
    Field(
      alias=AliasChoices(
        "Manufacturer_Multipack_Quantity", "Altria_Manufacturer_Multipack_Quantity"
      )
    ),
  ] = None
  manufacturer_multipack_discount_amt: Annotated[
    Optional[Decimal],
    Field(
      alias=AliasChoices(
        "Manufacturer_Multipack_Discount_Amt", "Altria_Manufacturer_Multipack_Discount_Amt"
      )
    ),
    AfterValidator(truncate_decimal),
  ] = None
  manufacturer_promo_desc: Annotated[Optional[str], Field(alias="Manufacturer_Promo_Desc")] = None
  manufacturer_buydown_desc: Annotated[Optional[str], Field(alias="Manufacturer_Buydown_Desc")] = (
    None
  )
  manufacturer_buydown_amt: Annotated[
    Optional[Decimal], Field(alias="Manufacturer_Buydown_Amt"), AfterValidator(truncate_decimal)
  ] = None
  manufacturer_multipack_desc: Annotated[
    Optional[str], Field(alias="Manufacturer_Multipack_Desc")
  ] = None
  account_loyalty_id_number: Annotated[
    Optional[str], Field(pattern=r"^[0-9a-zA-Z]*$", alias="CustNum")
  ]
  coupon_desc: Annotated[Optional[str], Field(alias="loyalty_disc_desc")] = None

  _field_name_lookup = {
    ItemizedInvoiceCols.Store_Number: RJRScanHeaders.outlet_number,
    ItemizedInvoiceCols.Store_Address: RJRScanHeaders.address_1,
    ItemizedInvoiceCols.Store_Address2: RJRScanHeaders.address_2,
    ItemizedInvoiceCols.Store_City: RJRScanHeaders.city,
    ItemizedInvoiceCols.Store_State: RJRScanHeaders.state,
    ItemizedInvoiceCols.Store_Zip: RJRScanHeaders.zip,
    ItemizedInvoiceCols.DateTime: RJRScanHeaders.transaction_date,
    ItemizedInvoiceCols.Invoice_Number: RJRScanHeaders.market_basket_id,
    ItemizedInvoiceCols.LineNum: RJRScanHeaders.scan_id,
    ItemizedInvoiceCols.Station_ID: RJRScanHeaders.register_id,
    ItemizedInvoiceCols.Quantity: RJRScanHeaders.quantity,
    ItemizedInvoiceCols.Inv_Price: RJRScanHeaders.price,
    ItemizedInvoiceCols.ItemNum: RJRScanHeaders.upc_code,
    ItemizedInvoiceCols.ItemName: RJRScanHeaders.upc_description,
    ItemizedInvoiceCols.Unit_Type: RJRScanHeaders.unit_of_measure,
    ItemizedInvoiceCols.Retail_Multipack_Quantity: RJRScanHeaders.outlet_multipack_quantity,
    ItemizedInvoiceCols.Retail_Multipack_Disc_Amt: RJRScanHeaders.outlet_multipack_discount_amt,
    ItemizedInvoiceCols.Acct_Promo_Name: RJRScanHeaders.acct_promo_name,
    ItemizedInvoiceCols.Acct_Discount_Amt: RJRScanHeaders.acct_discount_amt,
    ItemizedInvoiceCols.Manufacturer_Discount_Amt: RJRScanHeaders.manufacturer_discount_amt,
    ItemizedInvoiceCols.PID_Coupon: RJRScanHeaders.pid_coupon,
    ItemizedInvoiceCols.PID_Coupon_Discount_Amt: RJRScanHeaders.pid_coupon_discount_amt,
    ItemizedInvoiceCols.Manufacturer_Multipack_Quantity: RJRScanHeaders.manufacturer_multipack_quantity,
    ItemizedInvoiceCols.Manufacturer_Multipack_Discount_Amt: RJRScanHeaders.manufacturer_multipack_discount_amt,
    ItemizedInvoiceCols.Manufacturer_Promo_Desc: RJRScanHeaders.manufacturer_promo_desc,
    ItemizedInvoiceCols.Manufacturer_Buydown_Desc: RJRScanHeaders.manufacturer_buydown_desc,
    ItemizedInvoiceCols.Manufacturer_Buydown_Amt: RJRScanHeaders.manufacturer_buydown_amt,
    ItemizedInvoiceCols.Manufacturer_Multipack_Desc: RJRScanHeaders.manufacturer_multipack_desc,
    ItemizedInvoiceCols.Altria_Manufacturer_Multipack_Quantity: RJRScanHeaders.manufacturer_multipack_quantity,
    ItemizedInvoiceCols.Altria_Manufacturer_Multipack_Discount_Amt: RJRScanHeaders.manufacturer_multipack_discount_amt,
    ItemizedInvoiceCols.CustNum: RJRScanHeaders.account_loyalty_id_number,
    ItemizedInvoiceCols.loyalty_disc_desc: RJRScanHeaders.coupon_desc,
  }

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


FTXRJRValidationModel = create_model(
  "FTXRJRValidationModel",
  __base__=RJRValidationModel,
  scan_id=Annotated[str, Field(alias="LineNum")],
)
