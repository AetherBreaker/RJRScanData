if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import datetime
from decimal import Decimal
from re import compile
from typing import Annotated, Optional

from pydantic import AfterValidator, BeforeValidator, Field, ValidationInfo, field_validator

from types_custom import DiscountTypesEnum, StatesEnum, StoreNum, UnitsOfMeasureEnum
from utils import truncate_decimal
from validation_config import CustomBaseModel
from validators_shared import strip_string_to_digits, validate_unit_type


class BulkRateModel(CustomBaseModel):
  ItemNum: str
  Store_ID: StoreNum
  Bulk_Price: Decimal
  Bulk_Quan: int


class ItemizedInvoiceModel(CustomBaseModel):
  Invoice_Number: int
  CustNum: str
  Phone_1: Annotated[Optional[int], BeforeValidator(strip_string_to_digits)]
  AgeVerificationMethod: str
  AgeVerification: str
  LineNum: int
  Cashier_ID: str
  Station_ID: int
  ItemNum: str
  ItemName: str
  ItemName_Extra: Optional[str]
  DiffItemName: str
  Dept_ID: str
  Unit_Type: Annotated[UnitsOfMeasureEnum, BeforeValidator(validate_unit_type)]
  DateTime: datetime
  Quantity: int
  CostPer: Annotated[Decimal, AfterValidator(truncate_decimal)]
  PricePer: Annotated[Decimal, AfterValidator(truncate_decimal)]
  Tax1Per: Annotated[Decimal, AfterValidator(truncate_decimal)]
  origPricePer: Annotated[Decimal, AfterValidator(truncate_decimal)]
  BulkRate: Optional[str]
  SalePricePer: Annotated[Decimal, AfterValidator(truncate_decimal)]
  PricePerBeforeDiscount: Annotated[Decimal, AfterValidator(truncate_decimal)]
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
  Manufacturer_Discount_Amt: Optional[Decimal] = None
  PID_Coupon: Optional[str] = None
  PID_Coupon_Discount_Amt: Optional[Decimal] = None
  Manufacturer_Multipack_Quantity: Optional[int] = None
  Manufacturer_Multipack_Discount_Amt: Optional[Decimal] = None
  Manufacturer_Promo_Desc: Optional[str] = None
  Manufacturer_Buydown_Desc: Optional[str] = None
  Manufacturer_Buydown_Amt: Optional[Decimal] = None
  Manufacturer_Multipack_Desc: Optional[str] = None
  Coupon_Desc: Optional[str] = None


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
  UPC: str
  Item_Name: str
  Manufacturer: str
  Unit_of_Measure: Annotated[UnitsOfMeasureEnum, BeforeValidator(validate_unit_type)]
  Quantity: str


class VAPDiscountsModel(CustomBaseModel):
  UPC: str
  Item_Name: str
  Manufacturer: str
  Discount_Amt: Decimal
  Discount_Type: DiscountTypesEnum  # TODO Evaluate whether this should be string Literals


buydown_field_pattern = compile(r"(?:Buydown_(?P<FieldType>Type|Amt)_)(?P<FieldNum>\d{1,2})")


class BuydownsModel(CustomBaseModel):
  UPC: str
  State: str
  Item_Name: str
  Manufacturer: str
  Buydown_Type_1: Optional[DiscountTypesEnum]
  Buydown_Amt_1: Optional[Decimal]
  Buydown_Type_2: Optional[DiscountTypesEnum]
  Buydown_Amt_2: Optional[Decimal]
  Buydown_Type_3: Optional[DiscountTypesEnum]
  Buydown_Amt_3: Optional[Decimal]
  Buydown_Type_4: Optional[DiscountTypesEnum]
  Buydown_Amt_4: Optional[Decimal]
  Buydown_Type_5: Optional[DiscountTypesEnum]
  Buydown_Amt_5: Optional[Decimal]
  Buydown_Type_6: Optional[DiscountTypesEnum]
  Buydown_Amt_6: Optional[Decimal]
  Buydown_Type_7: Optional[DiscountTypesEnum]
  Buydown_Amt_7: Optional[Decimal]
  Buydown_Type_8: Optional[DiscountTypesEnum]
  Buydown_Amt_8: Optional[Decimal]
  Buydown_Type_9: Optional[DiscountTypesEnum]
  Buydown_Amt_9: Optional[Decimal]
  Buydown_Type_10: Optional[DiscountTypesEnum]
  Buydown_Amt_10: Optional[Decimal]

  @field_validator(
    "Buydown_Type_1",
    "Buydown_Amt_1",
    "Buydown_Type_2",
    "Buydown_Amt_2",
    "Buydown_Type_3",
    "Buydown_Amt_3",
    "Buydown_Type_4",
    "Buydown_Amt_4",
    "Buydown_Type_5",
    "Buydown_Amt_5",
    "Buydown_Type_6",
    "Buydown_Amt_6",
    "Buydown_Type_7",
    "Buydown_Amt_7",
    "Buydown_Type_8",
    "Buydown_Amt_8",
    "Buydown_Type_9",
    "Buydown_Amt_9",
    "Buydown_Type_10",
    "Buydown_Amt_10",
    mode="after",
  )
  @classmethod
  def validate_buydowns_paired[T: Optional[DiscountTypesEnum | Decimal]](
    cls, value: T, info: ValidationInfo
  ) -> T:
    field_match = buydown_field_pattern.match(info.field_name)
    buydown_field_type = field_match.group("FieldType")
    buydown_field_number = int(field_match.group("FieldNum"))

    paired_field_name = (
      f"Buydown_{'Amt' if buydown_field_type == 'Type' else 'Type'}_{buydown_field_number}"
    )

    if paired_field_name in info.data:
      paired_field_value = info.data[paired_field_name]
      # if value is not None, we must verify that it's paired field is ALSO not None
      if value is not None and paired_field_value is None:
        raise ValueError(
          f"Buydown_{buydown_field_type}_{buydown_field_number} is set, but paired field {paired_field_name} is not set.\n"
          " Both fields must be set together.\n"
        )
      elif value is None and paired_field_value is not None:
        raise ValueError(
          f"Buydown_{buydown_field_type}_{buydown_field_number} is not set, but paired field {paired_field_name} is set.\n"
          " Both fields must be set together.\n"
        )

    return value
