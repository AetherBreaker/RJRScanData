if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from logging import getLogger

from types_custom import StatesEnum
from validation_config import CustomBaseModel

logger = getLogger(__name__)


class PMUSAValidationModel(CustomBaseModel):
  AcountNumber: str
  WeekEndDate: str
  TransactionDate: str
  TransactionTime: str
  TransactionID: str
  StoreNumber: str
  StoreName: str
  StoreAddress: str
  StoreCity: str
  StoreState: StatesEnum
  StoreZip: str
  Category: str
  ManufacturerName: str
  SKUCode: str
  UPCCode: str
  ItemDescription: str
  UnitMeasure: str
  QtySold: str
  ConsumerUnits: str
  MultiUnitIndicator: str
  TotalMultiUnitDiscountQty: str
  TotalMultiUnitDiscountAmt: str
  RetailerFundedDiscountName: str
  RetailerFundedDiscountAmt: str
  CouponDiscountName: str
  CouponDiscountAmt: str
  OtherManufacturerDiscountName: str
  OtherManufacturerDiscountAmt: str
  LoyaltyDiscountName: str
  LoyaltyDiscountAmt: str
  FinalSalesPrice: str
  StoreTelephone: str
  StoreContactName: str
  StoreContactEmail: str
  ProductGroupingCode: str
  ProductGroupingName: str
  LoyaltyIDNumber: str
  AdultTobConsumerPhoneNum: str
  AgeValidationMethod: str
  ManufacturerOfferName: str
  ManufacturerOfferAmt: str
  PurchaseType: str
  ReservedField43: str
  ReservedField44: str
  ReservedField45: str
