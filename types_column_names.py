if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from enum import auto
from logging import getLogger

from types_custom import ColNameEnum, classproperty

logger = getLogger(__name__)


class RJRScanHeaders(ColNameEnum):
  outlet_name = auto()
  outlet_number = auto()
  address_1 = auto()
  address_2 = auto()
  city = auto()
  state = auto()
  zip = auto()
  transaction_date = auto()
  market_basket_id = auto()
  scan_id = auto()
  register_id = auto()
  quantity = auto()
  price = auto()
  upc_code = auto()
  upc_description = auto()
  unit_of_measure = auto()
  promotion_flag = auto()
  outlet_multipack_flag = auto()
  outlet_multipack_quantity = auto()
  outlet_multipack_discount_amt = auto()
  acct_promo_name = auto()
  acct_discount_amt = auto()
  manufacturer_discount_amt = auto()
  pid_coupon = auto()
  pid_coupon_discount_amt = auto()
  manufacturer_multipack_flag = auto()
  manufacturer_multipack_quantity = auto()
  manufacturer_multipack_discount_amt = auto()
  manufacturer_promo_desc = auto()
  manufacturer_buydown_desc = auto()
  manufacturer_buydown_amt = auto()
  manufacturer_multipack_desc = auto()
  account_loyalty_id_number = auto()
  coupon_desc = auto()


class RJRNamesFinal(ColNameEnum):
  outlet_name = "outletname"
  outlet_number = "outletnumber"
  address_1 = "address1"
  address_2 = "address2"
  city = "city"
  state = "state"
  zip = "zip"
  transaction_date = "transactiondate"
  market_basket_id = "marketbasketid"
  scan_id = "scanid"
  register_id = "registerid"
  quantity = "quantity"
  price = "price"
  upc_code = "upccode"
  upc_description = "upcdescription"
  unit_of_measure = "unitofmeasure"
  promotion_flag = "promotionflag"
  outlet_multipack_flag = "outletmultipackflag"
  outlet_multipack_quantity = "outletmultipackquantity"
  outlet_multipack_discount_amt = "outletmultipackdiscountamt"
  acct_promo_name = "acctpromoname"
  acct_discount_amt = "acctdiscountamt"
  manufacturer_discount_amt = "manufacturerdiscountamt"
  pid_coupon = "pidcoupon"
  pid_coupon_discount_amt = "pidcoupondiscountamt"
  manufacturer_multipack_flag = "manufacturermultipackflag"
  manufacturer_multipack_quantity = "manufacturermultipackquantity"
  manufacturer_multipack_discount_amt = "manufacturermultipackdiscountamt"
  manufacturer_promo_desc = "manufacturerpromodesc"
  manufacturer_buydown_desc = "manufacturerbuydowndesc"
  manufacturer_buydown_amt = "manufacturerbuydownamt"
  manufacturer_multipack_desc = "manufacturermultipackdesc"
  account_loyalty_id_number = "accountloyaltyidnumber"
  coupon_desc = "coupondesc"


class PMUSAScanHeaders(ColNameEnum):
  AcountNumber = auto()
  WeekEndDate = auto()
  TransactionDate = auto()
  TransactionTime = auto()
  TransactionID = auto()
  StoreNumber = auto()
  StoreName = auto()
  StoreAddress = auto()
  StoreCity = auto()
  StoreState = auto()
  StoreZip = auto()
  Category = auto()
  ManufacturerName = auto()
  SKUCode = auto()
  UPCCode = auto()
  ItemDescription = auto()
  UnitMeasure = auto()
  QtySold = auto()
  ConsumerUnits = auto()
  MultiUnitIndicator = auto()
  TotalMultiUnitDiscountQty = auto()
  TotalMultiUnitDiscountAmt = auto()
  RetailerFundedDiscountName = auto()
  RetailerFundedDiscountAmt = auto()
  CouponDiscountName = auto()
  CouponDiscountAmt = auto()
  OtherManufacturerDiscountName = auto()
  OtherManufacturerDiscountAmt = auto()
  LoyaltyDiscountName = auto()
  LoyaltyDiscountAmt = auto()
  FinalSalesPrice = auto()
  StoreTelephone = auto()
  StoreContactName = auto()
  StoreContactEmail = auto()
  ProductGroupingCode = auto()
  ProductGroupingName = auto()
  LoyaltyIDNumber = auto()
  AdultTobConsumerPhoneNum = auto()
  AgeValidationMethod = auto()
  ManufacturerOfferName = auto()
  ManufacturerOfferAmt = auto()
  PurchaseType = auto()
  ReservedField43 = auto()
  ReservedField44 = auto()
  ReservedField45 = auto()
  DateTime = auto()


class ITGScanHeaders(ColNameEnum):
  pass


class ItemizedInvoiceCols(ColNameEnum):
  __init_include__ = [
    "Invoice_Number",
    "CustNum",
    "Phone_1",
    "AgeVerificationMethod",
    "AgeVerification",
    "LineNum",
    "Cashier_ID",
    "Station_ID",
    "ItemNum",
    "ItemName",
    "ItemName_Extra",
    "DiffItemName",
    "Dept_ID",
    "Unit_Type",
    "Unit_Size",
    "DateTime",
    "Quantity",
    "CostPer",
    "PricePer",
    "Tax1Per",
    "Inv_Cost",
    "Inv_Price",
    "Inv_Retail_Price",
    "Coupon_Flat_Percent",
    "origPricePer",
    "MixNMatchRate",
    "SalePricePer",
    "PricePerBeforeDiscount",
    "PriceChangedBy",
  ]
  Invoice_Number = auto()
  CustNum = auto()
  Phone_1 = auto()
  AgeVerificationMethod = auto()
  AgeVerification = auto()
  LineNum = auto()
  Cashier_ID = auto()
  Station_ID = auto()
  ItemNum = auto()
  ItemName = auto()
  ItemName_Extra = auto()
  DiffItemName = auto()
  Dept_ID = auto()
  Unit_Type = auto()
  Unit_Size = auto()
  DateTime = auto()
  Quantity = auto()
  CostPer = auto()
  PricePer = auto()
  Tax1Per = auto()
  Inv_Cost = auto()
  Inv_Price = auto()
  Inv_Retail_Price = auto()
  Coupon_Flat_Percent = auto()
  # Store_ID = auto()
  origPricePer = auto()
  MixNMatchRate = auto()
  SalePricePer = auto()
  PricePerBeforeDiscount = auto()
  PriceChangedBy = auto()
  Store_Number = auto()
  Store_Name = auto()
  Store_Address = auto()
  Store_City = auto()
  Store_State = auto()
  Store_Zip = auto()
  Store_Telephone = auto()
  Store_ContactName = auto()
  Store_ContactEmail = auto()
  Retail_Multipack_Quantity = auto()
  Retail_Multipack_Disc_Amt = auto()
  Acct_Promo_Name = auto()
  Acct_Discount_Amt = auto()
  PID_Coupon = auto()
  PID_Coupon_Discount_Amt = auto()
  Manufacturer_Multipack_Quantity = auto()
  Manufacturer_Multipack_Discount_Amt = auto()
  Manufacturer_Multipack_Desc = auto()
  Altria_Manufacturer_Multipack_Quantity = auto()
  Altria_Manufacturer_Multipack_Discount_Amt = auto()
  Manufacturer_Promo_Desc = auto()
  Manufacturer_Discount_Amt = auto()
  Manufacturer_Buydown_Desc = auto()
  Manufacturer_Buydown_Amt = auto()
  loyalty_disc_desc = auto()
  loyalty_disc_amt = auto()


class BulkRateCols(ColNameEnum):
  ItemNum = auto()
  Bulk_Price = auto()
  Bulk_Quan = auto()


class GSheetsStoreInfoCols(ColNameEnum):
  StoreNum = auto()
  Phone = auto()
  Address = auto()
  Address2 = auto()
  City = auto()
  State = auto()
  Zip = auto()
  Email = auto()


class GSheetsUnitsOfMeasureCols(ColNameEnum):
  UPC = auto()
  Item_Name = auto()
  Manufacturer = auto()
  Unit_of_Measure = auto()
  Quantity = auto()


class GSheetsVAPDiscountsCols(ColNameEnum):
  UPC = auto()
  Item_Name = auto()
  Manufacturer = auto()
  Discount_Amt = auto()
  Discount_Type = auto()


class GSheetsBuydownsCols(ColNameEnum):
  UPC = auto()
  State = auto()
  Item_Name = auto()
  Manufacturer = auto()
  Buydown_Desc = auto()
  Buydown_Amt = auto()


class GSheetsScannableCouponsCols(ColNameEnum):
  Coupon_UPC = auto()
  Coupon_Description = auto()
  Coupon_Provider = auto()
  Applicable_Departments = auto()
  Applicable_UPCs = auto()


class LazyEmployeesCols(ColNameEnum):
  EmployeeID = auto()
  FirstName = auto()  # TODO implement employee name lookup
  LastName = auto()  # TODO implement employee name lookup
  StoreNum = auto()
  InfractionCount = auto()
  LastInfractionDate = auto()
  FalseLoyaltyInfractioncount = auto()

  @classproperty
  def _index_col(cls):
    return cls.EmployeeID


class BadCustNumsCols(ColNameEnum):
  CustNum = auto()
  Ocurrences = auto()
  LastInfractionDate = auto()

  @classproperty
  def _index_col(cls):
    return cls.CustNum
