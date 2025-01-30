if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from enum import Enum
from logging import getLogger
from typing import Literal, NamedTuple, Optional, TypedDict

from pandas import DataFrame
from pyodbc import Row
from pypika.queries import QueryBuilder

logger = getLogger(__name__)


class UnitsOfMeasureEnum(Enum):
  PACK = "Pack"
  CARTON = "Carton"
  CASE = "Case"
  EACH = "Each"
  BOX = "Box"
  BAG = "Bag"
  TUB = "Tub"
  CANISTER = "Canister"
  KIT = "Kit"
  BOTTLE = "Bottle"
  CARTRIDGE = "Cartridge"
  TUBES = "Tubes"
  ROLL = "Roll"
  CAN = "Can"


class DiscountTypesEnum(Enum):
  FLAT = "Flat"
  PERCENTAGE = "Percentage"


class StatesEnum(Enum):
  MI = "MI"
  OH = "OH"
  IA = "IA"
  WI = "WI"


type StoreNum = int

type SQLPWD = str
type SQLUID = str
type SQLHostName = str
type SQLDriver = str

type BulkDataRaw = list[Row]
type ItemizedDataRaw = list[Row]

type BulkRateDataType = DataFrame
type ItemizedInvoiceDataType = DataFrame
type AddressInfoType = DataFrame
type VAPDataType = DataFrame
type BuydownsDataType = DataFrame
type UnitOfMeasureDataType = DataFrame


type PromoFlag = Literal["N", "Y"]


class QueryDict(TypedDict):
  bulk_rate_data: QueryBuilder
  itemized_invoice_data: QueryBuilder


class SQLCreds(TypedDict):
  DRIVER: SQLDriver
  UID: SQLUID
  PWD: SQLPWD


class ScanDataPackage(TypedDict):
  bulk_data: dict[StoreNum, BulkRateDataType]
  itemized_invoice_data: ItemizedInvoiceDataType


class StoreScanData(NamedTuple):
  storenum: StoreNum
  bulk_rate_data: Optional[BulkRateDataType] = None
  itemized_invoice_data: Optional[ItemizedInvoiceDataType] = None
