if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from enum import Enum, auto
from logging import getLogger
from typing import Any, Callable, Literal, NamedTuple, Optional, TypedDict

from pandas import DataFrame
from pydantic import BaseModel, ValidationError
from pyodbc import Row
from pypika.queries import QueryBuilder
from types_column_names import ColNameEnum

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


class DeptIDsEnum(ColNameEnum):
  __exclude__ = [
    "Battery",
    "CigAcc",
    "CigTube",
    "Detox",
    "EcigAcc",
    "EcigCoil",
    "Ecigs",
    "FoodBev",
    "Fuel",
    "Furnish",
    "GWP",
    "GlassCln",
    "Grinder",
    "GrowSup",
    "Handpipe",
    "Herbal",
    "Hookahs",
    "Incenses",
    "Kits",
    "Lighter",
    "Novelty",
    "OWP",
    "OffSup",
    "PipeAcc",
    "PreHerb",
    "RetSup",
    "RollAcc",
    "RollMach",
    "Scales",
    "Scents",
    "Startup",
    "Storage",
    "T18Wrap",
    "TechSup",
    "Torches",
    "VapeLiq",
    "Zippo",
  ]
  __rjr_include__ = [
    "BDsBrand",
    "BDsLine",
    "BDsMisc",
    "BagTob",
    "ChewHelx",
    "ChewUSST",
    "Chews",
    "Chews18",
    "CigTube",
    "Cigar",
    "Cigs",
    "CigsMarl",
    "Coupon$",
    "Dispos",
    "FGIDisc",
    "HookaTob",
    "Hookahs",
    "PMCOUPON",
    "PMPromos",
    "PreHerb",
    "PreNJOY",
    "Prefills",
    "PromosLT",
    "PromosST",
    "T18Wrap",
    "T21JMC",
    "T21LCig",
    "T21Wrap",
    "VAPBOGO",
    "VAPBTGO",
    "VAPFlat",
    "VapeLiq",
  ]
  __pm_include__ = [
    "BDsBrand",
    "BDsLine",
    "BDsMisc",
    "ChewHelx",
    "ChewUSST",
    "Cigs",
    "CigsMarl",
    "Coupon$",
    "FGIDisc",
    "PMCOUPON",
    "PMPromos",
    "PreNJOY",
    "PromosLT",
    "PromosST",
    "T21JMC",
    "VAPBOGO",
    "VAPBTGO",
    "VAPFlat",
  ]
  BDsBrand = auto()
  BDsLine = auto()
  BDsMisc = auto()
  BagTob = auto()
  Battery = auto()
  ChewHelx = auto()
  ChewUSST = auto()
  Chews = auto()
  Chews18 = auto()
  CigAcc = auto()
  CigTube = auto()
  Cigar = auto()
  Cigs = auto()
  CigsMarl = auto()
  Coupon = "Coupon$"
  Detox = auto()
  Dispos = auto()
  EcigAcc = auto()
  EcigCoil = auto()
  Ecigs = auto()
  FGIDisc = auto()
  FoodBev = auto()
  Fuel = auto()
  Furnish = auto()
  GWP = auto()
  GlassCln = auto()
  Grinder = auto()
  GrowSup = auto()
  Handpipe = auto()
  Herbal = auto()
  HookaTob = auto()
  Hookahs = auto()
  Incenses = auto()
  Kits = auto()
  Lighter = auto()
  Novelty = auto()
  OWP = auto()
  OffSup = auto()
  PMCOUPON = auto()
  PMPromos = auto()
  PipeAcc = auto()
  PreHerb = auto()
  PreNJOY = auto()
  Prefills = auto()
  PromosLT = auto()
  PromosST = auto()
  RJRVAP = auto()
  RetSup = auto()
  RollAcc = auto()
  RollMach = auto()
  Scales = auto()
  Scents = auto()
  Startup = auto()
  Storage = auto()
  T18Wrap = auto()
  T21JMC = auto()
  T21LCig = auto()
  T21Wrap = auto()
  TechSup = auto()
  TobAcc = auto()
  Torches = auto()
  VAPBOGO = auto()
  VAPBTGO = auto()
  VAPFlat = auto()
  VapeLiq = auto()
  Zippo = auto()

  @classmethod
  def rjr_depts(cls) -> set[str]:
    return set(cls.__rjr_include__)

  @classmethod
  def pm_depts(cls) -> set[str]:
    return set(cls.__pm_include__)


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


class QueryResultsDict(TypedDict):
  bulk_rate_data: BulkDataRaw
  itemized_invoice_data: ItemizedDataRaw


class SQLCreds(TypedDict):
  DRIVER: SQLDriver
  UID: SQLUID
  PWD: SQLPWD


class ScanDataPackage(TypedDict):
  bulk_data: dict[StoreNum, BulkRateDataType]
  itemized_invoice_data: ItemizedInvoiceDataType


class ModelContextType(TypedDict):
  row_err: dict[str, "ValidationErrPackage"]
  skip_fields: dict[str, Callable | None]
  store_num: StoreNum
  input: dict[str, Any]
  row_id: int
  model: BaseModel


class ValidationErrPackage(NamedTuple):
  field_value: Any
  err: ValidationError


class StoreScanData(NamedTuple):
  storenum: StoreNum
  bulk_rate_data: Optional[BulkRateDataType] = None
  itemized_invoice_data: Optional[ItemizedInvoiceDataType] = None
