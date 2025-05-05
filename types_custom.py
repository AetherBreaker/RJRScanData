if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from collections import UserDict
from collections.abc import Callable
from enum import Enum, StrEnum, auto
from logging import getLogger
from typing import Any, Literal, NamedTuple, TypedDict

from pandas import DataFrame
from pydantic import BaseModel, ValidationError
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


class ColNameEnum(StrEnum):
  __exclude__ = []
  __init_include__ = []

  @classmethod
  def ordered_column_names(cls, *columns: list[str]) -> list[str]:
    columns = [str(column) for column in columns]
    return [str(column) for column in cls if str(column) in columns]

  @classmethod
  def all_columns(cls) -> list[str]:
    return [
      str(column)
      for column in cls
      if str(column) not in cls.__exclude__ and not str(column).startswith("_")
    ]

  @classmethod
  def init_columns(cls) -> list[str]:
    if not cls.__init_include__:
      return cls.all_columns()
    return [
      str(column)
      for column in cls
      if str(column) in cls.__init_include__ and not str(column).startswith("_")
    ]

  @classmethod
  def testing_columns(cls) -> list[str]:
    return [str(column) for column in cls if str(column) not in cls.__exclude__]

  @classmethod
  def true_all_columns(cls) -> list[str]:
    return [str(column) for column in cls]

  @staticmethod
  def _generate_next_value_(name, start, count, last_values):
    """
    Return the member name.
    """
    return name


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
    "ChewHusk",
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
    "HelxCoup",
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
    "USSTCoup",
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
    "ChewHusk",
    "ChewUSST",
    "Cigs",
    "CigsMarl",
    "Coupon$",
    "FGIDisc",
    "HelxCoup",
    "PMCOUPON",
    "PMPromos",
    "PreNJOY",
    "PromosLT",
    "PromosST",
    "T21JMC",
    "USSTCoup",
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
  ChewHusk = auto()
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
  HelxCoup = auto()
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
  USSTCoup = auto()
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


class FTXDeptIDsEnum(ColNameEnum):
  LittleCigars21 = "21+ Little Cigars"
  BagTobPouchCans = "Bag Tob/Pouch/Cans*"
  Batteries = "Batteries*"
  ButaneGas = "Butane/Gas*"
  CandlesandSprays = "Candles and Sprays*"
  Chew = "Chew*"
  CigarAccessories = "Cigar Accessories*"
  Cigar = "Cigar*"
  CigaretteTubes = "Cigarette Tubes*"
  Cigarettes = "Cigarettes"
  DetoxDrugTestFetishUrine = "Detox/Drug Test/Fetish Urine"
  Discontinued = "Discontinued"
  DisposableVapes = "Disposable Vapes"
  EcigCoilsPods = "Ecig Coils & Pods"
  ExtractionTubes = "Extraction Tubes*"
  FoodBeveragesPharmacy = "Food / Beverages / Pharmacy*"
  Furnishings = "Furnishings"
  GlassandPipeCleaners = "Glass and Pipe Cleaners"
  GlassWaterPipes = "Glass Water Pipes*"
  GrabBag = "Grab Bag*"
  Grinders = "Grinders*"
  HandPipes = "Hand Pipes*"
  HerbalSupplements = "Herbal Supplements*"
  HookahAccessories = "Hookah Accessories*"
  HookahTobacco = "Hookah Tobacco*"
  Hookahs = "Hookahs*"
  Incense = "Incense"
  KitsUnRegulated = "Kits UnRegulated*"
  LighterAccessories = "Lighter Accessories*"
  LightersMatches = "Lighters / Matches*"
  NitrusandAccessories = "Nitrus and Accessories"
  NoveltyConsumable = "Novelty Consumable"
  NoveltyOther = "Novelty Other"
  OtherWaterPipes = "Other Water Pipes"
  PipeAccessories = "Pipe Accessories"
  PrefilledNicotinePods = "Prefilled Nicotine Pods"
  Raffle = "Raffle"
  RetailSupplies = "Retail Supplies"
  RetailSupplies2 = "RetailSupplies"
  RillosWrapsCones = "Rillos / Wraps / Cones*"
  RollingAccessories = "Rolling Accessories*"
  RollingMachines = "Rolling Machines"
  RollingPapers = "Rolling Papers*"
  Scales = "Scales"
  StoreStartup = "Store Startup"
  TobaccoAccessories = "Tobacco Accessories*"
  TobaccoPipes = "Tobacco Pipes*"
  Torches = "Torches*"
  VapeAccessories = "Vape Accessories"
  VapeLiquid = "Vape Liquid*"
  VaporDevices = "Vapor Devices"


type StoreNum = int

type SQLPWD = str
type SQLUID = str
type SQLHostName = str
type SQLDriver = str


type BulkRateDataType = DataFrame
type ItemizedInvoiceDataType = DataFrame
type AddressInfoType = DataFrame
type VAPDataType = DataFrame
type BuydownsDataType = DataFrame
type UnitOfMeasureDataType = DataFrame

type PromoFlag = Literal["N", "Y"]

type QueryName = str
type QueryDict = dict[QueryName, QueryPackage]
type QueryResultRaw = list[Row]


class QueryPackage(NamedTuple):
  query: QueryBuilder
  cols: type[ColNameEnum] | list[str]


class SQLCreds(TypedDict):
  DRIVER: SQLDriver
  UID: SQLUID
  PWD: SQLPWD


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


class BulkDataPackage(NamedTuple):
  storenum: StoreNum
  bulk_rate_data: BulkRateDataType


class ItemizedDataPackage(NamedTuple):
  storenum: StoreNum
  itemized_invoice_data: ItemizedInvoiceDataType


class StoreResultsPackage(UserDict):
  data: dict[QueryName, DataFrame]

  def __init__(self, storenum: StoreNum, data=None) -> None:
    super().__init__(data)
    self.__storenum = storenum

  @property
  def storenum(self) -> StoreNum:
    return self.__storenum

  def __bool__(self):
    return any(val is not None for val in self.values())


class QueryResultsPackage(StoreResultsPackage):
  data: dict[QueryName, QueryResultRaw]


class classproperty(property):
  def __get__(self, owner_self, owner_cls):
    return self.fget(owner_cls)
