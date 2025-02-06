if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from itertools import chain
from logging import getLogger
from pathlib import Path
from typing import Optional

from gspread import service_account
from gspread.http_client import BackOffHTTPClient
from gspread.utils import ValueRenderOption, to_records
from gspread.worksheet import Worksheet
from pandas import DataFrame

from dataframe_transformations import apply_model_to_df
from dataframe_utils import NULL_VALUES
from types_column_names import (
  GSheetsBuydownsCols,
  GSheetsStoreInfoCols,
  GSheetsUnitsOfMeasureCols,
  GSheetsVAPDiscountsCols,
)
from types_custom import AddressInfoType, BuydownsDataType, UnitOfMeasureDataType, VAPDataType
from utils import SingletonType
from validation_other import BuydownsModel, StoreInfoModel, UnitsOfMeasureModel, VAPDiscountsModel

logger = getLogger(__name__)

CWD = Path.cwd()


GOOGLE_CREDENTIALS_PATH = (CWD / __file__).with_name("sft-scan-data-1c4d1707c2d5.json")
GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


SERVICE_ACCOUNT = service_account(
  filename=GOOGLE_CREDENTIALS_PATH, scopes=GOOGLE_SCOPES, http_client=BackOffHTTPClient
)


STORE_INFO_SHEET_ID = "178D5UCOlah9OCBn8J51fyLLjLQ0s9hKMVHu43g6UQAc"
STORE_INFO_SHEETNAME = "Sheet1"

UNITS_OF_MEASURE_SHEET_ID = "1nLk8hC0xnhXDf7rg9IlXZJz36fiRq_FHaMnuM_BZLIc"
UNITS_OF_MEASURE_SHEETNAME = "Sheet1"

VAP_DISCOUNTS_SHEET_ID = "1V7MB60Wh3S9kQXQ4SNuyRX735LnygxC9rfdIwcOxycM"
VAP_DISCOUNTS_SHEETNAME = "Sheet1"

MANUFACTURER_BUYDOWNS_SHEET_ID = "1-ha62QWkAYPPWL6ATc_we1uErqeNA9GBAyBTk8gRa6o"
MANUFACTURER_BUYDOWNS_SHEETNAME = "Buydowns"

SCANNABLE_COUPONS_SHEET_ID = "19s8xwxmMqbN6Me3rGzvIQbODRAUX9iXe_cMaEaKJKGw"
SCANNABLE_COUPONS_SHEETNAME = "Sheet1"


def get_all_records(
  worksheet: Worksheet,
  head: int = 1,
  value_render_option: Optional[ValueRenderOption] = None,
) -> list[dict[str, int | float | str]]:
  entire_sheet = worksheet.get(
    value_render_option=value_render_option,
  )
  if entire_sheet == [[]]:
    # see test_get_all_records_with_all_values_blank
    #  we don't know the length of the sheet so we return []
    return []

  keys = entire_sheet[head - 1]
  values = entire_sheet[head:]

  return to_records(keys, values)


class SheetCache(metaclass=SingletonType):
  def __init__(self):
    store_info_sheet = SERVICE_ACCOUNT.open_by_key(STORE_INFO_SHEET_ID).worksheet(
      STORE_INFO_SHEETNAME
    )
    bds_sheet = SERVICE_ACCOUNT.open_by_key(MANUFACTURER_BUYDOWNS_SHEET_ID).worksheet(
      MANUFACTURER_BUYDOWNS_SHEETNAME
    )
    vap_sheet = SERVICE_ACCOUNT.open_by_key(VAP_DISCOUNTS_SHEET_ID).worksheet(
      VAP_DISCOUNTS_SHEETNAME
    )
    uom_sheet = SERVICE_ACCOUNT.open_by_key(UNITS_OF_MEASURE_SHEET_ID).worksheet(
      UNITS_OF_MEASURE_SHEETNAME
    )

    info_table_raw = get_all_records(store_info_sheet, value_render_option="UNFORMATTED_VALUE")
    bds_table_raw = get_all_records(bds_sheet, value_render_option="UNFORMATTED_VALUE")
    vap_table_raw = get_all_records(vap_sheet, value_render_option="UNFORMATTED_VALUE")
    uom_table_raw = get_all_records(uom_sheet, value_render_option="UNFORMATTED_VALUE")

    # remove any column with empty header
    for record in chain(vap_table_raw, uom_table_raw, bds_table_raw, info_table_raw):
      for key in list(record.keys()):
        if not key.strip():
          record.pop(key)

    info = DataFrame(info_table_raw, dtype=str, columns=GSheetsStoreInfoCols.all_columns())
    bds = DataFrame(bds_table_raw, dtype=str, columns=GSheetsBuydownsCols.all_columns())
    vap = DataFrame(vap_table_raw, dtype=str, columns=GSheetsVAPDiscountsCols.all_columns())
    uom = DataFrame(uom_table_raw, dtype=str, columns=GSheetsUnitsOfMeasureCols.all_columns())

    info = info.replace(NULL_VALUES, value=None)
    bds = bds.replace(NULL_VALUES, value=None)
    vap = vap.replace(NULL_VALUES, value=None)
    uom = uom.replace(NULL_VALUES, value=None)

    info: AddressInfoType = info.apply(
      apply_model_to_df, model=StoreInfoModel, axis=1, result_type="broadcast"
    )
    bds: BuydownsDataType = bds.apply(
      apply_model_to_df, model=BuydownsModel, axis=1, result_type="broadcast"
    )
    vap: VAPDataType = vap.apply(
      apply_model_to_df, model=VAPDiscountsModel, axis=1, result_type="broadcast"
    )
    uom: UnitOfMeasureDataType = uom.apply(
      apply_model_to_df, model=UnitsOfMeasureModel, axis=1, result_type="broadcast"
    )

    self.info = info.set_index(GSheetsStoreInfoCols.StoreNum)
    self.bds = bds.set_index(GSheetsBuydownsCols.UPC)
    self.vap = vap.set_index(GSheetsVAPDiscountsCols.UPC)
    self.uom = uom.set_index(GSheetsUnitsOfMeasureCols.UPC)


if __name__ == "__main__":
  test = SheetCache()
