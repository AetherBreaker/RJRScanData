from datetime import datetime, timedelta
from io import StringIO

from config import SETTINGS
from dataframe_transformations import apply_model_to_df_transforming, context_setup
from dataframe_utils import fillnas
from pandas import DataFrame, concat, read_csv
from rich.progress import Progress
from types_column_names import (
  AltriaScanHeaders,
  ItemizedInvoiceCols,
  ITGScanHeaders,
  RJRNamesFinal,
  RJRScanHeaders,
)
from types_custom import ModelContextType
from utils import (
  CWD,
  alt_start_end_dates,
  decimal_converter,
  itg_start_end_dates,
  rjr_start_end_dates,
  taskgen_whencalled,
  truncate_decimal,
)
from validation_alt import AltriaValidationModel
from validation_itg import ITGValidationModel
from validation_rjr import RJRValidationModel

RJR_SCAN_FILENAME_FORMAT = "B56192_{datetime:%Y%m%d_%H%S}_SWEETFIRETOBACCO.txt"
ALT_SCAN_FILENAME_FORMAT = "SweetFireTobacco{date:%Y%m%d}.txt"
ITG_SCAN_MAIN_FILENAME_FORMAT = "SweetFireTobacco{date:%m%d%Y}.csv"
ITG_SCAN_TEST_FILENAME_FORMAT = "SweetFireTobacco{date:%m%d%Y}_TEST.csv"

# Monday - Sunday
rjr_scan_start_date, rjr_scan_end_date = rjr_start_end_dates(SETTINGS.week_shift)
itg_scan_start_date, itg_scan_end_date = itg_start_end_dates(SETTINGS.week_shift)
# Sunday - Saturday
altria_scan_start_date, altria_scan_end_date = alt_start_end_dates(SETTINGS.week_shift)

shifted_rjr_end_date = rjr_scan_end_date - timedelta(days=1)
shifted_alt_end_date = altria_scan_end_date - timedelta(days=1)
shifted_itg_end_date = itg_scan_end_date - timedelta(days=1)


RJR_OUTPUT_FOLDER = CWD / "Output RJR Scan Data"
rjr_result_folder = RJR_OUTPUT_FOLDER / "New" / f"Week Ending {shifted_rjr_end_date:%m-%d-%y}"
rjr_sub_folder = RJR_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_rjr_end_date:%m-%d-%y}"
rjr_result_folder.mkdir(exist_ok=True, parents=True)
rjr_sub_folder.mkdir(exist_ok=True, parents=True)

ALT_OUTPUT_FOLDER = CWD / "Output Altria Scan Data"
alt_sub_folder = ALT_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_alt_end_date:%m-%d-%y}"
alt_sub_folder.mkdir(exist_ok=True, parents=True)


ITG_OUTPUT_FOLDER = CWD / "Output ITG Scan Data"
itg_sub_folder = ITG_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_itg_end_date:%m-%d-%y}"
itg_sub_folder.mkdir(exist_ok=True, parents=True)


FTX_SCANDATA_INPUT_FOLDER = CWD / "Input FTX Scan Data"
FTX_SCANDATA_INPUT_FOLDER.mkdir(exist_ok=True)

FTX_RJR_SCAN_FILE_PATH = FTX_SCANDATA_INPUT_FOLDER / f"ftx_rjr_{shifted_rjr_end_date:%Y%m%d}.dat"
FTX_ALT_SCAN_FILE_PATH = FTX_SCANDATA_INPUT_FOLDER / f"ftx_alt_{shifted_alt_end_date:%Y%m%d}.txt"
FTX_ITG_SCAN_FILE_PATH = FTX_SCANDATA_INPUT_FOLDER / f"ftx_itg_{shifted_itg_end_date:%Y%m%d}.txt"


def apply_rjr_validation(
  pbar: Progress,
  input_data: DataFrame,
):
  input_data = input_data.copy(deep=True)

  input_data = input_data[
    (input_data[ItemizedInvoiceCols.DateTime] >= rjr_scan_start_date)
    & (input_data[ItemizedInvoiceCols.DateTime] < rjr_scan_end_date)
  ]

  new_rows = []

  rjr_errors = []
  rjr_rules: ModelContextType = {
    "fields_to_not_report": {
      ItemizedInvoiceCols.CustNum,
      ItemizedInvoiceCols.ItemNum,
    }
  }

  input_data.apply(
    taskgen_whencalled(
      pbar,
      "Validating RJR scan data",
      len(input_data),
    )(
      context_setup(
        model=RJRValidationModel,
        xtra_rules=rjr_rules,
        errors=rjr_errors,
      )(apply_model_to_df_transforming)
    )(),
    axis=1,
    new_rows=new_rows,
  )

  rjr_scan = concat(new_rows, axis=1).T

  rjr_scan = rjr_scan[RJRScanHeaders.all_columns()]

  ftx_df = read_csv(
    FTX_RJR_SCAN_FILE_PATH,
    sep="|",
    header=None,
    names=RJRScanHeaders.all_columns(),
    dtype=str,
  )

  ftx_df = ftx_df.map(fillnas)

  # ftx_rows = []

  # ftx_df.apply(
  #   taskgen_whencalled(
  #     pbar,
  #     "Validating FTX RJR scan data",
  #     len(ftx_df),
  #   )(apply_model_to_df_transforming)(),
  #   axis=1,
  #   new_rows=ftx_rows,
  #   model=FTXRJRValidationModel,
  # )

  # ftx_df = concat(ftx_rows, axis=1).T

  rjr_df = read_csv(
    StringIO(rjr_scan.to_csv(sep="|", index=False)),
    sep="|",
    header=0,
    dtype=str,
  )

  rjr_scan = concat([rjr_df, ftx_df], ignore_index=True)

  rjr_scan.rename(
    columns={
      old_col: new_col
      for old_col, new_col in zip(RJRScanHeaders.all_columns(), RJRNamesFinal.all_columns())
    },
    inplace=True,
  )

  now = datetime.now()
  rjr_scan.to_csv(
    (rjr_result_folder / RJR_SCAN_FILENAME_FORMAT.format(datetime=now)),
    sep="|",
    index=False,
  )


def apply_altria_validation(
  pbar: Progress,
  input_data: DataFrame,
):
  input_data = input_data.copy(deep=True)

  input_data = input_data[
    (input_data[ItemizedInvoiceCols.DateTime] >= altria_scan_start_date)
    & (input_data[ItemizedInvoiceCols.DateTime] < altria_scan_end_date)
  ]

  new_rows = []
  altria_errors = []

  altria_rules: ModelContextType = {
    "fields_to_not_report": {
      ItemizedInvoiceCols.CustNum,
      ItemizedInvoiceCols.ItemNum,
      AltriaScanHeaders.SKUCode,
    }
  }

  input_data.apply(
    taskgen_whencalled(
      pbar,
      "Validating Altria scan data",
      len(input_data),
    )(
      context_setup(
        model=AltriaValidationModel,
        xtra_rules=altria_rules,
        errors=altria_errors,
      )(apply_model_to_df_transforming)
    )(),
    axis=1,
    new_rows=new_rows,
  )

  altria_scan = concat(new_rows, axis=1).T

  altria_scan = altria_scan[AltriaScanHeaders.all_columns()]

  ftx_input = read_csv(
    FTX_ALT_SCAN_FILE_PATH,
    sep="|",
    header=None,
    names=AltriaScanHeaders.all_columns(),
    dtype=str,
  )

  # drop summary line from ftx_df
  ftx_input.drop(index=0, inplace=True)

  ftx_input = ftx_input.map(fillnas)

  # ftx_rows = []

  # ftx_input.apply(
  #   taskgen_whencalled(
  #     pbar,
  #     "Validating FTX Altria scan data",
  #     len(ftx_input),
  #   )(apply_model_to_df_transforming)(),
  #   axis=1,
  #   new_rows=ftx_rows,
  #   model=FTXPMUSAValidationModel,
  # )

  # ftx_df = concat(ftx_rows, axis=1).T

  ftx_input[AltriaScanHeaders.QtySold] = ftx_input[AltriaScanHeaders.QtySold].astype(int)
  ftx_input[AltriaScanHeaders.FinalSalesPrice] = ftx_input[AltriaScanHeaders.FinalSalesPrice].map(
    decimal_converter
  )

  altria_scan_new = concat([altria_scan, ftx_input], ignore_index=True)

  stream = StringIO(newline=None)

  summary_line = "|".join(
    [
      str(altria_scan_new.shape[0]),
      str(altria_scan_new[AltriaScanHeaders.QtySold].sum()),
      str(truncate_decimal(altria_scan_new[AltriaScanHeaders.FinalSalesPrice].sum())),
      "SweetFireTobacco",
    ]
  )

  stream.write(summary_line + "\n")

  altria_scan_new.to_csv(stream, sep="|", index=False, header=False)

  with (ALT_OUTPUT_FOLDER / ALT_SCAN_FILENAME_FORMAT.format(date=shifted_alt_end_date)).open(
    "w"
  ) as f:
    f.write(stream.getvalue())


def apply_itg_validation(
  pbar: Progress,
  input_data: DataFrame,
):
  input_data = input_data.copy(deep=True)

  input_data = input_data[
    (input_data[ItemizedInvoiceCols.DateTime] >= itg_scan_start_date)
    & (input_data[ItemizedInvoiceCols.DateTime] < itg_scan_end_date)
  ]

  new_rows = []
  itg_errors = []
  itg_rules: ModelContextType = {
    "fields_to_not_report": {
      ItemizedInvoiceCols.CustNum,
      ItemizedInvoiceCols.ItemNum,
    }
  }

  input_data.apply(
    taskgen_whencalled(
      pbar,
      "Validating ITG scan data",
      len(input_data),
    )(
      context_setup(
        model=ITGValidationModel,
        xtra_rules=itg_rules,
        errors=itg_errors,
      )(apply_model_to_df_transforming)
    )(),
    axis=1,
    new_rows=new_rows,
  )

  itg_scan = concat(new_rows, axis=1).T

  itg_scan = itg_scan[ITGScanHeaders.all_columns()]

  ftx_df = read_csv(
    FTX_ITG_SCAN_FILE_PATH,
    sep="|",
    header=None,
    names=ITGScanHeaders.all_columns(),
    dtype=str,
  )

  ftx_df = ftx_df.map(fillnas)

  # ftx_rows = []

  # ftx_df.apply(
  #   taskgen_whencalled(
  #     pbar,
  #     "Validating FTX ITG scan data",
  #     len(ftx_df),
  #   )(apply_model_to_df_transforming)(),
  #   axis=1,
  #   new_rows=ftx_rows,
  #   model=FTXITGValidationModel,
  # )

  # ftx_df = concat(ftx_rows, axis=1).T

  itg_df = read_csv(
    StringIO(itg_scan.to_csv(sep="|", index=False)),
    sep="|",
    header=0,
    dtype=str,
  )

  itg_scan = concat([itg_df, ftx_df], ignore_index=True)

  # itg_scan.rename(
  #   columns={
  #     old_col: new_col
  #     for old_col, new_col in zip(ITGScanHeaders.all_columns(), ITGNamesFinal.all_columns())
  #   },
  #   inplace=True,
  # )

  output_path = (
    (ITG_OUTPUT_FOLDER / ITG_SCAN_TEST_FILENAME_FORMAT.format(date=shifted_itg_end_date))
    if SETTINGS.test_file
    else (ITG_OUTPUT_FOLDER / ITG_SCAN_MAIN_FILENAME_FORMAT.format(date=shifted_itg_end_date))
  )

  itg_scan.to_csv(output_path, sep="|", index=False)
