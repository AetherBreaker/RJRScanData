if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import datetime, timedelta
from io import StringIO
from logging import getLogger

from config import SETTINGS
from dataframe_transformations import apply_model_to_df_transforming, context_setup
from dataframe_utils import fillnas
from pandas import DataFrame, concat, read_csv
from reporting_validation_errs import assemble_validation_error_report
from rich.progress import Progress
from types_column_names import AltriaScanHeaders, ItemizedInvoiceCols, ITGScanHeaders, RJRNamesFinal, RJRScanHeaders
from utils import (
  CWD,
  alt_start_end_dates,
  decimal_converter,
  itg_start_end_dates,
  rjr_start_end_dates,
  taskgen_whencalled,
  truncate_decimal,
)
from validation_final_alt import AltriaValidationModel, FTXPMUSAValidationModel
from validation_final_itg import FTXITGValidationModel, ITGValidationModel
from validation_final_rjr import FTXRJRValidationModel, RJRValidationModel

logger = getLogger(__name__)


RJR_SCAN_FILENAME_FORMAT = "B56192_{datetime:%Y%m%d_%H%M}_SWEETFIRETOBACCO.txt"
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
ALT_OUTPUT_FOLDER = CWD / "Output Altria Scan Data"
ITG_OUTPUT_FOLDER = CWD / "Output ITG Scan Data"

rjr_res_folder = RJR_OUTPUT_FOLDER / "New" / f"Week Ending {shifted_rjr_end_date:%m-%d-%y}"
rjr_sub_folder = RJR_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_rjr_end_date:%m-%d-%y}"
alt_sub_folder = ALT_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_alt_end_date:%m-%d-%y}"
itg_sub_folder = ITG_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_itg_end_date:%m-%d-%y}"

rjr_res_folder.mkdir(exist_ok=True, parents=True)
rjr_sub_folder.mkdir(exist_ok=True, parents=True)
alt_sub_folder.mkdir(exist_ok=True, parents=True)
itg_sub_folder.mkdir(exist_ok=True, parents=True)


FTX_SCANDATA_INPUT_FOLDER = CWD / "Input FTX Scan Data"
FTX_SCANDATA_INPUT_FOLDER.mkdir(exist_ok=True)

FTX_RJR_SCAN_FILE_PATH = FTX_SCANDATA_INPUT_FOLDER / f"ftx_rjr_{shifted_rjr_end_date:%Y%m%d}.dat"
FTX_ALT_SCAN_FILE_PATH = FTX_SCANDATA_INPUT_FOLDER / f"ftx_alt_{shifted_alt_end_date:%Y%m%d}.txt"
FTX_ITG_SCAN_FILE_PATH = FTX_SCANDATA_INPUT_FOLDER / f"ftx_itg_{shifted_itg_end_date:%Y%m%d}.txt"


ERR_OUTPUT_FOLDER = CWD / "Validation Errors Output"
ERR_OUTPUT_FOLDER.mkdir(exist_ok=True)

ALT_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / f"ALTScanErrors_{shifted_alt_end_date:%Y%m%d}.csv"
RJR_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / f"RJRScanErrors_{shifted_rjr_end_date:%Y%m%d}.csv"
ITG_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / f"ITGScanErrors_{shifted_itg_end_date:%Y%m%d}.csv"

ALT_FTX_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / f"FTXAltScanErrors_{shifted_alt_end_date:%Y%m%d}.csv"
RJR_FTX_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / f"FTXRJRScanErrors_{shifted_rjr_end_date:%Y%m%d}.csv"
ITG_FTX_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / f"FTXITGScanErrors_{shifted_itg_end_date:%Y%m%d}.csv"


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

  input_data.apply(
    taskgen_whencalled(
      pbar,
      "Validating RJR scan data",
      len(input_data),
    )(
      context_setup(
        model=RJRValidationModel,
        # xtra_rules=RJR_RULES,
        errors=rjr_errors,
      )(apply_model_to_df_transforming)
    )(),
    axis=1,
    new_rows=new_rows,
  )

  assemble_validation_error_report(pbar, rjr_errors, "RJR", RJR_ERR_OUTPUT_FILE)

  rjr_scan = concat(new_rows, axis=1).T

  rjr_scan = rjr_scan[RJRScanHeaders.all_columns()]

  ftx_df = read_csv(
    FTX_RJR_SCAN_FILE_PATH,
    sep="|",
    header=None,
    names=RJRScanHeaders.all_columns(),
    dtype=str,
    index_col=False,
  )

  ftx_df = ftx_df.map(fillnas)

  ftx_rows = []
  ftx_errs = []

  ftx_df.apply(
    taskgen_whencalled(
      pbar,
      "Validating FTX RJR scan data",
      len(ftx_df),
    )(
      context_setup(
        model=FTXRJRValidationModel,
        errors=ftx_errs,
      )(apply_model_to_df_transforming)
    )(),
    axis=1,
    new_rows=ftx_rows,
  )

  assemble_validation_error_report(pbar, ftx_errs, "FTX RJR", RJR_FTX_ERR_OUTPUT_FILE)

  ftx_df = concat(ftx_rows, axis=1).T

  rjr_df = read_csv(
    StringIO(rjr_scan.to_csv(sep="|", index=False)),
    sep="|",
    header=0,
    dtype=str,
  )

  rjr_scan = concat([rjr_df, ftx_df], ignore_index=True)

  rjr_scan.rename(
    columns={old_col: new_col for old_col, new_col in zip(RJRScanHeaders.all_columns(), RJRNamesFinal.all_columns())},
    inplace=True,
  )

  now = datetime.now()
  rjr_scan.to_csv(
    (rjr_res_folder / RJR_SCAN_FILENAME_FORMAT.format(datetime=now)),
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

  input_data.apply(
    taskgen_whencalled(
      pbar,
      "Validating Altria scan data",
      len(input_data),
    )(
      context_setup(
        model=AltriaValidationModel,
        # xtra_rules=ALTRIA_RULES,
        errors=altria_errors,
      )(apply_model_to_df_transforming)
    )(),
    axis=1,
    new_rows=new_rows,
  )

  assemble_validation_error_report(pbar, altria_errors, "Altria", ALT_ERR_OUTPUT_FILE)

  altria_scan = concat(new_rows, axis=1).T

  altria_scan = altria_scan[AltriaScanHeaders.all_columns()]

  ftx_df = read_csv(
    FTX_ALT_SCAN_FILE_PATH,
    sep="|",
    header=None,
    names=AltriaScanHeaders.all_columns(),
    dtype=str,
    index_col=False,
  )

  # drop summary line from ftx_df
  ftx_df.drop(index=0, inplace=True)

  ftx_df = ftx_df.map(fillnas)

  ftx_rows = []
  ftx_errs = []

  ftx_df.apply(
    taskgen_whencalled(
      pbar,
      "Validating FTX Altria scan data",
      len(ftx_df),
    )(
      context_setup(
        model=FTXPMUSAValidationModel,
        errors=ftx_errs,
      )(apply_model_to_df_transforming)
    )(),
    axis=1,
    new_rows=ftx_rows,
  )

  assemble_validation_error_report(pbar, ftx_errs, "FTX Altria", ALT_FTX_ERR_OUTPUT_FILE)

  ftx_df = concat(ftx_rows, axis=1).T

  ftx_df[AltriaScanHeaders.QtySold] = ftx_df[AltriaScanHeaders.QtySold].astype(int)
  ftx_df[AltriaScanHeaders.FinalSalesPrice] = ftx_df[AltriaScanHeaders.FinalSalesPrice].map(decimal_converter)

  altria_scan_new = concat([altria_scan, ftx_df], ignore_index=True)

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

  with (ALT_OUTPUT_FOLDER / ALT_SCAN_FILENAME_FORMAT.format(date=shifted_alt_end_date)).open("w") as f:
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

  input_data.apply(
    taskgen_whencalled(
      pbar,
      "Validating ITG scan data",
      len(input_data),
    )(
      context_setup(
        model=ITGValidationModel,
        # xtra_rules=ITG_RULES,
        errors=itg_errors,
      )(apply_model_to_df_transforming)
    )(),
    axis=1,
    new_rows=new_rows,
  )

  assemble_validation_error_report(pbar, itg_errors, "ITG", ITG_ERR_OUTPUT_FILE)

  itg_scan = concat(new_rows, axis=1).T

  itg_scan = itg_scan[ITGScanHeaders.all_columns()]

  ftx_df = read_csv(
    FTX_ITG_SCAN_FILE_PATH,
    sep="|",
    header=None,
    names=ITGScanHeaders.all_columns(),
    dtype=str,
    index_col=False,
  )

  ftx_df = ftx_df.map(fillnas)

  ftx_rows = []
  ftx_errs = []

  ftx_df.apply(
    taskgen_whencalled(
      pbar,
      "Validating FTX ITG scan data",
      len(ftx_df),
    )(
      context_setup(
        model=FTXITGValidationModel,
        errors=ftx_errs,
      )(apply_model_to_df_transforming)
    )(),
    axis=1,
    new_rows=ftx_rows,
  )

  assemble_validation_error_report(pbar, ftx_errs, "FTX ITG", ITG_FTX_ERR_OUTPUT_FILE)

  ftx_df = concat(ftx_rows, axis=1).T

  itg_df = read_csv(
    StringIO(itg_scan.to_csv(sep="|", index=False)),
    sep="|",
    header=0,
    dtype=str,
  )

  itg_scan = concat([itg_df, ftx_df], ignore_index=True)

  output_path = (
    (ITG_OUTPUT_FOLDER / ITG_SCAN_TEST_FILENAME_FORMAT.format(date=shifted_itg_end_date))
    if SETTINGS.test_file
    else (ITG_OUTPUT_FOLDER / ITG_SCAN_MAIN_FILENAME_FORMAT.format(date=shifted_itg_end_date))
  )

  itg_scan.to_csv(output_path, sep="|", index=False)
