from datetime import datetime, timedelta
from io import StringIO

from config import SETTINGS
from dataframe_transformations import apply_model_to_df_transforming
from dataframe_utils import fillnas
from pandas import DataFrame, concat, read_csv
from rich.progress import Progress
from types_column_names import ItemizedInvoiceCols, PMUSAScanHeaders, RJRNamesFinal, RJRScanHeaders
from utils import (
  CWD,
  decimal_converter,
  pm_start_end_dates,
  rjr_start_end_dates,
  taskgen_whencalled,
  truncate_decimal,
)
from validation_pmusa import FTXPMUSAValidationModel, PMUSAValidationModel
from validation_rjr import FTXRJRValidationModel, RJRValidationModel

RJR_SCAN_FILENAME_FORMAT = "B56192_{datetime:%Y%m%d_%H%S}_SWEETFIRETOBACCO.txt"

ALT_SCAN_FILENAME_FORMAT = "SweetFireTobacco{date:%Y%m%d}.txt"

rjr_scan_start_date, rjr_scan_end_date = rjr_start_end_dates(SETTINGS.week_shift)
altria_scan_start_date, altria_scan_end_date = pm_start_end_dates(SETTINGS.week_shift)

shifted_rjr_end_date = rjr_scan_end_date - timedelta(days=1)
shifted_altria_end_date = altria_scan_end_date - timedelta(days=1)


RJR_OUTPUT_FOLDER = CWD / "Output RJR Scan Data"
rjr_result_folder = RJR_OUTPUT_FOLDER / "New" / f"Week Ending {shifted_rjr_end_date:%m-%d-%y}"
rjr_sub_folder = RJR_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_rjr_end_date:%m-%d-%y}"
rjr_result_folder.mkdir(exist_ok=True, parents=True)
rjr_sub_folder.mkdir(exist_ok=True, parents=True)

ALT_OUTPUT_FOLDER = CWD / "Output Altria Scan Data"
alt_sub_folder = (
  ALT_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_altria_end_date:%m-%d-%y}"
)
alt_sub_folder.mkdir(exist_ok=True, parents=True)

FTX_SCANDATA_INPUT_FOLDER = CWD / "Input FTX Scan Data"
FTX_SCANDATA_INPUT_FOLDER.mkdir(exist_ok=True)

FTX_RJR_SCAN_FILE_PATH = FTX_SCANDATA_INPUT_FOLDER / f"ftx_rjr_{shifted_rjr_end_date:%Y%m%d}.dat"

FTX_ALT_SCAN_FILE_PATH = (
  FTX_SCANDATA_INPUT_FOLDER / f"ftx_altria_{shifted_altria_end_date:%Y%m%d}.txt"
)


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

  input_data.apply(
    taskgen_whencalled(
      pbar,
      "Validating RJR scan data",
      len(input_data),
    )(apply_model_to_df_transforming)(),
    axis=1,
    new_rows=new_rows,
    model=RJRValidationModel,
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

  input_data.apply(
    taskgen_whencalled(
      pbar,
      "Validating Altria scan data",
      len(input_data),
    )(apply_model_to_df_transforming)(),
    axis=1,
    new_rows=new_rows,
    model=PMUSAValidationModel,
  )

  altria_scan = concat(new_rows, axis=1).T

  altria_scan = altria_scan[PMUSAScanHeaders.all_columns()]

  ftx_input = read_csv(
    FTX_ALT_SCAN_FILE_PATH,
    sep="|",
    header=None,
    names=PMUSAScanHeaders.all_columns(),
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

  ftx_input[PMUSAScanHeaders.QtySold] = ftx_input[PMUSAScanHeaders.QtySold].astype(int)
  ftx_input[PMUSAScanHeaders.FinalSalesPrice] = ftx_input[PMUSAScanHeaders.FinalSalesPrice].map(
    decimal_converter
  )

  altria_scan_new = concat([altria_scan, ftx_input], ignore_index=True)

  stream = StringIO(newline=None)

  summary_line = "|".join(
    [
      str(altria_scan_new.shape[0]),
      str(altria_scan_new[PMUSAScanHeaders.QtySold].sum()),
      str(truncate_decimal(altria_scan_new[PMUSAScanHeaders.FinalSalesPrice].sum())),
      "SweetFireTobacco",
    ]
  )

  stream.write(summary_line + "\n")

  altria_scan_new.to_csv(stream, sep="|", index=False, header=False)

  with (ALT_OUTPUT_FOLDER / ALT_SCAN_FILENAME_FORMAT.format(date=shifted_altria_end_date)).open(
    "w"
  ) as f:
    f.write(stream.getvalue())


# if __name__ == "__main__":
#   with LiveCustom(
#     console=RICH_CONSOLE,
#     # transient=True,
#   ) as live:
#     pbar = live.pbar
#     ftx_input = read_csv(
#       FTX_ALT_SCAN_FILE_PATH,
#       sep="|",
#       header=None,
#       names=PMUSAScanHeaders.all_columns(),
#       dtype=str,
#     )

#     # drop summary line from ftx_df
#     ftx_input.drop(index=0, inplace=True)

#     ftx_input = ftx_input.map(fillnas)

#     ftx_rows = []

#     ftx_input.apply(
#       taskgen_whencalled(
#         pbar,
#         "Validating FTX Altria scan data",
#         len(ftx_input),
#       )(apply_model_to_df_transforming)(),
#       axis=1,
#       new_rows=ftx_rows,
#       model=FTXPMUSAValidationModel,
#     )

#     ftx_df = concat(ftx_rows, axis=1).T

#     pass
