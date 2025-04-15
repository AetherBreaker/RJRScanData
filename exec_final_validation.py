from datetime import datetime, timedelta
from decimal import Decimal
from io import StringIO

from config import SETTINGS
from dataframe_transformations import apply_model_to_df_transforming
from pandas import DataFrame, concat, read_csv
from rich.progress import Progress
from types_column_names import PMUSAScanHeaders, RJRNamesFinal, RJRScanHeaders
from utils import CWD, pm_start_end_dates, rjr_start_end_dates, taskgen_whencalled, truncate_decimal
from validation_pmusa import PMUSAValidationModel
from validation_rjr import RJRValidationModel

RJR_SCAN_FILENAME_FORMAT = "B56192_{datetime:%Y%m%d_%H%S}_SWEETFIRETOBACCO.txt"

ALT_SCAN_FILENAME_FORMAT = "SweetFireTobacco{date:%Y%m%d}.txt"

rjr_scan_start_date, rjr_scan_end_date = rjr_start_end_dates(SETTINGS.week_shift)
altria_scan_start_date, altria_scan_end_date = pm_start_end_dates(SETTINGS.week_shift)

RJR_OUTPUT_FOLDER = CWD / "RJR Scan Data"
ALT_OUTPUT_FOLDER = CWD / "Altria Scan Data"
rjr_sub_folder = RJR_OUTPUT_FOLDER / "submissions" / f"Week Ending {rjr_scan_end_date:%y-%m-%d}"
alt_sub_folder = ALT_OUTPUT_FOLDER / "submissions" / f"Week Ending {altria_scan_end_date:%y-%m-%d}"
rjr_sub_folder.mkdir(exist_ok=True, parents=True)
alt_sub_folder.mkdir(exist_ok=True, parents=True)


def apply_rjr_validation(
  pbar: Progress,
  input_data: DataFrame,
):
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

  # filter by date range
  rjr_scan = rjr_scan[
    (rjr_scan[RJRScanHeaders.transaction_date] >= rjr_scan_start_date)
    & (rjr_scan[RJRScanHeaders.transaction_date] < rjr_scan_end_date)
  ]

  ftx_df = read_csv(
    CWD / f"ftx_rjr_{rjr_scan_end_date - timedelta(days=1):%Y%m%d}.dat",
    sep="|",
    header=None,
    names=RJRScanHeaders.all_columns(),
    dtype=str,
  )

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
    (RJR_OUTPUT_FOLDER / RJR_SCAN_FILENAME_FORMAT.format(datetime=now)),
    sep="|",
    index=False,
  )


def apply_altria_validation(
  pbar: Progress,
  input_data: DataFrame,
):
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

  # filter by date range
  altria_scan = altria_scan[
    (altria_scan[PMUSAScanHeaders.DateTime] >= altria_scan_start_date)
    & (altria_scan[PMUSAScanHeaders.DateTime] < altria_scan_end_date)
  ]

  altria_scan.drop(columns=PMUSAScanHeaders.DateTime, inplace=True)

  week_ending_date = altria_scan_end_date - timedelta(days=1)

  ftx_df = read_csv(
    CWD / f"ftx_altria_{week_ending_date:%Y%m%d}.txt",
    sep="|",
    header=None,
    names=PMUSAScanHeaders.all_columns(),
    dtype=str,
  )

  ftx_summary_line = ftx_df.iloc[0]

  # drop summary line from ftx_df
  ftx_df.drop(index=0, inplace=True)

  altria_df = read_csv(
    StringIO(altria_scan.to_csv(sep="|", index=False)),
    sep="|",
    header=0,
    dtype=str,
  )

  altria_scan_new = concat([altria_df, ftx_df], ignore_index=True)

  stream = StringIO(newline=None)

  summary_line = "|".join(
    [
      str(altria_scan_new.shape[0]),
      str(
        altria_scan[PMUSAScanHeaders.QtySold].sum()
        + int(ftx_summary_line[PMUSAScanHeaders.WeekEndDate])
      ),
      str(
        truncate_decimal(
          altria_scan[PMUSAScanHeaders.FinalSalesPrice].sum()
          + Decimal(ftx_summary_line[PMUSAScanHeaders.TransactionDate])
        )
      ),
      "SweetFireTobacco",
    ]
  )

  stream.write(summary_line + "\n")

  altria_scan_new.to_csv(stream, sep="|", index=False, header=False)

  with (ALT_OUTPUT_FOLDER / ALT_SCAN_FILENAME_FORMAT.format(date=week_ending_date)).open("w") as f:
    f.write(stream.getvalue())
