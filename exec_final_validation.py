if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from io import StringIO
from logging import getLogger

from config import SETTINGS
from dataframe_transformations import apply_model_to_df_transforming, apply_model_to_ftx, context_setup
from dataframe_utils import fillnas
from gsheet_data_processing import SheetCache
from init_constants import (
  ALT_ERR_OUTPUT_FILE,
  ALT_FTX_ERR_OUTPUT_FILE,
  ALT_SCAN_FILE_PATH,
  ALTRIA_LOYALTY_TOTALS_FILE,
  ALTRIA_MULTIUNIT_TOTALS_FILE,
  FTX_ALT_SCAN_FILE_PATH,
  FTX_ITG_SCAN_FILE_PATH,
  FTX_RJR_SCAN_FILE_PATH,
  ITG_ERR_OUTPUT_FILE,
  ITG_FTX_ERR_OUTPUT_FILE,
  ITG_SCAN_FILE_PATH,
  RJR_ERR_OUTPUT_FILE,
  RJR_FTX_ERR_OUTPUT_FILE,
  RJR_SCAN_FILE_PATH,
)
from pandas import DataFrame, concat, read_csv
from reporting_validation_errs import assemble_validation_error_report
from rich.progress import Progress
from types_column_names import (
  AltriaScanHeaders,
  ItemizedInvoiceCols,
  ITGNamesFinal,
  ITGScanHeaders,
  RJRNamesFinal,
  RJRScanHeaders,
)
from utils import (
  alt_start_end_dates,
  decimal_converter,
  itg_start_end_dates,
  rjr_start_end_dates,
  taskgen_whencalled,
  truncate_decimal,
)
from validation_result_alt import AltriaValidationModel, FTXPMUSAValidationModel

# from validation_result_itg import FTXITGValidationModel, ITGValidationModel
from validation_result_rjr import FTXRJRValidationModel, RJRValidationModel

logger = getLogger(__name__)


# Monday - Sunday
rjr_scan_start_date, rjr_scan_end_date = rjr_start_end_dates(SETTINGS.week_shift)
itg_scan_start_date, itg_scan_end_date = itg_start_end_dates(SETTINGS.week_shift)
# Sunday - Saturday
altria_scan_start_date, altria_scan_end_date = alt_start_end_dates(SETTINGS.week_shift)


STORES_CHECK_LIST = {
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  16,
  17,
  18,
  19,
  20,
  21,
  22,
  23,
  24,
  25,
  26,
  27,
  28,
  29,
  30,
  31,
  32,
  34,
  35,
  36,
  37,
  38,
  40,
  42,
  43,
  44,
  45,
  46,
  48,
  49,
  50,
  51,
  53,
  54,
  55,
  56,
  57,
  58,
  59,
  60,
  61,
  62,
  63,
  64,
  65,
  66,
  67,
  68,
  69,
  70,
  72,
  82,
  83,
  84,
  85,
  86,
  87,
  88,
}


addr_data = SheetCache().info


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
      )(apply_model_to_ftx)
    )(),
    axis=1,
    new_rows=ftx_rows,
    addr_data=addr_data,
  )

  assemble_validation_error_report(pbar, ftx_errs, "FTX RJR", RJR_FTX_ERR_OUTPUT_FILE)

  ftx_df = concat(ftx_rows, axis=1).T

  # rjr_df = read_csv(
  #   StringIO(rjr_scan.to_csv(sep="|", index=False)),
  #   sep="|",
  #   header=0,
  #   dtype=str,
  # )

  rjr_scan = concat([rjr_scan, ftx_df], ignore_index=True)

  storenums_present = set(rjr_scan[RJRScanHeaders.outlet_number].tolist())

  # logger.info(f)

  if missing_stores := STORES_CHECK_LIST - storenums_present:
    logger.warning(
      f"Missing stores in RJR scan data: \n{'\n'.join(map(str, sorted(missing_stores)))}\n"
      "Please check the data for completeness."
    )

  rjr_scan.rename(
    columns={old_col: new_col for old_col, new_col in zip(RJRScanHeaders.all_columns(), RJRNamesFinal.all_columns())},
    inplace=True,
  )

  rjr_scan.to_csv(RJR_SCAN_FILE_PATH, sep="|", index=False)


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

  loyalty_sum = altria_scan[AltriaScanHeaders.LoyaltyDiscountAmt].sum()
  multipack_sum = altria_scan[AltriaScanHeaders.TotalMultiUnitDiscountAmt].sum()

  with ALTRIA_LOYALTY_TOTALS_FILE.open("w") as loyalty_file:
    loyalty_file.write(f"Total Loyalty Discount Amount: {truncate_decimal(loyalty_sum)}\n")
  with ALTRIA_MULTIUNIT_TOTALS_FILE.open("w") as multipack_file:
    multipack_file.write(f"Total Multi-Unit Discount Amount: {truncate_decimal(multipack_sum)}\n")

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
      )(apply_model_to_ftx)
    )(),
    axis=1,
    new_rows=ftx_rows,
    addr_data=addr_data,
  )

  assemble_validation_error_report(pbar, ftx_errs, "FTX Altria", ALT_FTX_ERR_OUTPUT_FILE)

  ftx_df = concat(ftx_rows, axis=1).T

  ftx_df[AltriaScanHeaders.QtySold] = ftx_df[AltriaScanHeaders.QtySold].astype(int)
  ftx_df[AltriaScanHeaders.FinalSalesPrice] = ftx_df[AltriaScanHeaders.FinalSalesPrice].map(decimal_converter)

  altria_scan_new = concat([altria_scan, ftx_df], ignore_index=True)

  storenums_present = set(altria_scan_new[AltriaScanHeaders.StoreNumber].tolist())

  if missing_stores := STORES_CHECK_LIST - storenums_present:
    logger.warning(
      f"Missing stores in Altria scan data: \n{'\n'.join(map(str, sorted(missing_stores)))}\n"
      "Please check the data for completeness."
    )

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

  with ALT_SCAN_FILE_PATH.open("w") as f:
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
        # model=ITGValidationModel,
        model=RJRValidationModel,
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
        # model=FTXITGValidationModel,
        model=FTXRJRValidationModel,
        errors=ftx_errs,
      )(apply_model_to_ftx)
    )(),
    axis=1,
    new_rows=ftx_rows,
    addr_data=addr_data,
  )

  assemble_validation_error_report(pbar, ftx_errs, "FTX ITG", ITG_FTX_ERR_OUTPUT_FILE)

  ftx_df = concat(ftx_rows, axis=1).T

  # itg_scan = read_csv(
  #   StringIO(itg_scan.to_csv(sep="|", index=False)),
  #   sep="|",
  #   header=0,
  #   dtype=str,
  # )

  itg_scan = concat([itg_scan, ftx_df], ignore_index=True)

  storenums_present = set(itg_scan[ITGScanHeaders.outlet_number].tolist())

  if missing_stores := STORES_CHECK_LIST - storenums_present:
    logger.warning(
      f"Missing stores in ITG scan data: \n{'\n'.join(map(str, sorted(missing_stores)))}\n"
      "Please check the data for completeness."
    )

  itg_scan.rename(
    columns={old_col: new_col for old_col, new_col in zip(ITGScanHeaders.all_columns(), ITGNamesFinal.all_columns())},
    inplace=True,
  )

  itg_scan.to_csv(
    ITG_SCAN_FILE_PATH,
    sep="|",
    index=False,
    header=True,
  )
