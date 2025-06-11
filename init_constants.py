if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import datetime, timedelta
from logging import getLogger
from pathlib import Path

from config import SETTINGS
from utils import alt_start_end_dates, itg_start_end_dates, rjr_start_end_dates

logger = getLogger(__name__)


CWD = Path.cwd()


PRECOMBINATION_ITEM_LINES_FOLDER = CWD / "item_lines"
PRECOMBINATION_ITEM_LINES_FOLDER.mkdir(exist_ok=True)

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


week_ending_date_str = "Week Ending {month:0>2}-{daymain:0>2}({dayalt:0>2})-{year:0>4}".format(
  month=shifted_alt_end_date.month,
  daymain=shifted_alt_end_date.day,
  dayalt=shifted_rjr_end_date.day,
  year=shifted_alt_end_date.year,
)


RJR_OUTPUT_FOLDER = CWD / "Output RJR Scan Data"
ALT_OUTPUT_FOLDER = CWD / "Output Altria Scan Data"
ITG_OUTPUT_FOLDER = CWD / "Output ITG Scan Data"


rjr_res_folder = RJR_OUTPUT_FOLDER / "New" / f"Week Ending {shifted_rjr_end_date:%m-%d-%y}"
rjr_res_folder.mkdir(exist_ok=True, parents=True)

RJR_SCAN_FILE_PATH = rjr_res_folder / RJR_SCAN_FILENAME_FORMAT.format(datetime=datetime.now())
ALT_SCAN_FILE_PATH = ALT_OUTPUT_FOLDER / ALT_SCAN_FILENAME_FORMAT.format(date=shifted_alt_end_date)
ITG_SCAN_FILE_PATH = (
  (ITG_OUTPUT_FOLDER / ITG_SCAN_TEST_FILENAME_FORMAT.format(date=shifted_itg_end_date))
  if SETTINGS.test_file
  else (ITG_OUTPUT_FOLDER / ITG_SCAN_MAIN_FILENAME_FORMAT.format(date=shifted_itg_end_date))
)

rjr_sub_folder = RJR_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_rjr_end_date:%m-%d-%y}"
alt_sub_folder = ALT_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_alt_end_date:%m-%d-%y}"
itg_sub_folder = ITG_OUTPUT_FOLDER / "submissions" / f"Week Ending {shifted_itg_end_date:%m-%d-%y}"
rjr_sub_folder.mkdir(exist_ok=True, parents=True)
alt_sub_folder.mkdir(exist_ok=True, parents=True)
itg_sub_folder.mkdir(exist_ok=True, parents=True)


FTX_SCANDATA_INPUT_FOLDER = CWD / "Input FTX Scan Data"
FTX_SCANDATA_INPUT_FOLDER.mkdir(exist_ok=True)

FTX_RJR_SCAN_FILE_PATH = FTX_SCANDATA_INPUT_FOLDER / f"ftx_rjr_{shifted_rjr_end_date:%Y%m%d}.dat"
FTX_ALT_SCAN_FILE_PATH = FTX_SCANDATA_INPUT_FOLDER / f"ftx_alt_{shifted_alt_end_date:%Y%m%d}.txt"
FTX_ITG_SCAN_FILE_PATH = FTX_SCANDATA_INPUT_FOLDER / f"ftx_itg_{shifted_itg_end_date:%Y%m%d}.txt"


ERR_OUTPUT_FOLDER = CWD / "Validation Errors Output" / week_ending_date_str
ERR_OUTPUT_FOLDER.mkdir(exist_ok=True, parents=True)

ALT_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / "ALTScanErrors.csv"
RJR_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / "RJRScanErrors.csv"
ITG_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / "ITGScanErrors.csv"
ALT_FTX_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / "FTXAltScanErrors.csv"
RJR_FTX_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / "FTXRJRScanErrors.csv"
ITG_FTX_ERR_OUTPUT_FILE = ERR_OUTPUT_FOLDER / "FTXITGScanErrors.csv"


REPORTING_FOLDER = CWD / "Generation Reports" / week_ending_date_str
REPORTING_FOLDER.mkdir(exist_ok=True, parents=True)

ALTRIA_LOYALTY_TOTALS_FILE = REPORTING_FOLDER / "Altria_Loyalty_Totals.csv"
ALTRIA_MULTIUNIT_TOTALS_FILE = REPORTING_FOLDER / "Altria_MultiUnit_Totals.csv"
