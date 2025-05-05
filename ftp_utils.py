if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import timedelta
from json import load
from pathlib import PurePosixPath
from re import Pattern, compile
from typing import Literal

from config import SETTINGS
from dateutil.rrule import DAILY, rrule
from paramiko import AutoAddPolicy, SFTPClient, SSHClient
from utils import CWD, pm_start_end_dates, rjr_start_end_dates

with (CWD / __file__).with_name("ftp_creds.json").open("r") as file:
  FTP_CREDS = load(file)


RJR_FIND_PATTERN = "_".join(
  (
    r"B56192",
    (r"(?P<Date>{dates})"),
    (
      r"(?P<Hour>2[0-3]|[01][0-9])"  #
      r"(?P<Minute>[0-5][0-9])"  #
    ),
    r"SWEETFIRETOBACCO\.dat",
  )
)
ALTRIA_FIND_PATTERN = (
  r"sweet-fire-tobacco"  #
  r"(?P<Date>{date:%Y%m%d})"  #
  r"_.*\.txt"  #
)

RJR_FILENAME_PATTERN = compile(
  r"B56192_(?P<Year>\d{4})(?P<Month>\d{2})(?P<Day>\d{2})_(?P<Hour>\d{2})(?P<Minute>\d{2})_SWEETFIRETOBACCO\.dat"
)
ALTRIA_FILENAME_PATTERN = compile(
  r"sweet-fire-tobacco(?P<Year>\d{4})(?P<Month>\d{2})(?P<Day>\d{2})_.*\.txt"
)


def create_dated_regexes(provider: Literal["RJR", "Altria", "ITG"]) -> Pattern:
  match provider:
    case "RJR":
      rjr_scan_start_date, rjr_scan_end_date = rjr_start_end_dates(SETTINGS.week_shift + 1)
      valid_rjr_dates = list(
        rrule(
          DAILY,
          dtstart=rjr_scan_start_date.date(),
          until=rjr_scan_end_date.date() - timedelta(days=1),
        )
      )
      dates = "|".join(date.strftime("%Y%m%d") for date in valid_rjr_dates)
      return compile(RJR_FIND_PATTERN.format(dates=dates))

    case "Altria":
      altria_scan_end_date = pm_start_end_dates(SETTINGS.week_shift)[1] - timedelta(days=1)
      return compile(ALTRIA_FIND_PATTERN.format(date=altria_scan_end_date))
    case "ITG":
      # ITG is not implemented yet
      raise NotImplementedError("ITG scan file pattern is not implemented yet.")
    case _:
      raise ValueError(f"Unknown provider: {provider}")


def find_scan_file(provider: Literal["RJR", "Altria", "ITG"]) -> str:
  pattern = create_dated_regexes(provider)
  source_folder = PurePosixPath("/RYO_SAS")

  matches = {}

  ssh_client = SSHClient()
  ssh_client.set_missing_host_key_policy(AutoAddPolicy())
  ssh_client.connect(
    hostname=FTP_CREDS["SAS_SFTP_HOSTNAME"],
    username=FTP_CREDS["SAS_SFTP_USER"],
    password=FTP_CREDS["SAS_SFTP_PWD"],
  )

  with SFTPClient.from_transport(ssh_client.get_transport()) as connection_handler:
    for filename in connection_handler.listdir(path=source_folder.as_posix()):
      if pattern.match(filename):
        t_stamp = connection_handler.stat(filename).st_atime
        matches[t_stamp] = filename

  return matches[max(matches.keys())]


def get_remote_file(
  connection_handler: FTP,
  remote_path: PurePosixPath,
  remote_size: int | None = None,
  local_path: Path | None = None,
) -> Path:
  if local_path is None:
    local_path = Path.cwd()

  local_path = local_path / remote_path.name

  with Progress(
    BarColumn(),
    TaskProgressColumn(),
    MofNCompleteColumn(),
    TimeRemainingColumn(),
    TextColumn("[progress.description]{task.description}"),
    console=rich_console,
  ) as progress:
    # connection_handler.cwd(str(remote_path.parent.name))
    connection_handler.voidcmd("TYPE I")

    remote_size = remote_size or connection_handler.size(remote_path.as_posix())

    # download file
    download_task = progress.add_task(f"Downloading {remote_path.name}", total=remote_size)
    with local_path.open("wb") as local_file:
      connection_handler.retrbinary(
        f"RETR {remote_path.as_posix()}", pbar_writer(progress, download_task, local_file)
      )

  return local_path


def archive_remote_file(
  connection_handler: FTP,
  remote_path: PurePosixPath,
) -> None:
  connection_handler.voidcmd("TYPE I")
  archive_loc = remote_path.parent / "Archive"

  # check if archive folder exists on remote
  if archive_loc.as_posix() not in connection_handler.nlst(remote_path.parent.as_posix()):
    connection_handler.mkd(archive_loc.as_posix())

  new_loc = archive_loc / remote_path.name

  connection_handler.rename(remote_path.as_posix(), new_loc.as_posix())


SAS_DESTINATION_FOLDER = PurePosixPath("/Incoming")
