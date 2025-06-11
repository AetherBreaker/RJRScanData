if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from logging import getLogger
from pathlib import Path

from pandas import concat
from rich.progress import Progress
from types_custom import RowErrPackage

logger = getLogger(__name__)


def assemble_validation_error_report(
  pbar: Progress,
  validation_errors: list[RowErrPackage],
  err_type: str,
  output_path: Path,
) -> None:
  """
  Assemble a report of validation errors.

  Args:
      validation_errors (list): A list of validation error messages.

  Returns:
      str: A formatted string containing the validation error report.
  """
  if output_path.exists():
    output_path.unlink()
  if not validation_errors:
    logger.info("No validation errors found.")
    return

  new_rows = []

  process_errs_task = pbar.add_task(f"Processing {err_type} Validation Errors", total=len(validation_errors))

  for field_name, field_input, err_reason, row in validation_errors:
    errs = err_reason.errors(
      include_context=False,
      include_input=False,
      include_url=False,
    )

    for err in errs:
      row["err_field_name"] = field_name
      row["err_field_input"] = field_input
      row["err_reason"] = f"{err["type"]}: {err["msg"]}"
      new_rows.append(row)
    pbar.update(process_errs_task, advance=1)

  result = concat(new_rows, ignore_index=True, axis=1).T

  col_names = result.columns.tolist()

  col_names.insert(0, col_names.pop(col_names.index("err_reason")))
  col_names.insert(0, col_names.pop(col_names.index("err_field_input")))
  col_names.insert(0, col_names.pop(col_names.index("err_field_name")))

  result = result[col_names]

  result.to_csv(output_path, index=False)
