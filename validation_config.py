if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from collections.abc import Callable
from dataclasses import dataclass
from inspect import get_annotations
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, Self

from pydantic import (
  BaseModel,
  ConfigDict,
  ModelWrapValidatorHandler,
  ValidationError,
  ValidationInfo,
  ValidatorFunctionWrapHandler,
  field_validator,
  model_validator,
)

if TYPE_CHECKING:
  from types_custom import ModelContextType

logger = getLogger(__name__)


VALIDATION_FAILED_CHECK_CONSTANT = "VALIDATION_FAILED"


class ValidationErrPackage(NamedTuple):
  field_value: Any
  err: ValidationError


@dataclass
class ReportingFieldInfo:
  report_field: bool = True
  remove_row_if_error: bool = True
  dont_report_if: Callable[[Any], bool] | None = None
  dont_remove_if: Callable[[Any], bool] | None = None
  force_remove: bool = False


class CustomBaseModel(BaseModel):
  remove_bad_rows: ClassVar[bool] = False
  model_config = ConfigDict(
    populate_by_name=True,
    use_enum_values=True,
    validate_default=True,
    validate_assignment=True,
    coerce_numbers_to_str=True,
  )

  @field_validator("*", mode="wrap", check_fields=False)
  @classmethod
  def log_failed_field_validations(cls, data: str, handler: ValidatorFunctionWrapHandler, info: ValidationInfo) -> Any:
    results = VALIDATION_FAILED_CHECK_CONSTANT
    context: "ModelContextType" = info.context

    annos = get_annotations(cls)

    anno = annos[info.field_name]

    reporting_meta: ReportingFieldInfo = (
      next((item for item in anno.__metadata__ if isinstance(item, ReportingFieldInfo)), ReportingFieldInfo())
      if anno and hasattr(anno, "__metadata__")
      else ReportingFieldInfo()
    )

    context["remove_row"][info.field_name] = cls.remove_bad_rows or reporting_meta.force_remove

    try:
      results = handler(data)
      context["remove_row"][info.field_name] = False
    except Exception as e:
      exc_type, exc_val, exc_tb = type(e), e, e.__traceback__

      # if the exception is a ValidationError...
      if isinstance(e, ValidationError):
        dont_report_if_func = reporting_meta.dont_report_if
        report_if = reporting_meta.report_field or not dont_report_if_func if dont_report_if_func else reporting_meta.report_field
        if report_if:
          context["row_err"][info.field_name] = ValidationErrPackage(field_value=data, err=e)

        if context["remove_row"]:
          dont_remove_check_func = reporting_meta.dont_remove_if
          remove_row = reporting_meta.remove_row_if_error

          dont_remove = (not remove_row) or dont_remove_check_func(data) if dont_remove_check_func else not remove_row

          if dont_remove:
            context["remove_row"][info.field_name] = False
      else:
        logger.error(
          f"Error validating {info.field_name} in {cls.__name__}: {e}",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )

    return data if results is VALIDATION_FAILED_CHECK_CONSTANT else results

  @model_validator(mode="wrap")
  @classmethod
  def log_failed_validation(cls, data: Any, handler: ModelWrapValidatorHandler[Self], info: ValidationInfo) -> Self:
    results = VALIDATION_FAILED_CHECK_CONSTANT
    context: "ModelContextType" = info.context
    context["remove_row"][info.field_name] = cls.remove_bad_rows

    try:
      results = handler(data)
      context["remove_row"][info.field_name] = False
    except ValidationError as e:
      exc_type, exc_val, exc_tb = type(e), e, e.__traceback__
      field_name = info.field_name

      if not field_name:
        errs = exc_val.errors()
        for err in errs:
          loc = err["loc"]
          if loc[0].lower() in ["inv_price", "price"]:
            field_name = "price"
          elif loc[0].casefold() == "Manufacturer_Multipack_Desc".casefold():
            field_name = "manufacturer_multipack_desc"

      reporting_meta: ReportingFieldInfo = next(
        (item for item in cls.__annotations__[field_name].__metadata__ if isinstance(item, ReportingFieldInfo)),
        ReportingFieldInfo(),
      )

      # if the exception is a ValidationError...
      if isinstance(e, ValidationError):
        dont_report_if_func = reporting_meta.dont_report_if
        report_if = reporting_meta.report_field or not dont_report_if_func if dont_report_if_func else reporting_meta.report_field
        if report_if:
          context["row_err"][info.field_name] = ValidationErrPackage(field_value=data, err=e)

        if context["remove_row"]:
          dont_remove_check_func = reporting_meta.dont_remove_if
          remove_row = reporting_meta.remove_row_if_error

          dont_remove = (not remove_row) or dont_remove_check_func(data) if dont_remove_check_func else not remove_row

          if dont_remove:
            context["remove_row"][info.field_name] = False
      else:
        logger.error(
          f"Error validating {cls.__name__}: {e}",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )

    return data if results is VALIDATION_FAILED_CHECK_CONSTANT else results
