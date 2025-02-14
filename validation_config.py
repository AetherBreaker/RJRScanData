if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from logging import getLogger
from typing import Any, Self

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

logger = getLogger(__name__)


class CustomBaseModel(BaseModel):
  model_config = ConfigDict(
    populate_by_name=True,
    use_enum_values=True,
    validate_default=True,
    validate_assignment=True,
    coerce_numbers_to_str=True,
  )

  @field_validator("*", mode="wrap", check_fields=False)
  @classmethod
  def log_failed_field_validations(
    cls, data: str, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
  ) -> Any:
    results = None
    try:
      results = handler(data)
    except Exception as e:
      exc_type, exc_val, exc_tb = type(e), e, e.__traceback__

      # if the exception is a ValidationError...
      if isinstance(e, ValidationError):
        if isinstance(row_errs := info.context.get("row_err"), dict):
          row_errs[info.field_name] = (data, e)
      else:
        logger.error(
          f"Error validating {info.field_name} in {cls.__name__}: {e}",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )

    return results or data

  @model_validator(mode="wrap")
  @classmethod
  def log_failed_validation(
    cls, data: Any, handler: ModelWrapValidatorHandler[Self], info: ValidationInfo
  ) -> Self:
    try:
      return handler(data)
    except ValidationError as e:
      exc_type, exc_val, exc_tb = type(e), e, e.__traceback__

      pass

      logger.error(
        f"Error validating {cls.__name__}: {e}",
        exc_info=(exc_type, exc_val, exc_tb),
        stack_info=True,
      )
      raise
