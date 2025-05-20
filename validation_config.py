if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

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
  from types_column_names import ColNameEnum, ItemizedInvoiceCols
  from types_custom import ModelContextType

logger = getLogger(__name__)


VALIDATION_FAILED_CHECK_CONSTANT = "VALIDATION_FAILED"


class ValidationErrPackage(NamedTuple):
  field_value: Any
  err: ValidationError


class CustomBaseModel(BaseModel):
  field_name_lookup: ClassVar[dict["ItemizedInvoiceCols", type["ColNameEnum"]]] = {}  # noqa: F821
  model_config = ConfigDict(
    populate_by_name=True,
    use_enum_values=True,
    validate_default=True,
    validate_assignment=True,
    coerce_numbers_to_str=True,
  )

  @classmethod
  def lookup_field(cls, field_name: "ColNameEnum") -> str:
    """Lookup the field name in the _field_name_lookup dictionary."""
    return cls.field_name_lookup.get(field_name, field_name)

  @field_validator("*", mode="wrap", check_fields=False)
  @classmethod
  def log_failed_field_validations(
    cls, data: str, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
  ) -> Any:
    results = VALIDATION_FAILED_CHECK_CONSTANT
    context: "ModelContextType" = info.context
    try:
      results = handler(data)
    except Exception as e:
      exc_type, exc_val, exc_tb = type(e), e, e.__traceback__

      # if the exception is a ValidationError...
      if isinstance(e, ValidationError):
        context["row_err"][info.field_name] = ValidationErrPackage(field_value=data, err=e)

        if not context["skip"]:
          skip_fields = context.get("skip_fields", {})

          skipcheck_func = skip_fields.get(info.field_name)
          is_skip_field = info.field_name in skip_fields

          do_skip = is_skip_field and skipcheck_func(data) if skipcheck_func else is_skip_field

          if do_skip:
            context["skip"] = True
          else:
            pass
      else:
        logger.error(
          f"Error validating {info.field_name} in {cls.__name__}: {e}",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )

    return data if results is VALIDATION_FAILED_CHECK_CONSTANT else results

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
