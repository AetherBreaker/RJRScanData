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
  remove_bad_rows: ClassVar[bool] = False
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
  def log_failed_field_validations(cls, data: str, handler: ValidatorFunctionWrapHandler, info: ValidationInfo) -> Any:
    results = VALIDATION_FAILED_CHECK_CONSTANT
    context: "ModelContextType" = info.context
    context["remove_row"][info.field_name] = cls.remove_bad_rows

    try:
      results = handler(data)
      context["remove_row"][info.field_name] = False
    except Exception as e:
      exc_type, exc_val, exc_tb = type(e), e, e.__traceback__

      test = cls.__annotations__.get(info.field_name, None).__metadata__

      if info.field_name == "":
        pass

      # if the exception is a ValidationError...
      if isinstance(e, ValidationError):
        context["row_err"][info.field_name] = ValidationErrPackage(field_value=data, err=e)

        if context["remove_row"]:
          dont_remove_fields = context["fields_to_not_remove"]

          dont_remove_check_func = context["special_dont_remove_conditions"].get(info.field_name)
          is_dont_remove_field = info.field_name in dont_remove_fields

          dont_remove = is_dont_remove_field or dont_remove_check_func(data) if dont_remove_check_func else False

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
    context["remove_row"][info.field_name] = True

    try:
      results = handler(data)
      context["remove_row"][info.field_name] = False
    except ValidationError as e:
      exc_type, exc_val, exc_tb = type(e), e, e.__traceback__

      if isinstance(e, ValidationError):
        context["row_err"][info.field_name] = ValidationErrPackage(field_value=data, err=e)

        if context["remove_row"]:
          dont_remove_fields = context["fields_to_not_remove"]

          dont_remove_check_func = context["special_dont_remove_conditions"].get(info.field_name)
          is_dont_remove_field = info.field_name in dont_remove_fields

          dont_remove = is_dont_remove_field or dont_remove_check_func(data) if dont_remove_check_func else False

          if dont_remove:
            context["remove_row"][info.field_name] = False
      else:
        logger.error(
          f"Error validating {cls.__name__}: {e}",
          exc_info=(exc_type, exc_val, exc_tb),
          stack_info=True,
        )

    return data if results is VALIDATION_FAILED_CHECK_CONSTANT else results
