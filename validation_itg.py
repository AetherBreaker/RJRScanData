if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from logging import getLogger

from validation_config import CustomBaseModel

logger = getLogger(__name__)


class ITGValidationModel(CustomBaseModel):
  pass
