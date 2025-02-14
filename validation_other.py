if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from decimal import Decimal
from logging import getLogger
from typing import Annotated

from pydantic import BeforeValidator
from validation_config import CustomBaseModel
from validators_shared import map_to_upca

logger = getLogger(__name__)


class BulkRateModel(CustomBaseModel):
  ItemNum: Annotated[str, BeforeValidator(map_to_upca)]
  Bulk_Price: Decimal
  Bulk_Quan: Decimal
