if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from logging import getLogger

from validation_result_rjr import FTXRJRValidationModel, RJRValidationModel

logger = getLogger(__name__)


PROMO_FIELDS = [
  "outlet_multipack_quantity",
  "outlet_multipack_discount_amt",
  "acct_promo_name",
  "acct_discount_amt",
  "manufacturer_discount_amt",
  "pid_coupon",
  "pid_coupon_discount_amt",
  "manufacturer_multipack_quantity",
  "manufacturer_multipack_discount_amt",
  "manufacturer_promo_desc",
  "manufacturer_multipack_desc",
  "coupon_desc",
]


# class ITGValidationModel(CustomBaseModel):
class ITGValidationModel(RJRValidationModel):
  """ITG Validation Model."""


# class FTXITGValidationModel(CustomBaseModel):
class FTXITGValidationModel(FTXRJRValidationModel):
  """FTX ITG Validation Model."""
