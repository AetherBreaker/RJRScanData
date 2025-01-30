NULL_VALUES = ["NULL", "", " ", float("nan")]


def coerce_null_values(value: str) -> None:
  if value in NULL_VALUES:
    return None
  return value
