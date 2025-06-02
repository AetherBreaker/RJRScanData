from typing import Annotated

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from utils import CWD


class Settings(BaseSettings):
  model_config = SettingsConfigDict(
    env_file=CWD / ".env",
    env_file_encoding="utf-8",
    env_ignore_empty=True,
  )
  week_shift: Annotated[int, Field(alias="WEEK_SHIFT")] = 0
  testing_stores: Annotated[list[int], Field(alias="TESTING_STORES")] = []
  test_file: Annotated[bool, Field(alias="TEST_FILE")] = False


SETTINGS = Settings()
