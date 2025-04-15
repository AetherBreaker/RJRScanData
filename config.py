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


SETTINGS = Settings()
