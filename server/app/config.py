from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    data_dir: Path = Field(default=Path("data"), validation_alias=AliasChoices("DDC_DATA_DIR", "DATA_DIR"))
    max_shard_bytes: int = Field(default=50 * 1024 * 1024, validation_alias=AliasChoices("DDC_MAX_SHARD_BYTES"))
    enable_hf_sync: bool = Field(default=False, validation_alias=AliasChoices("DDC_ENABLE_HF_SYNC"))
    auto_sync_enabled: bool = Field(default=True, validation_alias=AliasChoices("DDC_AUTO_SYNC_ENABLED"))
    auto_sync_min_interval_seconds: int = Field(default=180, validation_alias=AliasChoices("DDC_AUTO_SYNC_MIN_INTERVAL_SECONDS"))
    auto_sync_min_records: int = Field(default=10, validation_alias=AliasChoices("DDC_AUTO_SYNC_MIN_RECORDS"))
    delete_local_after_sync: bool = Field(default=True, validation_alias=AliasChoices("DDC_DELETE_LOCAL_AFTER_SYNC"))
    hf_repo_id: Optional[str] = Field(default=None, validation_alias=AliasChoices("DDC_HF_REPO_ID", "HF_REPO_ID"))
    hf_repo_type: str = Field(default="dataset", validation_alias=AliasChoices("DDC_HF_REPO_TYPE", "HF_REPO_TYPE"))
    hf_path_in_repo: str = Field(default="data", validation_alias=AliasChoices("DDC_HF_PATH_IN_REPO"))
    hf_token: Optional[str] = Field(default=None, validation_alias=AliasChoices("HF_TOKEN", "DDC_HF_TOKEN"))

    model_config = SettingsConfigDict(
        env_file=(".env", "server/.env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def resolved_data_dir(self) -> Path:
        if self.data_dir.is_absolute():
            return self.data_dir
        return REPO_ROOT / self.data_dir

    @property
    def dataset_dir(self) -> Path:
        return self.resolved_data_dir / "dataset"

    @property
    def metadata_db_path(self) -> Path:
        return self.resolved_data_dir / "metadata.sqlite3"


@lru_cache
def get_settings() -> Settings:
    return Settings()
