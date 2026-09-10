from datetime import datetime
from typing import List, Literal, Optional
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DDCRecord(BaseModel):
    domain: str = Field(min_length=1, max_length=253)
    url: str = Field(min_length=1, max_length=4096)
    title: str = Field(default="", max_length=300)
    discovered_at: datetime
    keywords: List[str] = Field(default_factory=list, max_length=256)
    source: Optional[Literal["page", "search_result"]] = None

    model_config = ConfigDict(extra="ignore")

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("url must be an http(s) URL")
        return value

    @field_validator("domain")
    @classmethod
    def normalize_domain(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("title")
    @classmethod
    def normalize_title(cls, value: str) -> str:
        return " ".join(str(value or "").split())[:300]

    @field_validator("keywords")
    @classmethod
    def normalize_keywords(cls, value: List[str]) -> List[str]:
        keywords = []
        seen = set()
        for item in value:
            keyword = " ".join(str(item).split()).strip().lower()[:128]
            if not keyword or keyword in seen:
                continue
            seen.add(keyword)
            keywords.append(keyword)
            if len(keywords) >= 128:
                break
        return keywords


class IngestRequest(BaseModel):
    records: List[DDCRecord] = Field(min_length=1, max_length=1000)


class IngestResponse(BaseModel):
    accepted: int
    duplicates: int
    rejected: int
    stored: int
    shard_files: List[str]
    sync_scheduled: bool = False
