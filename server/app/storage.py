import ipaddress
import os
import re
import sqlite3
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Settings
from .models import DDCRecord

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "utm_id", "utm_name",
    "utm_creative_format", "utm_marketing_tactic", "gclid", "dclid", "gbraid", "wbraid", "fbclid",
    "msclkid", "mc_cid", "mc_eid", "igshid", "yclid", "_hsenc", "_hsmi", "vero_id",
}
LOCAL_HOSTNAMES = {"localhost", "localhost.localdomain", "0.0.0.0"}
LOCAL_DOMAIN_SUFFIXES = (".localhost", ".local", ".test", ".invalid", ".internal", ".lan", ".home")
PRIVATE_PAGE_DOMAINS = {
    "app.slack.com", "calendar.google.com", "bard.google.com", "chat.openai.com", "chat.qwen.ai", "chatgpt.com",
    "claude.ai", "copilot.microsoft.com", "discord.com", "docs.google.com", "drive.google.com", "dropbox.com",
    "figma.com", "github.com", "gitlab.com", "huggingface.co", "icloud.com", "gemini.google.com", "linear.app",
    "mail.google.com", "miro.com", "notion.so", "perplexity.ai", "poe.com", "qwen.ai", "trello.com", "www.notion.so",
}

SCHEMA = pa.schema([
    pa.field("domain", pa.string(), nullable=False),
    pa.field("url", pa.string(), nullable=False),
    pa.field("title", pa.string(), nullable=False),
    pa.field("discovered_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("keywords", pa.list_(pa.string()), nullable=False),
])

@dataclass
class StoreResult:
    accepted: int
    duplicates: int
    rejected: int
    stored: int
    shard_files: List[str]

def is_tracking_param(name: str) -> bool:
    normalized = name.strip().lower()
    return normalized.startswith("utm_") or normalized in TRACKING_PARAMS

def is_local_or_private_hostname(hostname: str) -> bool:
    normalized = hostname.strip("[]").lower().rstrip(".")
    if not normalized:
        return True
    if normalized in LOCAL_HOSTNAMES or normalized.endswith(LOCAL_DOMAIN_SUFFIXES):
        return True
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    return any([
        address.is_loopback,
        address.is_private,
        address.is_link_local,
        address.is_reserved,
        address.is_multicast,
        address.is_unspecified,
    ])

def is_private_page_domain(hostname: str) -> bool:
    normalized = hostname.lower().rstrip(".")
    return any(normalized == domain or normalized.endswith(f".{domain}") for domain in PRIVATE_PAGE_DOMAINS)

def clean_url(raw_url: str) -> tuple[str, str]:
    parsed = urlsplit(raw_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("url must be an http(s) URL")
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        raise ValueError("url must include a hostname")
    if is_local_or_private_hostname(hostname):
        raise ValueError("local, private, and internal URLs are not collected")
    port = parsed.port
    netloc = hostname
    if port and not ((parsed.scheme == "http" and port == 80) or (parsed.scheme == "https" and port == 443)):
        netloc = f"{hostname}:{port}"
    query = urlencode(
        sorted((key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if not is_tracking_param(key)),
        doseq=True,
    )
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    return hostname, urlunsplit((parsed.scheme.lower(), netloc, path, query, ""))

def safe_domain_path(domain: str) -> str:
    return re.sub(r"[^a-z0-9.-]+", "_", domain.lower()).strip("._") or "unknown"

class LocalParquetStore:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.lock = threading.Lock()
        self.settings.resolved_data_dir.mkdir(parents=True, exist_ok=True)
        self.settings.dataset_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.settings.metadata_db_path)

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    url TEXT PRIMARY KEY,
                    domain TEXT NOT NULL,
                    title TEXT NOT NULL,
                    discovered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            connection.execute("""
                CREATE TABLE IF NOT EXISTS domain_shards (
                    domain TEXT PRIMARY KEY,
                    next_part INTEGER NOT NULL
                )
            """)
            connection.commit()

    def store_records(self, records: Iterable[DDCRecord]) -> StoreResult:
        with self.lock:
            accepted_records = []
            duplicates = 0
            rejected = 0
            batch_discovered_at = datetime.now(timezone.utc)
            with self._connect() as connection:
                for record in records:
                    try:
                        domain, cleaned_url = clean_url(record.url)
                    except ValueError:
                        rejected += 1
                        continue
                    if record.source == "page" and is_private_page_domain(domain):
                        rejected += 1
                        continue
                    normalized = record.model_copy(update={
                        "domain": domain,
                        "url": cleaned_url,
                        "discovered_at": batch_discovered_at,
                    })
                    discovered = batch_discovered_at.isoformat()
                    cursor = connection.execute("""
                        INSERT OR IGNORE INTO urls (url, domain, title, discovered_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (normalized.url, normalized.domain, normalized.title, discovered, discovered))
                    if cursor.rowcount == 0:
                        duplicates += 1
                        connection.execute("""
                            UPDATE urls
                            SET title = COALESCE(NULLIF(?, ''), title),
                                discovered_at = MAX(discovered_at, ?),
                                updated_at = ?
                            WHERE url = ?
                        """, (normalized.title, discovered, discovered, normalized.url))
                        continue
                    accepted_records.append(normalized)
                connection.commit()
            shard_files = self._write_parquet(accepted_records)
            return StoreResult(
                accepted=len(accepted_records),
                duplicates=duplicates,
                rejected=rejected,
                stored=len(accepted_records),
                shard_files=shard_files,
            )

    def _write_parquet(self, records: List[DDCRecord]) -> List[str]:
        if not records:
            return []
        written_files = []
        by_domain: dict[str, list[DDCRecord]] = defaultdict(list)
        for record in records:
            by_domain[record.domain].append(record)
        for domain, domain_records in by_domain.items():
            table = self._records_to_table(domain_records)
            shard_path = self._write_domain_table(domain, table)
            written_files.append(str(shard_path.relative_to(self.settings.dataset_dir)))
        return written_files

    def _records_to_table(self, records: List[DDCRecord]) -> pa.Table:
        return pa.Table.from_arrays([
            pa.array([record.domain for record in records], type=pa.string()),
            pa.array([record.url for record in records], type=pa.string()),
            pa.array([record.title for record in records], type=pa.string()),
            pa.array([record.discovered_at.astimezone(timezone.utc) for record in records], type=pa.timestamp("us", tz="UTC")),
            pa.array([record.keywords for record in records], type=pa.list_(pa.string())),
        ], schema=SCHEMA)

    def _write_domain_table(self, domain: str, table: pa.Table) -> Path:
        domain_dir = self.settings.dataset_dir / f"domain_key={safe_domain_path(domain)}"
        domain_dir.mkdir(parents=True, exist_ok=True)
        temp_path = domain_dir / f".incoming-{os.getpid()}-{threading.get_ident()}.parquet"
        pq.write_table(table, temp_path, compression="zstd")
        target_path = self._select_shard_path(domain, domain_dir, temp_path.stat().st_size)
        if target_path.exists() and target_path.stat().st_size + temp_path.stat().st_size <= self.settings.max_shard_bytes:
            existing = pq.read_table(target_path, schema=SCHEMA)
            merged = pa.concat_tables([existing, table], promote_options="default")
            merged_path = target_path.with_suffix(".parquet.tmp")
            pq.write_table(merged, merged_path, compression="zstd")
            os.replace(merged_path, target_path)
            temp_path.unlink(missing_ok=True)
        else:
            os.replace(temp_path, target_path)
        return target_path

    def _select_shard_path(self, domain: str, domain_dir: Path, incoming_size: int) -> Path:
        parts = sorted(domain_dir.glob("part-*.parquet"))
        latest = parts[-1] if parts else None
        if latest and latest.stat().st_size + incoming_size <= self.settings.max_shard_bytes:
            return latest
        return self._allocate_shard_path(domain, domain_dir, parts)

    def _allocate_shard_path(self, domain: str, domain_dir: Path, parts: List[Path]) -> Path:
        max_existing_part = -1
        for path in parts:
            try:
                max_existing_part = max(max_existing_part, int(path.stem.split("-")[-1]))
            except ValueError:
                continue
        with self._connect() as connection:
            connection.execute("INSERT OR IGNORE INTO domain_shards (domain, next_part) VALUES (?, ?)", (domain, max_existing_part + 1))
            row = connection.execute("SELECT next_part FROM domain_shards WHERE domain = ?", (domain,)).fetchone()
            next_part = max(int(row[0]), max_existing_part + 1)
            connection.execute("UPDATE domain_shards SET next_part = ? WHERE domain = ?", (next_part + 1, domain))
            connection.commit()
        return domain_dir / f"part-{next_part:06d}.parquet"

    def has_pending_dataset_files(self) -> bool:
        return self.settings.dataset_dir.exists() and any(self.settings.dataset_dir.rglob("*.parquet"))

    def clear_dataset_files(self) -> int:
        deleted = 0
        if not self.settings.dataset_dir.exists():
            return deleted
        for path in sorted(self.settings.dataset_dir.rglob("*.parquet")):
            path.unlink(missing_ok=True)
            deleted += 1
        for directory in sorted((path for path in self.settings.dataset_dir.rglob("*") if path.is_dir()), reverse=True):
            try:
                directory.rmdir()
            except OSError:
                pass
        return deleted
