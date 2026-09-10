import ipaddress
import os
import re
import shutil
import sqlite3
import threading
import uuid
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


@dataclass(frozen=True)
class SyncBatch:
    path: Path
    from_live_dataset: bool


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
        self.sync_staging_dir.mkdir(parents=True, exist_ok=True)
        self.write_staging_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @property
    def sync_staging_dir(self) -> Path:
        return self.settings.resolved_data_dir / "sync-staging"

    @property
    def write_staging_dir(self) -> Path:
        return self.settings.resolved_data_dir / "write-staging"

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
        """Persist one ingestion batch without committing dedupe metadata early.

        New Parquet files are prepared under a transaction-specific staging
        directory. They are moved into the live dataset only after every
        domain file has been written successfully. SQLite dedupe metadata is
        committed last. Any ordinary exception before commit removes all new
        Parquet files and rolls the SQLite transaction back, so retrying the
        same records cannot turn missing data into permanent duplicates.
        """
        with self.lock:
            accepted_records: list[DDCRecord] = []
            duplicate_updates: list[tuple[str, str, str]] = []
            duplicates = 0
            rejected = 0
            installed_files: list[Path] = []
            batch_discovered_at = datetime.now(timezone.utc)

            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
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
                    existing = connection.execute(
                        "SELECT 1 FROM urls WHERE url = ?",
                        (normalized.url,),
                    ).fetchone()
                    if existing is not None:
                        duplicates += 1
                        duplicate_updates.append((normalized.title, discovered, normalized.url))
                        continue
                    accepted_records.append(normalized)

                shard_files, installed_files = self._write_parquet_transactional(accepted_records)

                for normalized in accepted_records:
                    discovered = normalized.discovered_at.isoformat()
                    connection.execute("""
                        INSERT INTO urls (url, domain, title, discovered_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        normalized.url,
                        normalized.domain,
                        normalized.title,
                        discovered,
                        discovered,
                    ))

                for title, discovered, url in duplicate_updates:
                    connection.execute("""
                        UPDATE urls
                        SET title = COALESCE(NULLIF(?, ''), title),
                            discovered_at = MAX(discovered_at, ?),
                            updated_at = ?
                        WHERE url = ?
                    """, (title, discovered, discovered, url))

                connection.commit()
            except Exception:
                connection.rollback()
                for path in installed_files:
                    path.unlink(missing_ok=True)
                self._remove_empty_directories(self.settings.dataset_dir)
                raise
            finally:
                connection.close()

            return StoreResult(
                accepted=len(accepted_records),
                duplicates=duplicates,
                rejected=rejected,
                stored=len(accepted_records),
                shard_files=shard_files,
            )

    def _write_parquet_transactional(self, records: List[DDCRecord]) -> tuple[List[str], List[Path]]:
        """Prepare all new shards first, then install them as immutable files.

        A fresh file is created per domain for this ingestion batch. Avoiding
        in-place merges means rollback can safely remove only files created by
        the current transaction without restoring older dataset content.
        """
        if not records:
            return [], []

        transaction_id = uuid.uuid4().hex
        transaction_dir = self.write_staging_dir / f"txn-{transaction_id}"
        prepared: list[tuple[Path, Path]] = []
        installed: list[Path] = []
        by_domain: dict[str, list[DDCRecord]] = defaultdict(list)
        for record in records:
            by_domain[record.domain].append(record)

        try:
            for domain, domain_records in by_domain.items():
                table = self._records_to_table(domain_records)
                domain_key = safe_domain_path(domain)
                staged_dir = transaction_dir / f"domain_key={domain_key}"
                staged_dir.mkdir(parents=True, exist_ok=True)
                staged_path = staged_dir / f"part-{transaction_id}.parquet"
                pq.write_table(table, staged_path, compression="zstd")

                target_dir = self.settings.dataset_dir / f"domain_key={domain_key}"
                target_path = target_dir / f"part-{transaction_id}.parquet"
                prepared.append((staged_path, target_path))

            for staged_path, target_path in prepared:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(staged_path, target_path)
                installed.append(target_path)
        except Exception:
            for target_path in installed:
                target_path.unlink(missing_ok=True)
            raise
        finally:
            shutil.rmtree(transaction_dir, ignore_errors=True)
            self._remove_empty_directories(self.write_staging_dir)

        return (
            [str(path.relative_to(self.settings.dataset_dir)) for path in installed],
            installed,
        )

    def _write_parquet(self, records: List[DDCRecord]) -> List[str]:
        """Compatibility wrapper used by tests and local callers."""
        shard_files, _ = self._write_parquet_transactional(records)
        return shard_files

    def _records_to_table(self, records: List[DDCRecord]) -> pa.Table:
        return pa.Table.from_arrays([
            pa.array([record.domain for record in records], type=pa.string()),
            pa.array([record.url for record in records], type=pa.string()),
            pa.array([record.title for record in records], type=pa.string()),
            pa.array([record.discovered_at.astimezone(timezone.utc) for record in records], type=pa.timestamp("us", tz="UTC")),
            pa.array([record.keywords for record in records], type=pa.list_(pa.string())),
        ], schema=SCHEMA)

    def has_pending_dataset_files(self) -> bool:
        with self.lock:
            return self._has_parquet(self.settings.dataset_dir) or any(
                self._has_parquet(path) for path in self._staged_batch_dirs()
            )

    def acquire_sync_batch(self) -> SyncBatch | None:
        """Return an immutable directory for one upload attempt.

        Failed batches remain staged and are retried before live data. When no
        staged batch exists, all currently completed Parquet files are moved
        under the same lock used by writers. New writes therefore land in a
        fresh live dataset and can never be deleted with the in-flight batch.
        """
        with self.lock:
            staged = self._staged_batch_dirs()
            if staged:
                return SyncBatch(staged[0], from_live_dataset=False)

            parquet_files = sorted(self.settings.dataset_dir.rglob("*.parquet"))
            if not parquet_files:
                return None

            batch_dir = self.sync_staging_dir / f"batch-{uuid.uuid4().hex}"
            for source in parquet_files:
                relative = source.relative_to(self.settings.dataset_dir)
                target = batch_dir / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(source, target)

            self._remove_empty_directories(self.settings.dataset_dir)
            self.settings.dataset_dir.mkdir(parents=True, exist_ok=True)
            return SyncBatch(batch_dir, from_live_dataset=True)

    def complete_sync_batch(self, batch: Path) -> int:
        """Delete only the immutable batch that was confirmed uploaded."""
        with self.lock:
            count = sum(1 for _ in batch.rglob("*.parquet")) if batch.exists() else 0
            shutil.rmtree(batch, ignore_errors=True)
            return count

    def restore_sync_batch(self, batch: Path) -> int:
        """Move an uploaded batch back to live storage when deletion is disabled."""
        with self.lock:
            if not batch.exists():
                return 0
            moved = 0
            for source in sorted(batch.rglob("*.parquet")):
                relative = source.relative_to(batch)
                target = self.settings.dataset_dir / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    raise RuntimeError(f"sync batch restore collision: {relative}")
                os.replace(source, target)
                moved += 1
            shutil.rmtree(batch, ignore_errors=True)
            return moved

    def _staged_batch_dirs(self) -> list[Path]:
        if not self.sync_staging_dir.exists():
            return []
        return sorted(
            (path for path in self.sync_staging_dir.iterdir() if path.is_dir() and self._has_parquet(path)),
            key=lambda path: path.name,
        )

    @staticmethod
    def _has_parquet(directory: Path) -> bool:
        return directory.exists() and any(directory.rglob("*.parquet"))

    @staticmethod
    def _remove_empty_directories(root: Path) -> None:
        if not root.exists():
            return
        for directory in sorted((path for path in root.rglob("*") if path.is_dir()), reverse=True):
            try:
                directory.rmdir()
            except OSError:
                pass
