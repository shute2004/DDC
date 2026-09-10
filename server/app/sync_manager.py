import threading
import time
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from .hf_sync import sync_to_huggingface
from .storage import LocalParquetStore


class AutoSyncManager:
    def __init__(self, settings: Settings, store: LocalParquetStore):
        self.settings = settings
        self.store = store
        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._pending_records = 0
        self._last_sync_at: datetime | None = None
        self._last_error: str | None = None
        self._last_result: dict[str, Any] | None = None
        self._syncing = False

    @property
    def is_configured(self) -> bool:
        return bool(
            self.settings.auto_sync_enabled
            and self.settings.enable_hf_sync
            and self.settings.hf_repo_id
            and self.settings.hf_token
        )

    def start(self) -> None:
        if not self.is_configured or self._thread:
            return
        self._thread = threading.Thread(target=self._run, name="ddc-auto-hf-sync", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._condition:
            self._condition.notify_all()
        if self._thread:
            self._thread.join(timeout=5)

    def record_ingested(self, stored_records: int) -> bool:
        if stored_records <= 0 or not self.is_configured:
            return False
        with self._condition:
            self._pending_records += stored_records
            should_trigger = self._pending_records >= self.settings.auto_sync_min_records
            if should_trigger:
                self._condition.notify_all()
            return should_trigger

    def sync_now(self, force: bool = False) -> dict[str, Any]:
        if not self.is_configured:
            return {
                "skipped": "not_configured",
                "auto_sync_enabled": self.settings.auto_sync_enabled,
                "huggingface_sync_enabled": self.settings.enable_hf_sync,
                "huggingface_repo_configured": bool(self.settings.hf_repo_id),
                "huggingface_token_configured": bool(self.settings.hf_token),
            }
        with self._condition:
            if self._syncing:
                return {"skipped": "already_syncing"}
            self._syncing = True
        try:
            if not self.store.has_pending_dataset_files():
                result: dict[str, Any] = {"skipped": "no_local_parquet_files"}
            elif not force and not self._interval_elapsed() and self._pending_records < self.settings.auto_sync_min_records:
                result = {"skipped": "waiting_for_interval_or_record_threshold"}
            else:
                result = sync_to_huggingface(self.settings)
                if result.get("uploaded_files", 0) > 0 and self.settings.delete_local_after_sync:
                    result["deleted_local_files"] = self.store.clear_dataset_files()
                self._pending_records = 0
                self._last_sync_at = datetime.now(timezone.utc)
            self._last_result = result
            self._last_error = None
            return result
        except Exception as error:
            self._last_error = str(error)
            raise
        finally:
            with self._condition:
                self._syncing = False

    def status(self) -> dict[str, Any]:
        return {
            "configured": self.is_configured,
            "syncing": self._syncing,
            "pending_records": self._pending_records,
            "pending_local_files": self.store.has_pending_dataset_files(),
            "delete_local_after_sync": self.settings.delete_local_after_sync,
            "min_interval_seconds": self.settings.auto_sync_min_interval_seconds,
            "min_records": self.settings.auto_sync_min_records,
            "last_sync_at": self._last_sync_at.isoformat() if self._last_sync_at else None,
            "last_error": self._last_error,
            "last_result": self._last_result,
        }

    def _interval_elapsed(self) -> bool:
        if self._last_sync_at is None:
            return True
        elapsed = datetime.now(timezone.utc) - self._last_sync_at
        return elapsed.total_seconds() >= self.settings.auto_sync_min_interval_seconds

    def _run(self) -> None:
        interval = max(30, self.settings.auto_sync_min_interval_seconds)
        while not self._stop_event.is_set():
            with self._condition:
                self._condition.wait(timeout=interval)
            if self._stop_event.is_set():
                break
            try:
                self.sync_now(force=False)
            except Exception:
                time.sleep(min(interval, 60))
