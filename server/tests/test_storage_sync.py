import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from server.app.models import DDCRecord
from server.app.storage import LocalParquetStore
from server.app.sync_manager import AutoSyncManager


def make_settings(root: Path):
    data_dir = root / "data"
    return SimpleNamespace(
        resolved_data_dir=data_dir,
        dataset_dir=data_dir / "dataset",
        metadata_db_path=data_dir / "metadata.sqlite3",
        max_shard_bytes=50 * 1024 * 1024,
        auto_sync_enabled=True,
        enable_hf_sync=True,
        auto_sync_min_interval_seconds=180,
        auto_sync_min_records=1,
        delete_local_after_sync=True,
        hf_repo_id="example/ddc-test",
        hf_repo_type="dataset",
        hf_path_in_repo="data",
        hf_token="test-token",
    )


def record(url: str, *, source: str = "search_result") -> DDCRecord:
    return DDCRecord(
        domain="placeholder.invalid",
        url=url,
        title="Example",
        discovered_at=datetime.now(timezone.utc),
        keywords=["example"],
        source=source,
    )


class StorageTests(unittest.TestCase):
    def test_deduplicates_urls_and_writes_parquet(self):
        with tempfile.TemporaryDirectory() as temp:
            store = LocalParquetStore(make_settings(Path(temp)))
            first = store.store_records([record("https://example.com/page?utm_source=test&id=1")])
            second = store.store_records([record("https://example.com/page?id=1")])

            self.assertEqual(first.stored, 1)
            self.assertEqual(second.duplicates, 1)
            self.assertEqual(second.stored, 0)
            self.assertEqual(len(list(store.settings.dataset_dir.rglob("*.parquet"))), 1)

    def test_rejects_private_page_domains(self):
        with tempfile.TemporaryDirectory() as temp:
            store = LocalParquetStore(make_settings(Path(temp)))
            result = store.store_records([record("https://github.com/private/project", source="page")])

            self.assertEqual(result.rejected, 1)
            self.assertEqual(result.stored, 0)
            self.assertEqual(list(store.settings.dataset_dir.rglob("*.parquet")), [])

    def test_failed_batch_remains_staged_for_retry(self):
        with tempfile.TemporaryDirectory() as temp:
            store = LocalParquetStore(make_settings(Path(temp)))
            store.store_records([record("https://example.com/first")])

            batch = store.acquire_sync_batch()
            self.assertIsNotNone(batch)
            assert batch is not None
            self.assertTrue(list(batch.path.rglob("*.parquet")))
            self.assertEqual(list(store.settings.dataset_dir.rglob("*.parquet")), [])

            retry = store.acquire_sync_batch()
            self.assertIsNotNone(retry)
            assert retry is not None
            self.assertEqual(retry.path, batch.path)
            self.assertFalse(retry.from_live_dataset)

    def test_new_records_during_upload_are_not_deleted_with_uploaded_batch(self):
        with tempfile.TemporaryDirectory() as temp:
            settings = make_settings(Path(temp))
            store = LocalParquetStore(settings)
            manager = AutoSyncManager(settings, store)

            store.store_records([record("https://example.com/before-sync")])
            manager.record_ingested(1)

            upload_started = threading.Event()
            allow_upload_to_finish = threading.Event()
            result_holder = {}
            error_holder = []

            def fake_sync(_settings, dataset_dir):
                uploaded = list(Path(dataset_dir).rglob("*.parquet"))
                self.assertTrue(uploaded)
                upload_started.set()
                self.assertTrue(allow_upload_to_finish.wait(timeout=5))
                return {"uploaded_files": len(uploaded)}

            def run_sync():
                try:
                    result_holder["result"] = manager.sync_now(force=True)
                except Exception as error:  # pragma: no cover - diagnostic capture
                    error_holder.append(error)

            with patch("server.app.sync_manager.sync_to_huggingface", side_effect=fake_sync):
                thread = threading.Thread(target=run_sync)
                thread.start()
                self.assertTrue(upload_started.wait(timeout=5))

                # This record is written after the immutable batch was detached.
                store.store_records([record("https://example.com/during-sync")])
                manager.record_ingested(1)

                allow_upload_to_finish.set()
                thread.join(timeout=5)

            self.assertFalse(thread.is_alive())
            self.assertEqual(error_holder, [])
            self.assertGreater(result_holder["result"].get("deleted_staged_files", 0), 0)
            self.assertTrue(list(settings.dataset_dir.rglob("*.parquet")))
            self.assertEqual(manager.status()["pending_records"], 1)
            self.assertEqual(list(store.sync_staging_dir.rglob("*.parquet")), [])


if __name__ == "__main__":
    unittest.main()
