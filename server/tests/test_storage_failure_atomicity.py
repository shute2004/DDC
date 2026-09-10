import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from server.app.storage import LocalParquetStore
from server.tests.test_storage_sync import make_settings, record


class StorageFailureAtomicityTests(unittest.TestCase):
    def test_parquet_failure_does_not_commit_dedupe_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            settings = make_settings(Path(temp))
            store = LocalParquetStore(settings)
            records = [
                record("https://alpha.example/item"),
                record("https://beta.example/item"),
            ]

            from server.app import storage as storage_module

            original_write = storage_module.pq.write_table
            calls = 0

            def fail_second_write(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("simulated parquet I/O failure")
                return original_write(*args, **kwargs)

            with patch.object(storage_module.pq, "write_table", side_effect=fail_second_write):
                with self.assertRaises(OSError):
                    store.store_records(records)

            self.assertEqual(list(settings.dataset_dir.rglob("*.parquet")), [])
            with sqlite3.connect(settings.metadata_db_path) as connection:
                count = connection.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
            self.assertEqual(count, 0)

            retry = store.store_records(records)
            self.assertEqual(retry.stored, 2)
            self.assertEqual(retry.duplicates, 0)
            self.assertEqual(len(list(settings.dataset_dir.rglob("*.parquet"))), 2)

            duplicate_retry = store.store_records(records)
            self.assertEqual(duplicate_retry.stored, 0)
            self.assertEqual(duplicate_retry.duplicates, 2)

    def test_same_batch_duplicate_is_stored_once(self):
        with tempfile.TemporaryDirectory() as temp:
            settings = make_settings(Path(temp))
            store = LocalParquetStore(settings)

            result = store.store_records([
                record("https://example.com/page?utm_source=a&id=1"),
                record("https://example.com/page?id=1"),
            ])

            self.assertEqual(result.stored, 1)
            self.assertEqual(result.duplicates, 1)
            with sqlite3.connect(settings.metadata_db_path) as connection:
                count = connection.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
            self.assertEqual(count, 1)
            self.assertEqual(len(list(settings.dataset_dir.rglob("*.parquet"))), 1)


if __name__ == "__main__":
    unittest.main()
