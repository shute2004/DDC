# DDC

DDC (Distributed Datasets in Cyberspace) is a privacy-aware system for building flat web datasets without depending on search-engine ranking or visibility decisions.

The current implementation consists of two parts:

- **Chromium MV3 extension** — extracts URLs, titles, and keywords from search-result DOMs and from visible text on pages the user actually opens.
- **Local relay API** — validates, normalizes, deduplicates, and stores records as domain-partitioned Parquet data. Optional Hugging Face Datasets synchronization is handled server-side.

## Architecture

```text
extension/        Chromium MV3 collector
server/           FastAPI relay, SQLite metadata, Parquet storage, HF sync
tests/            browser-side normalization/keyword tests
server/tests/     storage and synchronization invariant tests
```

## Setup

```bash
cp .env.example .env
uv venv .venv
source .venv/bin/activate
uv pip install --python .venv/bin/python -r server/requirements.txt
uvicorn server.app.main:app --host 127.0.0.1 --port 8787 --reload
```

Load `extension/` as an unpacked extension from `chrome://extensions` with developer mode enabled. The default relay endpoint is `http://127.0.0.1:8787/ingest`.

**The relay is designed for loopback/local use.** The administrative Hugging Face sync endpoint is intentionally not an Internet-facing authenticated control plane. Do not bind this development relay to a public interface without adding an authentication and deployment boundary appropriate for that environment.

## Local storage consistency

DDC keeps deduplication metadata in SQLite and collected rows in Parquet. A record is not permanently marked as deduplicated before its Parquet batch has been written successfully.

For each ingestion batch, Parquet files are first prepared under a transaction-specific `write-staging/` directory. Every domain file must be written successfully before any of those new immutable files are installed into the live dataset. SQLite deduplication metadata is committed last. On an ordinary write or database exception before commit, the SQLite transaction is rolled back and any newly installed Parquet files from that ingestion batch are removed, so retrying the same records does not silently convert missing data into duplicates.

New ingestion batches create fresh immutable Parquet files rather than mutating an older live shard in place. This makes rollback of a failed ingestion batch local to files created by that batch.

## Hugging Face synchronization

Set the following values in `.env` to enable server-side synchronization:

```bash
DDC_ENABLE_HF_SYNC=true
DDC_HF_REPO_ID=your-name/your-dataset
HF_TOKEN=<your-hugging-face-token>
```

The token is never stored in the browser extension.

Before a network upload starts, completed Parquet files are moved under the same storage lock into an immutable `sync-staging/` batch. New records arriving while that batch is uploading remain in the live dataset and are not part of its post-upload cleanup. Failed staged batches are retained for retry.

## Privacy and collection constraints

- The extension does not background-fetch URLs discovered in search results.
- Form values from `input`, `textarea`, `select`, and similar fields are not collected.
- Page-body collection is skipped when password fields or `noindex` directives are present.
- Localhost, private-network, `.local`, and `.test` URLs are rejected.
- Common authenticated application domains are excluded from page-body collection.
- Raw page text is not stored in the extension queue; only normalized URLs, titles, and extracted keywords are queued.
- URL fragments and common tracking parameters are removed on both the extension and relay sides.

## Data shape

```json
{
  "domain": "example.com",
  "url": "https://example.com/page?id=1",
  "title": "Example Page",
  "discovered_at": "2026-05-26T00:00:00.000Z",
  "keywords": ["example", "page"],
  "source": "page"
}
```

## Tests

```bash
npm test
.venv/bin/python -m unittest discover -s server/tests -p 'test_*.py'
.venv/bin/python -m compileall server
```

The Python tests cover URL deduplication, Parquet output, private-page rejection, staged retry behavior, synchronization concurrent with new ingestion, and the regression case where a multi-domain Parquet write fails before SQLite deduplication metadata may commit.

GitHub Actions runs both the Python storage/synchronization tests and the browser-side Node tests when a runner is available.

## License

Source-visible, all rights reserved. See [LICENSE](LICENSE).
