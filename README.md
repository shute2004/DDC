# DDC

DDC (Distributed Datasets in Cyberspace) is a privacy-aware system for building flat web datasets without depending on search-engine ranking or visibility decisions.

The current implementation consists of two parts:

- **Chromium MV3 extension** — extracts URLs, titles, and keywords from search-result DOMs and from visible text on pages the user actually opens.
- **Local relay API** — validates, normalizes, deduplicates, and stores records as domain-partitioned Parquet data. Optional Hugging Face Datasets synchronization is handled server-side.

## Architecture

```text
extension/        Chromium MV3 collector
server/           FastAPI relay and storage backend
tests/            lightweight tests for shared normalization logic
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

## Hugging Face synchronization

Set the following values in `.env` to enable server-side synchronization:

```bash
DDC_ENABLE_HF_SYNC=true
DDC_HF_REPO_ID=your-name/your-dataset
HF_TOKEN=<your-hugging-face-token>
```

The token is never stored in the browser extension.

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
.venv/bin/python -m compileall server
```

## License

Source-visible, all rights reserved. See [LICENSE](LICENSE).
