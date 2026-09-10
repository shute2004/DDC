from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware

from .config import get_settings
from .models import IngestRequest, IngestResponse
from .storage import LocalParquetStore
from .sync_manager import AutoSyncManager

settings = get_settings()
store = LocalParquetStore(settings)
sync_manager = AutoSyncManager(settings, store)

app = FastAPI(
    title="DDC Relay API",
    version="0.1.0",
    description="Receives DDC browser-extension records and stores them as domain-sharded Parquet files.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"chrome-extension://.*|http://localhost(:\d+)?|http://127\.0\.0\.1(:\d+)?",
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)

@app.get("/health")
def health() -> dict:
    return {
        "ok": True,
        "dataset_dir": str(settings.dataset_dir),
        "huggingface_sync_enabled": settings.enable_hf_sync,
        "huggingface_repo_configured": bool(settings.hf_repo_id),
        "huggingface_token_configured": bool(settings.hf_token),
        "auto_sync": sync_manager.status(),
    }

@app.post("/ingest", response_model=IngestResponse)
async def ingest(payload: IngestRequest) -> IngestResponse:
    result = await run_in_threadpool(store.store_records, payload.records)
    sync_scheduled = sync_manager.record_ingested(result.stored)
    return IngestResponse(
        accepted=result.accepted,
        duplicates=result.duplicates,
        rejected=result.rejected,
        stored=result.stored,
        shard_files=result.shard_files,
        sync_scheduled=sync_scheduled,
    )

@app.post("/admin/sync-huggingface")
async def sync_huggingface() -> dict:
    try:
        return await run_in_threadpool(sync_manager.sync_now, True)
    except RuntimeError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

@app.on_event("startup")
def start_auto_sync() -> None:
    sync_manager.start()

@app.on_event("shutdown")
def stop_auto_sync() -> None:
    sync_manager.stop()
