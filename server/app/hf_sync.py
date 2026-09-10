from datetime import datetime, timezone

from huggingface_hub import HfApi

from .config import Settings, get_settings


def sync_to_huggingface(settings: Settings | None = None) -> dict:
    resolved = settings or get_settings()
    if not resolved.enable_hf_sync:
        raise RuntimeError("Hugging Face sync is disabled. Set DDC_ENABLE_HF_SYNC=true to enable it.")
    if not resolved.hf_token:
        raise RuntimeError("HF_TOKEN is not set.")
    if not resolved.hf_repo_id:
        raise RuntimeError("DDC_HF_REPO_ID is not set.")
    if not resolved.dataset_dir.exists():
        raise RuntimeError(f"Dataset directory does not exist: {resolved.dataset_dir}")

    parquet_files = sorted(resolved.dataset_dir.rglob("*.parquet"))
    if not parquet_files:
        return {
            "repo_id": resolved.hf_repo_id,
            "repo_type": resolved.hf_repo_type,
            "path_in_repo": resolved.hf_path_in_repo,
            "uploaded_files": 0,
            "skipped": "no_local_parquet_files",
        }

    commit_message = f"DDC dataset sync {datetime.now(timezone.utc).isoformat()}"
    api = HfApi(token=resolved.hf_token)
    api.upload_folder(
        repo_id=resolved.hf_repo_id,
        repo_type=resolved.hf_repo_type,
        folder_path=str(resolved.dataset_dir),
        path_in_repo=resolved.hf_path_in_repo,
        commit_message=commit_message,
    )

    return {
        "repo_id": resolved.hf_repo_id,
        "repo_type": resolved.hf_repo_type,
        "path_in_repo": resolved.hf_path_in_repo,
        "commit_message": commit_message,
        "uploaded_files": len(parquet_files),
    }
