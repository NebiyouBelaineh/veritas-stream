"""
api/services/document_service.py — Document file management.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
DOCUMENTS_DIR = _ROOT / "documents"


def list_documents_for_app(app_id: str) -> list[dict]:
    app_dir = DOCUMENTS_DIR / app_id
    if not app_dir.exists():
        return []
    files = []
    for f in sorted(app_dir.iterdir()):
        if f.is_file():
            stat = f.stat()
            files.append({
                "filename": f.name,
                "app_id": app_id,
                "file_path": str(f),
                "file_size_bytes": stat.st_size,
                "last_modified": datetime.fromtimestamp(stat.st_mtime),
            })
    return files


def list_all_app_document_indexes() -> list[dict]:
    if not DOCUMENTS_DIR.exists():
        return []
    indexes = []
    for app_dir in sorted(DOCUMENTS_DIR.iterdir()):
        if app_dir.is_dir():
            files = list_documents_for_app(app_dir.name)
            indexes.append({
                "app_id": app_dir.name,
                "document_count": len(files),
                "files": files,
            })
    return indexes


async def save_upload(app_id: str, filename: str, content: bytes) -> dict:
    app_dir = DOCUMENTS_DIR / app_id
    app_dir.mkdir(parents=True, exist_ok=True)
    dest = app_dir / filename
    dest.write_bytes(content)
    stat = dest.stat()
    return {
        "filename": filename,
        "app_id": app_id,
        "file_path": str(dest),
        "file_size_bytes": stat.st_size,
        "last_modified": datetime.fromtimestamp(stat.st_mtime),
    }
