"""
api/routers/documents.py — Document file listing and upload.
"""
from __future__ import annotations

from fastapi import APIRouter, File, HTTPException, UploadFile

from api.models.responses import AppDocumentIndex, DocumentFileResponse
from api.services.document_service import (
    list_all_app_document_indexes,
    list_documents_for_app,
    save_upload,
)

router = APIRouter(tags=["documents"])


def _file_dict_to_response(d: dict) -> DocumentFileResponse:
    return DocumentFileResponse(**d)


@router.get("/documents", response_model=list[AppDocumentIndex])
async def list_all_documents():
    indexes = list_all_app_document_indexes()
    return [
        AppDocumentIndex(
            app_id=idx["app_id"],
            document_count=idx["document_count"],
            files=[_file_dict_to_response(f) for f in idx["files"]],
        )
        for idx in indexes
    ]


@router.get("/documents/{app_id}", response_model=list[DocumentFileResponse])
async def list_app_documents(app_id: str):
    files = list_documents_for_app(app_id)
    return [_file_dict_to_response(f) for f in files]


@router.post("/documents/{app_id}/upload", response_model=DocumentFileResponse, status_code=201)
async def upload_document(app_id: str, file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    content = await file.read()
    result = await save_upload(app_id, file.filename, content)
    return _file_dict_to_response(result)
