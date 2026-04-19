"""Task status query endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from ..models.schemas import TaskStatusResponse
from ..models.task_store import TaskStore
from .dependencies import get_task_store


router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("/{trace_id}", response_model=TaskStatusResponse)
def get_task(trace_id: str, store: TaskStore = Depends(get_task_store)) -> TaskStatusResponse:
    record = store.get(trace_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="task not found")
    return TaskStatusResponse(
        trace_id=record.trace_id,
        status=record.status,
        data_date=record.data_date,
        predict_date=record.predict_date,
        created_at=record.created_at,
        updated_at=record.updated_at,
        error=record.error,
        stages=record.stages,
        report_path=record.report_path,
        feishu_results=record.feishu_results,
    )


@router.get("", summary="List recent tasks")
def list_tasks(limit: int = 50, store: TaskStore = Depends(get_task_store)):
    records = store.list_recent(limit=max(1, min(limit, 500)))
    return {"count": len(records), "items": [r.model_dump() for r in records]}
