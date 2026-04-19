"""Processor callback endpoint."""

from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, status
from fastapi.responses import JSONResponse

from ..core.logging import bind_trace, logger
from ..models.schemas import NotificationAccepted, ProcessorNotification
from ..services.pipeline import Pipeline
from .dependencies import get_pipeline


router = APIRouter(tags=["notifications"])


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=NotificationAccepted,
    summary="Receive processor callback",
)
async def receive_notification(
    payload: ProcessorNotification,
    background: BackgroundTasks,
    pipeline: Pipeline = Depends(get_pipeline),
) -> JSONResponse:
    record = pipeline.enqueue(payload)
    bind_trace(record.trace_id)
    logger.info(
        "notification accepted",
        extra={"trace_id": record.trace_id, "dedup_key": record.dedup_key},
    )
    background.add_task(pipeline.submit, record.trace_id, payload)
    body = NotificationAccepted.now(record.trace_id).model_dump()
    body["trace_id"] = record.trace_id  # may be dedup match (existing trace)
    return JSONResponse(body, status_code=status.HTTP_202_ACCEPTED)
