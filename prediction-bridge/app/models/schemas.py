"""Request/response Pydantic models."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


_MD5_RE = re.compile(r"^[0-9a-fA-F]{32}$")


class DateRange(BaseModel):
    start: date
    end: date

    @model_validator(mode="after")
    def _ordered(self) -> "DateRange":
        if self.end < self.start:
            raise ValueError("date_range.end must be >= date_range.start")
        return self


class ProcessorNotification(BaseModel):
    """Body sent by the processor service."""

    model_config = ConfigDict(populate_by_name=True)

    categories: List[str] = Field(default_factory=list)
    date_range: DateRange
    object_name: str = Field(min_length=1)
    md5: str
    download_url: str = Field(min_length=1)

    @field_validator("md5")
    @classmethod
    def _valid_md5(cls, v: str) -> str:
        if not _MD5_RE.match(v):
            raise ValueError("md5 must be a 32-character hex string")
        return v.lower()


class NotificationAccepted(BaseModel):
    status: str = "accepted"
    trace_id: str
    received_at: str

    @classmethod
    def now(cls, trace_id: str) -> "NotificationAccepted":
        return cls(
            trace_id=trace_id,
            received_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )


class TaskStageEnum(str):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    EXTRACTING = "extracting"
    PREDICTING = "predicting"
    NOTIFYING = "notifying"
    DONE = "done"
    FAILED = "failed"


class TaskRecord(BaseModel):
    """Persisted per-task record."""

    trace_id: str
    status: str = TaskStageEnum.PENDING
    dedup_key: str
    object_name: str
    md5: str
    data_date: str           # YYYY-MM-DD — source archive data date
    predict_date: str        # YYYY-MM-DD — data_date + 1 day
    created_at: str
    updated_at: str
    error: Optional[str] = None
    stages: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    report_path: Optional[str] = None
    feishu_results: List[Dict[str, Any]] = Field(default_factory=list)


class TaskStatusResponse(BaseModel):
    trace_id: str
    status: str
    data_date: str
    predict_date: str
    created_at: str
    updated_at: str
    error: Optional[str] = None
    stages: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    report_path: Optional[str] = None
    feishu_results: List[Dict[str, Any]] = Field(default_factory=list)


class HealthComponent(BaseModel):
    ok: bool
    detail: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    checked_at: str
    components: Dict[str, HealthComponent]


class ErrorResponse(BaseModel):
    detail: str
    errors: Optional[List[Dict[str, Any]]] = None
