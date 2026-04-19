"""FastAPI dependency providers.

App components are created once at startup and stashed on ``app.state``;
these helpers let route handlers pull them out in a typed way.
"""

from __future__ import annotations

from fastapi import Depends, Request

from ..core.config import Settings
from ..models.task_store import TaskStore
from ..services.feishu_client import FeishuClient
from ..services.minio_client import MinioDownloader
from ..services.pipeline import Pipeline
from ..services.predictor_client import PredictorClient
from ..services.report_renderer import ReportRenderer


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_task_store(request: Request) -> TaskStore:
    return request.app.state.task_store


def get_pipeline(request: Request) -> Pipeline:
    return request.app.state.pipeline


def get_minio(request: Request) -> MinioDownloader:
    return request.app.state.minio


def get_predictor(request: Request) -> PredictorClient:
    return request.app.state.predictor


def get_feishu(request: Request) -> FeishuClient:
    return request.app.state.feishu


def get_renderer(request: Request) -> ReportRenderer:
    return request.app.state.renderer
