"""FastAPI entry point."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .api.health import router as health_router
from .api.notifications import router as notifications_router
from .api.tasks import router as tasks_router
from .core.config import Settings, load_settings
from .core.errors import BridgeError
from .core.logging import init_logging, logger
from .models.task_store import build_task_store
from .services.feishu_client import FeishuClient
from .services.minio_client import MinioDownloader
from .services.pipeline import Pipeline
from .services.predictor_client import PredictorClient
from .services.report_renderer import ReportRenderer


def _build_components(settings: Settings):
    task_store = build_task_store(
        settings.task_store.backend, settings.task_store.sqlite_path
    )
    downloader = MinioDownloader(settings.minio)
    predictor = PredictorClient(settings.predictor)
    renderer = ReportRenderer(settings.report)
    feishu = FeishuClient(settings.feishu)

    executor = None
    if settings.concurrency.mode == "thread_pool":
        executor = ThreadPoolExecutor(
            max_workers=max(1, settings.concurrency.thread_pool_size),
            thread_name_prefix="prediction-bridge-pipeline",
        )

    pipeline = Pipeline(
        settings=settings,
        task_store=task_store,
        downloader=downloader,
        predictor=predictor,
        renderer=renderer,
        feishu=feishu,
        executor=executor,
    )
    return task_store, downloader, predictor, renderer, feishu, pipeline, executor


def _install_components(app: FastAPI, settings: Settings) -> None:
    (
        task_store,
        downloader,
        predictor,
        renderer,
        feishu,
        pipeline,
        executor,
    ) = _build_components(settings)
    app.state.settings = settings
    app.state.task_store = task_store
    app.state.minio = downloader
    app.state.predictor = predictor
    app.state.renderer = renderer
    app.state.feishu = feishu
    app.state.pipeline = pipeline
    app.state.executor = executor


@asynccontextmanager
async def _default_lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Settings were already installed in create_app() — just bookend logging.
    logger.info(
        "prediction-bridge starting",
        extra={"config_path": app.state.settings.config_path or "(none)"},
    )
    try:
        yield
    finally:
        logger.info("prediction-bridge shutting down")
        executor = getattr(app.state, "executor", None)
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=False)


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    """Factory.

    If ``settings`` is not supplied it is loaded from YAML + env at call time;
    startup failures (e.g. missing required config) surface synchronously.
    """
    if settings is None:
        settings = load_settings()
    init_logging(settings.logging)

    app = FastAPI(
        title="prediction-bridge",
        description="Bridge between processor -> MinIO -> SFP-2 predictor -> Feishu.",
        version="0.1.0",
        lifespan=_default_lifespan,
    )
    _install_components(app, settings)

    prefix = settings.app.api_prefix.rstrip("/")
    callback_path = settings.app.callback_path

    app.include_router(health_router, prefix="/health")
    app.include_router(notifications_router, prefix=f"{prefix}{callback_path}")
    app.include_router(tasks_router, prefix=prefix)

    @app.exception_handler(BridgeError)
    async def _bridge_err_handler(_: Request, exc: BridgeError):
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": str(exc)},
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(_: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": "validation error", "errors": exc.errors()},
        )

    return app


def _build_default_app() -> FastAPI:
    """Used by ``uvicorn app.main:app``. Fails cleanly when config is missing."""
    try:
        return create_app()
    except Exception as exc:
        # Print a readable error so uvicorn doesn't bury it.
        print(f"[prediction-bridge] failed to start: {exc}", flush=True)
        raise


app = _build_default_app() if os.environ.get("PREDICTION_BRIDGE_SKIP_BOOT") != "1" else None  # type: ignore[assignment]
