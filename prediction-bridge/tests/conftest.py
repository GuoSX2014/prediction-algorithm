"""Pytest fixtures.

We short-circuit ``app.main``'s default app creation via ``PREDICTION_BRIDGE_SKIP_BOOT`` to
avoid requiring a real YAML at import time; tests build their own Settings in-memory.
"""

from __future__ import annotations

import os
import sys

os.environ.setdefault("PREDICTION_BRIDGE_SKIP_BOOT", "1")

# Ensure project root (parent of ``tests``) is on sys.path when pytest is invoked
# from anywhere.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pathlib import Path

import pytest

from app.core.config import (
    AppSection,
    CallbackSection,
    ConcurrencySection,
    FeishuSection,
    FeishuTarget,
    LoggingSection,
    MinioSection,
    PredictorSection,
    ReportSection,
    Settings,
    StorageSection,
    TaskStoreSection,
)


@pytest.fixture
def tmp_dirs(tmp_path: Path) -> dict[str, Path]:
    dirs = {
        "downloads": tmp_path / "downloads",
        "traindata": tmp_path / "traindata",
        "reports": tmp_path / "reports",
        "logs": tmp_path / "logs",
    }
    for p in dirs.values():
        p.mkdir(parents=True, exist_ok=True)
    return dirs


@pytest.fixture
def test_settings(tmp_dirs: dict[str, Path]) -> Settings:
    return Settings(
        app=AppSection(api_prefix="/api/v1", callback_path="/notifications/processor"),
        callback=CallbackSection(),
        minio=MinioSection(
            endpoint="localhost:9000",
            bucket="test",
            download_dir=str(tmp_dirs["downloads"]),
            md5_retry=1,
            md5_retry_interval_sec=0,
        ),
        storage=StorageSection(
            traindata_root=str(tmp_dirs["traindata"]),
            on_conflict="overwrite",
            keep_failed_artifacts=False,
        ),
        predictor=PredictorSection(
            base_url="http://predictor.test",
            timeout_sec=5,
            retry=1,
            retry_interval_sec=0,
        ),
        report=ReportSection(
            output_dir=str(tmp_dirs["reports"]),
            template_path="app/templates/prediction.md.j2",
        ),
        feishu=FeishuSection(
            enabled=False,
            app_id="cli_test",
            app_secret="secret",
            targets=[FeishuTarget(chat_id="oc_test")],
        ),
        task_store=TaskStoreSection(backend="in_memory"),
        concurrency=ConcurrencySection(mode="serial"),
        logging=LoggingSection(
            level="INFO", dir=str(tmp_dirs["logs"]), console=False, json=False
        ),
    )
