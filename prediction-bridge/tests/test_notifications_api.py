"""Integration test: POST notification, mock externals, assert pipeline runs."""

from __future__ import annotations

import hashlib
import io
import tarfile
import time
from pathlib import Path

import httpx
import respx
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.main import create_app
from app.services.feishu_client import BASE_URL as FEISHU_BASE


def _make_tar_gz(path: Path, payload: dict[str, bytes]) -> str:
    with tarfile.open(path, "w:gz") as tar:
        for name, data in payload.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return hashlib.md5(path.read_bytes()).hexdigest()


@respx.mock
def test_end_to_end_with_mocks(test_settings: Settings, tmp_path: Path) -> None:
    # Build a fake archive in the "MinIO bucket" (served over HTTP fallback).
    archive = tmp_path / "2026-03-26.tar.gz"
    md5 = _make_tar_gz(archive, {"output/dummy.csv": b"a,b\n1,2\n"})
    download_url = "http://minio.test/sxpx/output/2026-03-26.tar.gz"

    respx.get(download_url).mock(
        return_value=httpx.Response(200, content=archive.read_bytes())
    )

    # Disable the MinIO SDK path by forcing HTTP fallback only.
    test_settings.minio.access_key = ""
    test_settings.minio.secret_key = ""
    test_settings.minio.fallback_to_http = True

    # Mock predictor
    respx.post("http://predictor.test/predict").mock(
        return_value=httpx.Response(
            200,
            json={
                "date": "2026-03-27",
                "segments": {
                    f"T{i}": {
                        "调频容量需求": 1000.0 + i,
                        "边际排序价格": 5.0 + i,
                        "市场出清价格_预测均价": 6.0 + i,
                    }
                    for i in range(1, 6)
                },
                "model_version": "hidden_dim=128",
                "generated_at": "2026-03-27T00:00:00Z",
            },
        )
    )

    app = create_app(settings=test_settings)
    with TestClient(app) as client:
        resp = client.post(
            "/api/v1/notifications/processor",
            json={
                "categories": ["实时市场出清概况"],
                "date_range": {"start": "2026-03-26", "end": "2026-03-26"},
                "object_name": "2026-03-26.tar.gz",
                "md5": md5,
                "download_url": download_url,
            },
        )
        assert resp.status_code == 202, resp.text
        trace_id = resp.json()["trace_id"]

        # Serial pipeline runs in the background task; TestClient flushes on context exit
        # but we poll briefly for safety.
        deadline = time.time() + 10
        status = None
        while time.time() < deadline:
            r = client.get(f"/api/v1/tasks/{trace_id}")
            assert r.status_code == 200
            status = r.json()["status"]
            if status in ("done", "failed"):
                break
            time.sleep(0.1)

        assert status == "done", r.json()
        # Report file was written.
        report_path = Path(r.json()["report_path"])
        assert report_path.is_file()
        body = report_path.read_text(encoding="utf-8")
        assert "# 2026-03-27 预测结果" in body
        # Traindata dir landed under data_date (not predict_date).
        assert (Path(test_settings.storage.traindata_root) / "2026-03-26").is_dir()


def test_invalid_md5_returns_400(test_settings: Settings) -> None:
    app = create_app(settings=test_settings)
    with TestClient(app) as client:
        resp = client.post(
            "/api/v1/notifications/processor",
            json={
                "categories": [],
                "date_range": {"start": "2026-03-26", "end": "2026-03-26"},
                "object_name": "x.tar.gz",
                "md5": "not-md5",
                "download_url": "http://x/y",
            },
        )
        assert resp.status_code == 400


def test_dedup_returns_same_trace(test_settings: Settings) -> None:
    """Second identical call within TTL reuses the existing trace_id."""
    app = create_app(settings=test_settings)
    with TestClient(app) as client:
        # Use a payload whose download will fail so the task ends up "failed" quickly
        # (we only assert that dedup returns the same trace_id, not success).
        body = {
            "categories": [],
            "date_range": {"start": "2026-03-26", "end": "2026-03-26"},
            "object_name": "dedup.tar.gz",
            "md5": "0" * 32,
            "download_url": "http://does-not-exist.local/x.tar.gz",
        }
        r1 = client.post("/api/v1/notifications/processor", json=body)
        r2 = client.post("/api/v1/notifications/processor", json=body)
        assert r1.status_code == 202 and r2.status_code == 202
        assert r1.json()["trace_id"] == r2.json()["trace_id"]
