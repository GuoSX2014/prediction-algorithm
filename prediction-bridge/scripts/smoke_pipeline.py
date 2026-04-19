"""Offline smoke test: run the full pipeline with all externals mocked.

Usage:
    python scripts/smoke_pipeline.py [--archive path/to/file.tar.gz]

Skips the real MinIO / predictor / Feishu services; writes artifacts under a
temp directory and prints the generated report path on success.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import os
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path

os.environ.setdefault("PREDICTION_BRIDGE_SKIP_BOOT", "1")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

import httpx
import respx

from app.core.config import (  # noqa: E402
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
from app.models.schemas import ProcessorNotification, DateRange  # noqa: E402
from app.services.feishu_client import FeishuClient  # noqa: E402
from app.services.minio_client import MinioDownloader  # noqa: E402
from app.services.pipeline import Pipeline  # noqa: E402
from app.services.predictor_client import PredictorClient  # noqa: E402
from app.services.report_renderer import ReportRenderer  # noqa: E402
from app.models.task_store import build_task_store  # noqa: E402


def _make_fixture_tar(dest: Path) -> str:
    with tarfile.open(dest, "w:gz") as tar:
        info = tarfile.TarInfo(name="output/sample.csv")
        data = b"time,value\n00:00,1.23\n"
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    return hashlib.md5(dest.read_bytes()).hexdigest()


def _build_settings(tmp: Path) -> Settings:
    (tmp / "downloads").mkdir()
    (tmp / "traindata").mkdir()
    (tmp / "reports").mkdir()
    return Settings(
        app=AppSection(),
        callback=CallbackSection(),
        minio=MinioSection(
            endpoint="localhost:9000",
            bucket="test",
            download_dir=str(tmp / "downloads"),
            fallback_to_http=True,
            md5_retry=1,
            md5_retry_interval_sec=0,
        ),
        storage=StorageSection(
            traindata_root=str(tmp / "traindata"),
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
            output_dir=str(tmp / "reports"),
            template_path=str(_ROOT / "app" / "templates" / "prediction.md.j2"),
        ),
        feishu=FeishuSection(
            enabled=False,
            app_id="cli",
            app_secret="s",
            targets=[FeishuTarget(chat_id="oc_test")],
        ),
        task_store=TaskStoreSection(backend="in_memory"),
        concurrency=ConcurrencySection(mode="serial"),
        logging=LoggingSection(
            dir=str(tmp / "logs"), console=True, json=False
        ),
    )


@respx.mock
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, default=None)
    args = parser.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="prediction-bridge-smoke-"))
    print(f"[smoke] workdir: {tmp}")

    if args.archive:
        archive = tmp / args.archive.name
        shutil.copy2(args.archive, archive)
        md5 = hashlib.md5(archive.read_bytes()).hexdigest()
    else:
        archive = tmp / "2026-03-26.tar.gz"
        md5 = _make_fixture_tar(archive)

    download_url = f"http://minio.test/sxpx/output/{archive.name}"
    respx.get(download_url).mock(
        return_value=httpx.Response(200, content=archive.read_bytes())
    )
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

    settings = _build_settings(tmp)

    downloader = MinioDownloader(settings.minio)
    predictor = PredictorClient(settings.predictor)
    renderer = ReportRenderer(settings.report)
    feishu = FeishuClient(settings.feishu)
    store = build_task_store(settings.task_store.backend, settings.task_store.sqlite_path)
    pipeline = Pipeline(
        settings=settings,
        task_store=store,
        downloader=downloader,
        predictor=predictor,
        renderer=renderer,
        feishu=feishu,
    )

    notification = ProcessorNotification(
        categories=["实时市场出清概况"],
        date_range=DateRange(start="2026-03-26", end="2026-03-26"),
        object_name=archive.name,
        md5=md5,
        download_url=download_url,
    )
    record = pipeline.enqueue(notification)
    pipeline.run(record.trace_id, notification)

    final = store.get(record.trace_id)
    assert final is not None
    print(f"[smoke] status: {final.status}")
    print(f"[smoke] report: {final.report_path}")
    print(f"[smoke] traindata: {Path(settings.storage.traindata_root) / final.data_date}")

    if final.status != "done":
        print(f"[smoke] ERROR: {final.error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
