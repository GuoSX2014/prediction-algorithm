"""End-to-end pipeline orchestrator.

Stages (each independently retryable / testable):
    pending -> downloading -> extracting -> predicting -> notifying -> done | failed
"""

from __future__ import annotations

import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from ..core.config import Settings
from ..core.errors import BridgeError
from ..core.logging import bind_stage, bind_trace, logger, stage_context
from ..models.schemas import ProcessorNotification, TaskRecord, TaskStageEnum
from ..models.task_store import TaskStore, make_dedup_key
from .archive import extract_tar_gz, make_workdir, place_into_traindata
from .feishu_client import FeishuClient
from .minio_client import MinioDownloader
from .predictor_client import PredictorClient
from .report_renderer import ReportRenderer


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def compute_predict_date(data_date_str: str) -> str:
    data_date = datetime.strptime(data_date_str, "%Y-%m-%d").date()
    return (data_date + timedelta(days=1)).strftime("%Y-%m-%d")


class Pipeline:
    """Bundles the collaborators needed by :meth:`run`."""

    def __init__(
        self,
        *,
        settings: Settings,
        task_store: TaskStore,
        downloader: MinioDownloader,
        predictor: PredictorClient,
        renderer: ReportRenderer,
        feishu: FeishuClient,
        executor: Optional[ThreadPoolExecutor] = None,
    ) -> None:
        self._settings = settings
        self._store = task_store
        self._downloader = downloader
        self._predictor = predictor
        self._renderer = renderer
        self._feishu = feishu
        self._executor = executor

    # ------------------------------------------------------------------ #

    def enqueue(self, notification: ProcessorNotification) -> TaskRecord:
        """Create a task record. Returns an existing record on dedup hit."""
        dedup_key = make_dedup_key(notification.object_name, notification.md5)
        existing = self._store.find_by_dedup(
            dedup_key, self._settings.task_store.dedup_ttl_sec
        )
        if existing is not None:
            logger.info(
                "dedup hit; reusing task",
                extra={"trace_id": existing.trace_id, "dedup_key": dedup_key},
            )
            return existing

        from uuid import uuid4

        data_date = notification.date_range.start.strftime("%Y-%m-%d")
        predict_date = compute_predict_date(data_date)
        trace_id = str(uuid4())
        now = _now_iso()
        record = TaskRecord(
            trace_id=trace_id,
            status=TaskStageEnum.PENDING,
            dedup_key=dedup_key,
            object_name=notification.object_name,
            md5=notification.md5,
            data_date=data_date,
            predict_date=predict_date,
            created_at=now,
            updated_at=now,
        )
        self._store.create(record)
        logger.info(
            "task created",
            extra={
                "trace_id": trace_id,
                "data_date": data_date,
                "predict_date": predict_date,
                "object_name": notification.object_name,
            },
        )
        return record

    def submit(self, trace_id: str, notification: ProcessorNotification) -> None:
        """Dispatch :meth:`run` based on concurrency mode."""
        if self._executor is not None:
            self._executor.submit(self._safe_run, trace_id, notification)
        else:
            self._safe_run(trace_id, notification)

    # ------------------------------------------------------------------ #

    def _safe_run(self, trace_id: str, notification: ProcessorNotification) -> None:
        bind_trace(trace_id)
        try:
            self.run(trace_id, notification)
        except Exception as exc:  # noqa: BLE001
            logger.opt(exception=True).error(
                "pipeline crashed",
                extra={"trace_id": trace_id},
            )
            self._store.update(
                trace_id, status=TaskStageEnum.FAILED, error=str(exc)
            )
            self._alert(trace_id, exc)
        finally:
            bind_stage("")

    def run(self, trace_id: str, notification: ProcessorNotification) -> None:
        rec = self._store.get(trace_id)
        if rec is None:
            raise BridgeError(f"task not found: {trace_id}")

        data_date = rec.data_date
        predict_date = rec.predict_date
        workdir: Optional[Path] = None
        archive_path: Optional[Path] = None

        try:
            # --- download ---
            self._store.update(trace_id, status=TaskStageEnum.DOWNLOADING)
            with stage_context("download"):
                archive_path = self._downloader.download(
                    object_name=notification.object_name,
                    download_url=notification.download_url,
                    expected_md5=notification.md5,
                )
                self._store.set_stage(
                    trace_id,
                    "download",
                    {"path": str(archive_path), "size": archive_path.stat().st_size},
                )

            # --- extract + place ---
            self._store.update(trace_id, status=TaskStageEnum.EXTRACTING)
            with stage_context("extract"):
                workdir = make_workdir()
                output_dir = extract_tar_gz(archive_path, workdir)
                target = place_into_traindata(
                    source_dir=output_dir,
                    traindata_root=Path(self._settings.storage.traindata_root),
                    data_date=data_date,
                    on_conflict=self._settings.storage.on_conflict,
                )
                self._store.set_stage(
                    trace_id,
                    "extract",
                    {"traindata_dir": str(target)},
                )

            # --- (optional) rebuild dataset ---
            if self._settings.predictor.rebuild_dataset_before_predict:
                with stage_context("rebuild_dataset"):
                    self._predictor.rebuild_dataset()

            # --- predict ---
            self._store.update(trace_id, status=TaskStageEnum.PREDICTING)
            with stage_context("predict"):
                prediction = self._predictor.predict(predict_date)
                self._store.set_stage(
                    trace_id,
                    "predict",
                    {
                        "predict_date": predict_date,
                        "model_version": prediction.get("model_version"),
                    },
                )

            # --- render ---
            with stage_context("render"):
                report_path = self._renderer.render(
                    predict_date=predict_date,
                    data_date=data_date,
                    trace_id=trace_id,
                    prediction=prediction,
                )
                self._store.update(trace_id, report_path=str(report_path))

            # --- feishu ---
            self._store.update(trace_id, status=TaskStageEnum.NOTIFYING)
            with stage_context("feishu"):
                feishu_results = self._feishu.send_report(
                    report_path=report_path,
                    predict_date=predict_date,
                    data_date=data_date,
                    trace_id=trace_id,
                )
                self._store.update(trace_id, feishu_results=feishu_results)

            self._store.update(trace_id, status=TaskStageEnum.DONE, error=None)
            logger.info(
                "pipeline done",
                extra={"trace_id": trace_id, "predict_date": predict_date},
            )
        except Exception as exc:
            self._store.update(
                trace_id, status=TaskStageEnum.FAILED, error=str(exc)
            )
            self._alert(trace_id, exc)
            raise
        finally:
            self._cleanup(
                archive_path=archive_path,
                workdir=workdir,
                failed=self._is_failed(trace_id),
            )

    # ------------------------------------------------------------------ #

    def _is_failed(self, trace_id: str) -> bool:
        rec = self._store.get(trace_id)
        return bool(rec and rec.status == TaskStageEnum.FAILED)

    def _cleanup(
        self,
        *,
        archive_path: Optional[Path],
        workdir: Optional[Path],
        failed: bool,
    ) -> None:
        keep = self._settings.storage.keep_failed_artifacts and failed
        if keep:
            logger.info(
                "keeping failed artifacts for debugging",
                extra={"archive": str(archive_path) if archive_path else None,
                       "workdir": str(workdir) if workdir else None},
            )
            return
        if archive_path and archive_path.exists():
            try:
                archive_path.unlink()
            except OSError:
                logger.opt(exception=True).warning("archive cleanup failed")
        if workdir and workdir.exists():
            shutil.rmtree(workdir, ignore_errors=True)

    def _alert(self, trace_id: str, exc: BaseException) -> None:
        try:
            self._feishu.send_alert(
                f"[prediction-bridge] 任务 {trace_id} 失败：{type(exc).__name__}: {exc}"
            )
        except Exception:
            logger.opt(exception=True).warning("failed to send failure alert")
