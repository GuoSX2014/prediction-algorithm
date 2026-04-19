"""HTTP client for SFP-2 prediction service."""

from __future__ import annotations

from typing import Any, Dict

import httpx
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ..core.config import PredictorSection
from ..core.errors import PredictorError, PredictorStartingError
from ..core.logging import logger


class PredictorClient:
    def __init__(self, cfg: PredictorSection) -> None:
        self._cfg = cfg

    # ------------------------------------------------------------------ #

    def predict(self, predict_date: str) -> Dict[str, Any]:
        retrying = retry(
            reraise=True,
            stop=stop_after_attempt(max(1, self._cfg.retry)),
            wait=wait_exponential(
                multiplier=self._cfg.retry_interval_sec,
                min=self._cfg.retry_interval_sec,
                max=self._cfg.retry_interval_sec * 8,
            ),
            retry=retry_if_exception_type(
                (PredictorStartingError, httpx.HTTPError)
            ),
        )

        @retrying
        def _call() -> Dict[str, Any]:
            return self._do_predict(predict_date)

        try:
            return _call()
        except RetryError as exc:  # pragma: no cover — reraise=True short-circuits this
            raise PredictorError(f"predict failed: {exc}") from exc

    def rebuild_dataset(self) -> Dict[str, Any]:
        url = f"{self._cfg.base_url.rstrip('/')}/datasets/rebuild"
        logger.info("triggering dataset rebuild", extra={"url": url})
        try:
            resp = httpx.post(url, timeout=self._cfg.timeout_sec * 5)
        except httpx.HTTPError as exc:
            raise PredictorError(f"dataset rebuild network error: {exc}") from exc
        if resp.status_code >= 400:
            raise PredictorError(
                f"dataset rebuild failed: {resp.status_code} {resp.text[:500]}"
            )
        return resp.json()

    def health(self) -> bool:
        url = f"{self._cfg.base_url.rstrip('/')}/health"
        try:
            resp = httpx.get(url, timeout=5)
            return resp.status_code == 200 and resp.json().get("status") == "ok"
        except Exception:
            return False

    # ------------------------------------------------------------------ #

    def _do_predict(self, predict_date: str) -> Dict[str, Any]:
        url = f"{self._cfg.base_url.rstrip('/')}/predict"
        logger.info(
            "calling predict", extra={"url": url, "predict_date": predict_date}
        )
        try:
            resp = httpx.post(
                url,
                params={"date": predict_date},
                timeout=self._cfg.timeout_sec,
            )
        except httpx.HTTPError as exc:
            logger.warning("predict network error", extra={"error": str(exc)})
            raise

        if resp.status_code == 503:
            logger.warning(
                "predictor still starting",
                extra={"status": 503, "body": resp.text[:500]},
            )
            raise PredictorStartingError(resp.text[:500])

        if resp.status_code >= 400:
            # 4xx/5xx that is not 503 — terminal.
            raise PredictorError(
                f"predict {resp.status_code}: {resp.text[:500]}"
            )

        try:
            return resp.json()
        except ValueError as exc:
            raise PredictorError(f"predict returned invalid JSON: {exc}") from exc
