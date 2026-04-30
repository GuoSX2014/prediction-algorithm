"""HTTP client for SFP-2 prediction service."""

from __future__ import annotations

import time
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

    def reload(self) -> Dict[str, Any]:
        """Trigger async data reload, then poll until complete."""
        base = self._cfg.base_url.rstrip("/")

        # Step 1: trigger reload
        trigger_url = f"{base}/reload"
        logger.info("triggering data reload", extra={"url": trigger_url})
        try:
            resp = httpx.post(trigger_url, timeout=self._cfg.timeout_sec)
        except httpx.HTTPError as exc:
            raise PredictorError(f"reload trigger network error: {exc}") from exc
        if resp.status_code >= 400:
            raise PredictorError(
                f"reload trigger failed: {resp.status_code} {resp.text[:500]}"
            )
        trigger_body = resp.json()
        logger.info(
            "reload triggered",
            extra={"status_code": resp.status_code, "body": trigger_body},
        )

        # Step 2: poll /reload/status until code=2 (success) or code=3 (failed)
        status_url = f"{base}/reload/status"
        poll_interval = self._cfg.reload_poll_interval_sec
        deadline = time.monotonic() + self._cfg.reload_timeout_sec

        while True:
            time.sleep(poll_interval)
            if time.monotonic() > deadline:
                raise PredictorError(
                    f"reload timed out after {self._cfg.reload_timeout_sec}s"
                )
            try:
                poll_resp = httpx.get(status_url, timeout=self._cfg.timeout_sec)
            except httpx.HTTPError as exc:
                logger.warning("reload status poll error", extra={"error": str(exc)})
                continue

            poll_body = poll_resp.json()
            code = poll_body.get("code")
            logger.info(
                "reload status poll",
                extra={"status_code": poll_resp.status_code, "body": poll_body},
            )

            if code == 2:
                logger.info("reload completed successfully", extra={"body": poll_body})
                return poll_body
            if code == 3:
                error_msg = poll_body.get("error", poll_body.get("message", "unknown"))
                raise PredictorError(f"reload failed: {error_msg}")
            # code 0 (idle) or 1 (running) — keep polling

    def health(self) -> bool:
        url = f"{self._cfg.base_url.rstrip('/')}/health"
        try:
            resp = httpx.get(url, timeout=5)
            body = resp.json()
            healthy = resp.status_code == 200 and body.get("status") == "ready"
            logger.info(
                "health check response",
                extra={"status_code": resp.status_code, "body": body, "healthy": healthy},
            )
            return healthy
        except Exception:
            logger.warning("health check failed", exc_info=True)
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
            raise PredictorError(
                f"predict {resp.status_code}: {resp.text[:500]}"
            )

        try:
            result = resp.json()
        except ValueError as exc:
            raise PredictorError(f"predict returned invalid JSON: {exc}") from exc

        logger.info(
            "predict response",
            extra={"status_code": resp.status_code, "body": result},
        )
        return result
