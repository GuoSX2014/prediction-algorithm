"""PredictorClient tests using respx to mock httpx."""

from __future__ import annotations

import httpx
import pytest
import respx

from app.core.config import PredictorSection
from app.core.errors import PredictorError, PredictorStartingError
from app.services.predictor_client import PredictorClient


BASE = "http://predictor.test"


def _cfg(retry: int = 1) -> PredictorSection:
    return PredictorSection(
        base_url=BASE,
        timeout_sec=5,
        retry=retry,
        retry_interval_sec=0,
    )


@respx.mock
def test_predict_happy_path() -> None:
    route = respx.post(f"{BASE}/predict", params={"date": "2026-03-27"}).mock(
        return_value=httpx.Response(200, json={"date": "2026-03-27", "segments": {}}),
    )
    result = PredictorClient(_cfg()).predict("2026-03-27")
    assert route.called
    assert result["date"] == "2026-03-27"


@respx.mock
def test_predict_400_raises_predictor_error() -> None:
    respx.post(f"{BASE}/predict").mock(
        return_value=httpx.Response(400, json={"detail": "bad"}),
    )
    with pytest.raises(PredictorError):
        PredictorClient(_cfg()).predict("bad-date")


@respx.mock
def test_predict_503_retries_then_raises() -> None:
    respx.post(f"{BASE}/predict").mock(
        return_value=httpx.Response(503, json={"detail": "启动中"}),
    )
    with pytest.raises(PredictorStartingError):
        PredictorClient(_cfg(retry=2)).predict("2026-03-27")


@respx.mock
def test_predict_timeout_bubbles_up() -> None:
    respx.post(f"{BASE}/predict").mock(
        side_effect=httpx.ReadTimeout("timeout"),
    )
    with pytest.raises(httpx.HTTPError):
        PredictorClient(_cfg(retry=1)).predict("2026-03-27")


@respx.mock
def test_health_probe_ok() -> None:
    respx.get(f"{BASE}/health").mock(
        return_value=httpx.Response(200, json={"status": "ready", "method": 9, "error": None})
    )
    assert PredictorClient(_cfg()).health() is True


@respx.mock
def test_reload_ok() -> None:
    respx.post(f"{BASE}/reload").mock(
        return_value=httpx.Response(200, json={"code": 1, "status": "running", "message": "reload running"})
    )
    respx.get(f"{BASE}/reload/status").mock(
        return_value=httpx.Response(200, json={"code": 2, "status": "success", "message": "reload completed", "elapsed_seconds": 5.0})
    )
    cfg = _cfg()
    cfg = cfg.model_copy(update={"reload_poll_interval_sec": 0})
    result = PredictorClient(cfg).reload()
    assert result["code"] == 2
    assert result["status"] == "success"


@respx.mock
def test_reload_failed_raises() -> None:
    respx.post(f"{BASE}/reload").mock(
        return_value=httpx.Response(200, json={"code": 1, "status": "running", "message": "reload running"})
    )
    respx.get(f"{BASE}/reload/status").mock(
        return_value=httpx.Response(200, json={"code": 3, "status": "failed", "message": "reload failed", "error": "bad data"})
    )
    cfg = _cfg()
    cfg = cfg.model_copy(update={"reload_poll_interval_sec": 0})
    with pytest.raises(PredictorError, match="reload failed"):
        PredictorClient(cfg).reload()
