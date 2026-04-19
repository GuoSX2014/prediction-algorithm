"""Health check with cached dependency probes."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from ..models.schemas import HealthComponent, HealthResponse
from ..services.feishu_client import FeishuClient
from ..services.minio_client import MinioDownloader
from ..services.predictor_client import PredictorClient
from .dependencies import get_feishu, get_minio, get_predictor


router = APIRouter(tags=["health"])

_PROBE_CACHE_TTL = 30.0  # seconds
_probe_cache: dict[str, tuple[float, bool, str]] = {}


def _cached_probe(name: str, fn) -> HealthComponent:
    now = time.monotonic()
    cached = _probe_cache.get(name)
    if cached and (now - cached[0]) < _PROBE_CACHE_TTL:
        return HealthComponent(ok=cached[1], detail=cached[2] or None)
    try:
        ok = bool(fn())
        detail = "" if ok else "probe returned False"
    except Exception as exc:  # noqa: BLE001
        ok = False
        detail = f"{type(exc).__name__}: {exc}"
    _probe_cache[name] = (now, ok, detail)
    return HealthComponent(ok=ok, detail=detail or None)


@router.get("", response_model=HealthResponse)
def healthcheck(
    minio: MinioDownloader = Depends(get_minio),
    predictor: PredictorClient = Depends(get_predictor),
    feishu: FeishuClient = Depends(get_feishu),
) -> HealthResponse:
    from .. import __version__

    components = {
        "minio": _cached_probe("minio", minio.ping),
        "predictor": _cached_probe("predictor", predictor.health),
        "feishu": _cached_probe("feishu", feishu.ping),
    }
    overall = "ok" if all(c.ok for c in components.values()) else "degraded"
    return HealthResponse(
        status=overall,
        version=__version__,
        checked_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        components=components,
    )
