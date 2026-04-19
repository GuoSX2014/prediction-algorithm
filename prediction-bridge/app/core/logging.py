"""Structured logging via loguru.

All records include ``trace_id`` and ``stage`` keys (empty when not in a pipeline context).
Secrets passed to ``extra`` may be masked by wrapping them with :func:`mask_secret`.
"""

from __future__ import annotations

import sys
from contextvars import ContextVar
from pathlib import Path
from typing import Any

from loguru import logger

from .config import LoggingSection


_SENSITIVE_SUBSTRINGS = ("secret", "token", "access_key", "app_secret", "password")

_trace_id_var: ContextVar[str] = ContextVar("trace_id", default="")
_stage_var: ContextVar[str] = ContextVar("stage", default="")

_LOGGING_INITIALIZED = False


def mask_secret(value: str | None, *, keep: int = 4) -> str:
    if not value:
        return ""
    if len(value) <= keep * 2:
        return "*" * len(value)
    return f"{value[:keep]}{'*' * (len(value) - keep * 2)}{value[-keep:]}"


def _scrub(record_extra: dict[str, Any]) -> dict[str, Any]:
    scrubbed: dict[str, Any] = {}
    for k, v in record_extra.items():
        lk = k.lower()
        if any(s in lk for s in _SENSITIVE_SUBSTRINGS) and isinstance(v, str):
            scrubbed[k] = mask_secret(v)
        else:
            scrubbed[k] = v
    return scrubbed


def _context_patcher(record: dict[str, Any]) -> None:
    record["extra"].setdefault("trace_id", _trace_id_var.get())
    record["extra"].setdefault("stage", _stage_var.get())
    record["extra"] = _scrub(record["extra"])


def init_logging(cfg: LoggingSection) -> None:
    global _LOGGING_INITIALIZED
    if _LOGGING_INITIALIZED:
        return

    logger.remove()
    logger.configure(patcher=_context_patcher)

    if cfg.console:
        logger.add(
            sys.stderr,
            level=cfg.level,
            serialize=cfg.json,
            backtrace=True,
            diagnose=False,
            enqueue=False,
        )

    if cfg.dir:
        log_dir = Path(cfg.dir).expanduser()
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            logger.warning(
                "log directory not writable; falling back to stderr-only",
                extra={"dir": str(log_dir)},
            )
        else:
            logger.add(
                str(log_dir / "prediction-bridge.log"),
                level=cfg.level,
                rotation=cfg.rotation,
                retention=cfg.retention,
                serialize=cfg.json,
                enqueue=True,
            )

    _LOGGING_INITIALIZED = True


def bind_trace(trace_id: str) -> None:
    _trace_id_var.set(trace_id)


def bind_stage(stage: str) -> None:
    _stage_var.set(stage)


class stage_context:  # noqa: N801 — used as context manager
    """Context manager that sets (and restores) the logging stage name."""

    def __init__(self, stage: str) -> None:
        self._stage = stage
        self._token = None

    def __enter__(self):
        self._token = _stage_var.set(self._stage)
        logger.info("stage start")
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is None:
            logger.info("stage end")
        else:
            logger.opt(exception=exc).error("stage failed")
        if self._token is not None:
            _stage_var.reset(self._token)
        return False


__all__ = [
    "init_logging",
    "logger",
    "bind_trace",
    "bind_stage",
    "stage_context",
    "mask_secret",
]
