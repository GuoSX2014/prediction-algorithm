"""Configuration loader.

Layered config: YAML (base) + environment variables (override).
Environment variables use ``__`` to address nested keys, e.g. ``MINIO__ACCESS_KEY``.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_CONFIG_PATH_ENV = "PREDICTION_BRIDGE_CONFIG_PATH"
DEFAULT_CONFIG_PATH = "config/config.yaml"


class AppSection(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080
    api_prefix: str = "/api/v1"
    callback_path: str = "/notifications/processor"


class CallbackSection(BaseModel):
    secret: str = ""


class MinioSection(BaseModel):
    endpoint: str = ""
    access_key: str = ""
    secret_key: str = ""
    secure: bool = False
    bucket: str = ""
    object_prefix: str = ""
    download_dir: str = "/var/lib/prediction-bridge/downloads"
    md5_retry: int = 3
    md5_retry_interval_sec: int = 5
    fallback_to_http: bool = True
    http_timeout_sec: int = 300


class StorageSection(BaseModel):
    traindata_root: str = "/data/deploy/electricity-prediction/sfp2-deploy/traindata"
    on_conflict: Literal["overwrite", "skip"] = "overwrite"
    keep_failed_artifacts: bool = True


class PredictorSection(BaseModel):
    base_url: str = "http://127.0.0.1:9527"
    rebuild_dataset_before_predict: bool = False
    timeout_sec: int = 60
    retry: int = 3
    retry_interval_sec: int = 10


class ReportSection(BaseModel):
    output_dir: str = "/var/lib/prediction-bridge/reports"
    template_path: str = "app/templates/prediction.md.j2"


class FeishuTarget(BaseModel):
    name: str = ""
    chat_id: str
    mention_all: bool = True
    mention_ids: List[str] = Field(default_factory=list)
    mention_names: List[str] = Field(default_factory=list)
    message: str = "请查阅 {predict_date} 预测结果。"


class FeishuSection(BaseModel):
    enabled: bool = True
    app_id: str = ""
    app_secret: str = ""
    http_timeout_sec: int = 30
    alert_on_failure: bool = True
    targets: List[FeishuTarget] = Field(default_factory=list)


class TaskStoreSection(BaseModel):
    backend: Literal["in_memory", "sqlite"] = "in_memory"
    sqlite_path: str = "/var/lib/prediction-bridge/tasks.sqlite"
    dedup_ttl_sec: int = 900


class ConcurrencySection(BaseModel):
    mode: Literal["serial", "thread_pool"] = "serial"
    thread_pool_size: int = 2


class LoggingSection(BaseModel):
    level: str = "INFO"
    dir: str = "/var/log/prediction-bridge"
    rotation: str = "100 MB"
    retention: str = "30 days"
    json: bool = True
    console: bool = True

    @field_validator("level")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()


class Settings(BaseSettings):
    """Fully resolved application settings."""

    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app: AppSection = Field(default_factory=AppSection)
    callback: CallbackSection = Field(default_factory=CallbackSection)
    minio: MinioSection = Field(default_factory=MinioSection)
    storage: StorageSection = Field(default_factory=StorageSection)
    predictor: PredictorSection = Field(default_factory=PredictorSection)
    report: ReportSection = Field(default_factory=ReportSection)
    feishu: FeishuSection = Field(default_factory=FeishuSection)
    task_store: TaskStoreSection = Field(default_factory=TaskStoreSection)
    concurrency: ConcurrencySection = Field(default_factory=ConcurrencySection)
    logging: LoggingSection = Field(default_factory=LoggingSection)

    # Path of the YAML that was loaded (informational).
    config_path: Optional[str] = None


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping, got: {type(data).__name__}")
    return data


def _resolve_config_path() -> Optional[Path]:
    raw = os.environ.get(DEFAULT_CONFIG_PATH_ENV) or DEFAULT_CONFIG_PATH
    if not raw:
        return None
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = Path.cwd() / p
    return p if p.is_file() else None


_SETTINGS_CACHE: Optional[Settings] = None


def load_settings(force_reload: bool = False) -> Settings:
    """Load settings. YAML is the base; env vars override via nested delimiter."""
    global _SETTINGS_CACHE
    if _SETTINGS_CACHE is not None and not force_reload:
        return _SETTINGS_CACHE

    yaml_path = _resolve_config_path()
    yaml_data: dict[str, Any] = _load_yaml(yaml_path) if yaml_path else {}

    # Build base Settings from YAML, then let env vars override.
    try:
        settings = Settings(**yaml_data)
    except Exception as exc:  # pragma: no cover - surfaced clearly at startup
        raise RuntimeError(
            f"Failed to parse configuration (yaml={yaml_path}): {exc}"
        ) from exc

    if yaml_path is not None:
        settings = settings.model_copy(update={"config_path": str(yaml_path)})

    _validate_required(settings)
    _SETTINGS_CACHE = settings
    return settings


def _validate_required(s: Settings) -> None:
    """Fail fast on common misconfiguration."""
    problems: list[str] = []
    if not s.storage.traindata_root:
        problems.append("storage.traindata_root is required")
    if not s.predictor.base_url:
        problems.append("predictor.base_url is required")
    if s.feishu.enabled:
        if not s.feishu.app_id:
            problems.append("feishu.app_id is required when feishu.enabled=true")
        if not s.feishu.app_secret:
            problems.append("feishu.app_secret is required when feishu.enabled=true")
        if not s.feishu.targets:
            problems.append("feishu.targets must contain at least one chat")
    if problems:
        raise RuntimeError(
            "Invalid configuration:\n  - " + "\n  - ".join(problems)
        )


def get_settings() -> Settings:
    """Lazily-cached accessor used by DI."""
    return load_settings()


def reset_settings_cache() -> None:
    """Testing hook."""
    global _SETTINGS_CACHE
    _SETTINGS_CACHE = None
