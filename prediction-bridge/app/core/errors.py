"""Domain exceptions."""

from __future__ import annotations


class BridgeError(Exception):
    """Base class for domain errors."""


class ConfigError(BridgeError):
    pass


class DownloadError(BridgeError):
    pass


class Md5MismatchError(DownloadError):
    pass


class ArchiveError(BridgeError):
    pass


class StorageConflictError(BridgeError):
    pass


class PredictorError(BridgeError):
    pass


class PredictorStartingError(PredictorError):
    """Predictor returned 503 - booting up."""


class FeishuError(BridgeError):
    pass
