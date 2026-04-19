"""MinIO downloader.

Primary path uses the MinIO SDK with configured credentials.
Fallback path (toggleable) streams the ``download_url`` directly over HTTP.
MD5 is verified after download with configurable retries.
"""

from __future__ import annotations

import hashlib
import shutil
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx

from ..core.config import MinioSection
from ..core.errors import DownloadError, Md5MismatchError
from ..core.logging import logger, mask_secret


try:  # SDK is optional at runtime
    from minio import Minio
    from minio.error import S3Error
except Exception:  # pragma: no cover
    Minio = None  # type: ignore[assignment]
    S3Error = Exception  # type: ignore[assignment]


def _compute_md5(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.md5()
    with path.open("rb") as fp:
        while True:
            buf = fp.read(chunk)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


class MinioDownloader:
    def __init__(self, cfg: MinioSection) -> None:
        self._cfg = cfg
        Path(cfg.download_dir).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def download(self, *, object_name: str, download_url: str, expected_md5: str) -> Path:
        """Download archive and verify MD5. Returns the local path."""
        dest = Path(self._cfg.download_dir) / Path(object_name).name
        if dest.exists():
            dest.unlink()

        last_err: Optional[Exception] = None
        for attempt in range(1, self._cfg.md5_retry + 1):
            try:
                self._download_once(
                    object_name=object_name,
                    download_url=download_url,
                    dest=dest,
                )
                actual = _compute_md5(dest)
                if actual.lower() == expected_md5.lower():
                    logger.info(
                        "archive downloaded and verified",
                        extra={"object_name": object_name, "md5": actual, "path": str(dest)},
                    )
                    return dest
                logger.warning(
                    "md5 mismatch",
                    extra={
                        "attempt": attempt,
                        "expected_md5": expected_md5,
                        "actual_md5": actual,
                    },
                )
                last_err = Md5MismatchError(
                    f"md5 mismatch on attempt {attempt}: expected={expected_md5} actual={actual}"
                )
            except Exception as exc:  # noqa: BLE001 — we re-raise below
                last_err = exc
                logger.warning(
                    "download attempt failed",
                    extra={"attempt": attempt, "error": str(exc)},
                )

            if attempt < self._cfg.md5_retry:
                time.sleep(self._cfg.md5_retry_interval_sec)

        # all attempts failed — caller decides whether to preserve artifacts.
        raise DownloadError(
            f"download failed after {self._cfg.md5_retry} attempts: {last_err}"
        ) from last_err

    def cleanup(self, path: Path) -> None:
        try:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
        except Exception:  # pragma: no cover
            logger.opt(exception=True).warning("cleanup failed")

    def ping(self) -> bool:
        """Lightweight connectivity probe for /health. Returns True when reachable."""
        try:
            client = self._sdk_client()
            if client is None:
                return False
            client.bucket_exists(self._cfg.bucket)
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _download_once(
        self, *, object_name: str, download_url: str, dest: Path
    ) -> None:
        client = self._sdk_client()
        full_object = self._full_object_name(object_name, download_url)
        if client is not None:
            try:
                logger.info(
                    "downloading via MinIO SDK",
                    extra={
                        "bucket": self._cfg.bucket,
                        "object_name": full_object,
                        "endpoint": self._cfg.endpoint,
                        "access_key": mask_secret(self._cfg.access_key),
                    },
                )
                client.fget_object(self._cfg.bucket, full_object, str(dest))
                return
            except S3Error as exc:  # type: ignore[misc]
                logger.warning(
                    "MinIO SDK download failed",
                    extra={"error": str(exc), "object_name": full_object},
                )
                if not self._cfg.fallback_to_http:
                    raise DownloadError(f"MinIO SDK error: {exc}") from exc
            except Exception as exc:
                logger.opt(exception=True).warning("MinIO SDK crash; will try HTTP fallback")
                if not self._cfg.fallback_to_http:
                    raise DownloadError(f"MinIO SDK crash: {exc}") from exc

        # HTTP fallback
        self._download_via_http(download_url, dest)

    def _sdk_client(self):
        if Minio is None:
            return None
        if not self._cfg.endpoint or not self._cfg.access_key or not self._cfg.secret_key:
            return None
        return Minio(
            self._cfg.endpoint,
            access_key=self._cfg.access_key,
            secret_key=self._cfg.secret_key,
            secure=self._cfg.secure,
        )

    def _full_object_name(self, object_name: str, download_url: str) -> str:
        """Join configured prefix with object_name.

        If ``object_name`` already contains a path segment we trust it.
        Otherwise fall back to ``object_prefix``; otherwise derive from ``download_url``.
        """
        if "/" in object_name.strip("/"):
            return object_name
        if self._cfg.object_prefix:
            prefix = self._cfg.object_prefix.strip("/")
            return f"{prefix}/{object_name}" if prefix else object_name
        # derive from URL path
        parsed = urlparse(download_url)
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 2:
            return "/".join(parts[1:])  # skip bucket segment
        return object_name

    def _download_via_http(self, url: str, dest: Path) -> None:
        logger.info("downloading via HTTP fallback", extra={"url": url})
        with httpx.stream(
            "GET",
            url,
            timeout=self._cfg.http_timeout_sec,
            follow_redirects=True,
        ) as resp:
            resp.raise_for_status()
            with dest.open("wb") as fp:
                for chunk in resp.iter_bytes():
                    fp.write(chunk)


# Public helper for tests.
compute_md5 = _compute_md5
