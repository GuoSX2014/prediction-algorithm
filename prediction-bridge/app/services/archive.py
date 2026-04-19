"""Archive extraction + atomic placement under ``traindata_root``."""

from __future__ import annotations

import shutil
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Literal

from ..core.errors import ArchiveError, StorageConflictError
from ..core.logging import logger


def _safe_extract(tar: tarfile.TarFile, dest: Path) -> None:
    """Reject path-traversal entries before extracting."""
    dest_resolved = dest.resolve()
    for member in tar.getmembers():
        target = (dest_resolved / member.name).resolve()
        if not str(target).startswith(str(dest_resolved)):
            raise ArchiveError(f"unsafe archive member: {member.name}")
    tar.extractall(dest)


def extract_tar_gz(archive_path: Path, workdir: Path) -> Path:
    """Extract ``archive_path`` into ``workdir`` and return the ``output/`` directory."""
    if not archive_path.is_file():
        raise ArchiveError(f"archive not found: {archive_path}")

    workdir.mkdir(parents=True, exist_ok=True)
    logger.info(
        "extracting archive", extra={"archive": str(archive_path), "workdir": str(workdir)}
    )
    with tarfile.open(archive_path, "r:gz") as tar:
        _safe_extract(tar, workdir)

    output = workdir / "output"
    if not output.is_dir():
        # Fallback: top-level dir is not exactly "output" — take the first dir.
        candidates = [p for p in workdir.iterdir() if p.is_dir()]
        if len(candidates) == 1:
            logger.warning(
                "archive root was %s, not 'output/'; using it as-is",
                candidates[0].name,
            )
            return candidates[0]
        raise ArchiveError(
            f"'output/' directory not found in archive: {archive_path}"
        )
    return output


def place_into_traindata(
    source_dir: Path,
    traindata_root: Path,
    data_date: str,
    on_conflict: Literal["overwrite", "skip"] = "overwrite",
) -> Path:
    """Move ``source_dir`` to ``<traindata_root>/<data_date>`` atomically.

    Conflict handling:
      - overwrite: backup existing to ``<data_date>.bak-<ts>`` then replace
      - skip:       log warning and return existing target untouched

    Implementation uses rename when same filesystem; falls back to copy+swap.
    """
    traindata_root = traindata_root.expanduser()
    traindata_root.mkdir(parents=True, exist_ok=True)
    target = traindata_root / data_date

    if target.exists():
        if on_conflict == "skip":
            logger.warning(
                "target traindata dir exists; skipping per config",
                extra={"target": str(target)},
            )
            return target
        ts = time.strftime("%Y%m%d%H%M%S")
        backup = traindata_root / f"{data_date}.bak-{ts}"
        logger.info(
            "backing up existing traindata dir",
            extra={"target": str(target), "backup": str(backup)},
        )
        try:
            target.rename(backup)
        except OSError as exc:
            raise StorageConflictError(f"failed to back up existing target: {exc}") from exc

    # Stage next to the target so an atomic rename is possible.
    staging = traindata_root / f".{data_date}.staging-{time.strftime('%Y%m%d%H%M%S')}"
    try:
        if staging.exists():
            shutil.rmtree(staging)
        try:
            # shutil.move handles cross-device by falling back to copy+remove.
            shutil.move(str(source_dir), str(staging))
        except Exception as exc:
            raise ArchiveError(f"failed to stage data into traindata_root: {exc}") from exc
        try:
            staging.rename(target)
        except OSError as exc:
            raise ArchiveError(f"failed to rename staging to target: {exc}") from exc
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)

    logger.info(
        "placed archive into traindata",
        extra={"target": str(target), "data_date": data_date},
    )
    return target


def make_workdir(parent: Path | None = None) -> Path:
    """Create a temporary working directory."""
    base = Path(parent) if parent else Path(tempfile.gettempdir())
    base.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="prediction-bridge-", dir=str(base)))
