"""Archive extraction + placement tests."""

from __future__ import annotations

import tarfile
from pathlib import Path

import pytest

from app.core.errors import ArchiveError
from app.services.archive import extract_tar_gz, make_workdir, place_into_traindata


def _make_tar(tmp: Path, payload: dict[str, bytes]) -> Path:
    archive = tmp / "sample.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for name, data in payload.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, __import__("io").BytesIO(data))
    return archive


def test_extract_outputs_output_dir(tmp_path: Path) -> None:
    archive = _make_tar(
        tmp_path,
        {"output/subdir/a.csv": b"foo,bar\n1,2\n"},
    )
    workdir = make_workdir(tmp_path)
    out = extract_tar_gz(archive, workdir)
    assert out.name == "output"
    assert (out / "subdir" / "a.csv").read_text() == "foo,bar\n1,2\n"


def test_extract_rejects_path_traversal(tmp_path: Path) -> None:
    archive = _make_tar(tmp_path, {"../evil.txt": b"nope"})
    workdir = make_workdir(tmp_path)
    with pytest.raises(ArchiveError):
        extract_tar_gz(archive, workdir)


def test_place_overwrite_creates_backup(tmp_path: Path) -> None:
    traindata = tmp_path / "traindata"
    traindata.mkdir()
    existing = traindata / "2026-03-26"
    existing.mkdir()
    (existing / "old.txt").write_text("old")

    staging_src = tmp_path / "src"
    staging_src.mkdir()
    (staging_src / "new.txt").write_text("new")

    result = place_into_traindata(
        source_dir=staging_src,
        traindata_root=traindata,
        data_date="2026-03-26",
        on_conflict="overwrite",
    )

    assert result == traindata / "2026-03-26"
    assert (result / "new.txt").read_text() == "new"
    backups = list(traindata.glob("2026-03-26.bak-*"))
    assert len(backups) == 1
    assert (backups[0] / "old.txt").read_text() == "old"


def test_place_skip_keeps_existing(tmp_path: Path) -> None:
    traindata = tmp_path / "traindata"
    traindata.mkdir()
    existing = traindata / "2026-03-26"
    existing.mkdir()
    (existing / "old.txt").write_text("old")

    staging_src = tmp_path / "src"
    staging_src.mkdir()
    (staging_src / "new.txt").write_text("new")

    result = place_into_traindata(
        source_dir=staging_src,
        traindata_root=traindata,
        data_date="2026-03-26",
        on_conflict="skip",
    )
    assert (result / "old.txt").read_text() == "old"
    assert not (result / "new.txt").exists()
