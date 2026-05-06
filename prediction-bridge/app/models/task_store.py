"""Task state storage.

Two backends: in-process dict (default) and SQLite file (for restarts).
Also tracks ``(object_name, md5)`` deduplication for idempotent callbacks.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

from .schemas import TaskRecord


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_dedup_key(object_name: str, md5: str) -> str:
    return f"{object_name}::{md5.lower()}"


class TaskStore(Protocol):
    def create(self, record: TaskRecord) -> None: ...
    def get(self, trace_id: str) -> Optional[TaskRecord]: ...
    def update(self, trace_id: str, **fields: Any) -> Optional[TaskRecord]: ...
    def set_stage(self, trace_id: str, stage: str, payload: Dict[str, Any]) -> None: ...
    def find_by_dedup(self, dedup_key: str, ttl_sec: int) -> Optional[TaskRecord]: ...
    def list_recent(self, limit: int = 50) -> List[TaskRecord]: ...


class InMemoryTaskStore:
    def __init__(self, max_records: int = 500, expire_after_days: int = 7) -> None:
        self._data: Dict[str, TaskRecord] = {}
        self._lock = threading.RLock()
        self._max_records = max_records
        self._expire_after_days = expire_after_days

    def create(self, record: TaskRecord) -> None:
        with self._lock:
            self._data[record.trace_id] = record
            self._evict_if_needed()

    def get(self, trace_id: str) -> Optional[TaskRecord]:
        with self._lock:
            return self._data.get(trace_id)

    def update(self, trace_id: str, **fields: Any) -> Optional[TaskRecord]:
        with self._lock:
            rec = self._data.get(trace_id)
            if rec is None:
                return None
            data = rec.model_dump()
            data.update(fields)
            data["updated_at"] = _now_iso()
            new_rec = TaskRecord(**data)
            self._data[trace_id] = new_rec
            return new_rec

    def set_stage(self, trace_id: str, stage: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            rec = self._data.get(trace_id)
            if rec is None:
                return
            stages = dict(rec.stages)
            stages[stage] = payload
            self.update(trace_id, stages=stages)

    def find_by_dedup(self, dedup_key: str, ttl_sec: int) -> Optional[TaskRecord]:
        with self._lock:
            cutoff = time.time() - ttl_sec
            # scan newest-first
            items = sorted(
                self._data.values(), key=lambda r: r.created_at, reverse=True
            )
            for rec in items:
                if rec.dedup_key != dedup_key:
                    continue
                try:
                    ts = datetime.fromisoformat(rec.created_at.replace("Z", "+00:00"))
                except ValueError:
                    continue
                if ts.timestamp() >= cutoff:
                    return rec
            return None

    def list_recent(self, limit: int = 50) -> List[TaskRecord]:
        with self._lock:
            return sorted(self._data.values(), key=lambda r: r.created_at, reverse=True)[:limit]

    def _evict_if_needed(self) -> None:
        """Remove expired records and enforce max_records limit (LRU eviction)."""
        # Step 1: Remove expired records
        if self._expire_after_days > 0:
            cutoff = time.time() - (self._expire_after_days * 86400)
            expired_ids = []
            for trace_id, rec in self._data.items():
                try:
                    ts = datetime.fromisoformat(rec.created_at.replace("Z", "+00:00"))
                    if ts.timestamp() < cutoff:
                        expired_ids.append(trace_id)
                except ValueError:
                    continue
            for trace_id in expired_ids:
                del self._data[trace_id]

        # Step 2: Enforce max_records limit (keep newest records)
        if len(self._data) > self._max_records:
            sorted_records = sorted(
                self._data.items(),
                key=lambda item: item[1].created_at,
                reverse=True
            )
            # Keep only the newest max_records
            to_keep = dict(sorted_records[:self._max_records])
            self._data = to_keep


class SQLiteTaskStore:
    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS tasks (
        trace_id TEXT PRIMARY KEY,
        dedup_key TEXT NOT NULL,
        created_at TEXT NOT NULL,
        payload TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_tasks_dedup ON tasks(dedup_key, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at DESC);
    """

    def __init__(self, path: str, max_records: int = 500, expire_after_days: int = 7) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._max_records = max_records
        self._expire_after_days = expire_after_days
        self._conn = sqlite3.connect(
            str(self._path),
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.executescript(self._SCHEMA)

    def _dump(self, rec: TaskRecord) -> str:
        return json.dumps(rec.model_dump(), ensure_ascii=False)

    def _load(self, payload: str) -> TaskRecord:
        return TaskRecord(**json.loads(payload))

    def create(self, record: TaskRecord) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO tasks(trace_id, dedup_key, created_at, payload) VALUES (?,?,?,?)",
                (record.trace_id, record.dedup_key, record.created_at, self._dump(record)),
            )
            self._evict_if_needed()

    def get(self, trace_id: str) -> Optional[TaskRecord]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT payload FROM tasks WHERE trace_id = ?", (trace_id,)
            )
            row = cur.fetchone()
            return self._load(row[0]) if row else None

    def update(self, trace_id: str, **fields: Any) -> Optional[TaskRecord]:
        with self._lock:
            rec = self.get(trace_id)
            if rec is None:
                return None
            data = rec.model_dump()
            data.update(fields)
            data["updated_at"] = _now_iso()
            new_rec = TaskRecord(**data)
            self._conn.execute(
                "UPDATE tasks SET payload = ? WHERE trace_id = ?",
                (self._dump(new_rec), trace_id),
            )
            return new_rec

    def set_stage(self, trace_id: str, stage: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            rec = self.get(trace_id)
            if rec is None:
                return
            stages = dict(rec.stages)
            stages[stage] = payload
            self.update(trace_id, stages=stages)

    def find_by_dedup(self, dedup_key: str, ttl_sec: int) -> Optional[TaskRecord]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT payload FROM tasks WHERE dedup_key = ? ORDER BY created_at DESC LIMIT 1",
                (dedup_key,),
            )
            row = cur.fetchone()
            if not row:
                return None
            rec = self._load(row[0])
            try:
                ts = datetime.fromisoformat(rec.created_at.replace("Z", "+00:00"))
            except ValueError:
                return None
            if ts.timestamp() >= (time.time() - ttl_sec):
                return rec
            return None

    def list_recent(self, limit: int = 50) -> List[TaskRecord]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT payload FROM tasks ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            return [self._load(row[0]) for row in cur.fetchall()]

    def _evict_if_needed(self) -> None:
        """Remove expired records and enforce max_records limit."""
        # Step 1: Remove expired records
        if self._expire_after_days > 0:
            cutoff_ts = time.time() - (self._expire_after_days * 86400)
            cutoff_iso = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            self._conn.execute(
                "DELETE FROM tasks WHERE created_at < ?",
                (cutoff_iso,)
            )

        # Step 2: Enforce max_records limit (keep newest records)
        count_cur = self._conn.execute("SELECT COUNT(*) FROM tasks")
        count = count_cur.fetchone()[0]
        if count > self._max_records:
            # Delete oldest records beyond max_records
            self._conn.execute(
                """
                DELETE FROM tasks WHERE trace_id IN (
                    SELECT trace_id FROM tasks ORDER BY created_at DESC LIMIT -1 OFFSET ?
                )
                """,
                (self._max_records,)
            )


def build_task_store(backend: str, sqlite_path: str, max_records: int = 500, expire_after_days: int = 7) -> TaskStore:
    if backend == "sqlite":
        return SQLiteTaskStore(sqlite_path, max_records, expire_after_days)
    return InMemoryTaskStore(max_records, expire_after_days)
