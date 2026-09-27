"""SQLite 持久化层：任务与消费位点落盘，进程重启后可恢复。"""
from __future__ import annotations

import sqlite3
import threading

from jobqueue.models import Job, JobStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    dedup_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    lease_owner TEXT,
    lease_expires_at REAL,
    next_attempt_at REAL NOT NULL,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_dedup ON jobs(dedup_key, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_due ON jobs(status, next_attempt_at);
CREATE TABLE IF NOT EXISTS offsets (
    consumer_group TEXT PRIMARY KEY,
    offset INTEGER NOT NULL
);
"""

_COLUMNS = (
    "job_id, dedup_key, payload_hash, payload, status, attempts, max_attempts, seq,"
    " lease_owner, lease_expires_at, next_attempt_at, last_error, created_at, updated_at"
)


def _row_to_job(row: sqlite3.Row) -> Job:
    return Job(
        job_id=row["job_id"],
        dedup_key=row["dedup_key"],
        payload_hash=row["payload_hash"],
        payload=row["payload"],
        status=JobStatus(row["status"]),
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        seq=row["seq"],
        lease_owner=row["lease_owner"],
        lease_expires_at=row["lease_expires_at"],
        next_attempt_at=row["next_attempt_at"],
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class JobStorage:
    """基于本地 SQLite 文件的持久化，进程重启后状态可恢复。

    单连接 + 锁，写操作串行化，保证并发消费者下不会出现双执行。
    """

    def __init__(self, db_path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def insert_job(self, job: Job) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                f"INSERT INTO jobs ({_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                _job_to_tuple(job),
            )

    def get_by_dedup_key(self, dedup_key: str, window_start: float) -> Job | None:
        """返回去重窗口内（created_at >= window_start）最新的同 key 任务。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE dedup_key = ? AND created_at >= ?"
                " ORDER BY created_at DESC LIMIT 1",
                (dedup_key, window_start),
            ).fetchone()
        return _row_to_job(row) if row else None

    def get_by_id(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return _row_to_job(row) if row else None

    def update_job(self, job: Job) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE jobs SET {', '.join(c + ' = ?' for c in _COLUMNS.split(', '))}"
                " WHERE job_id = ?",
                (*_job_to_tuple(job), job.job_id),
            )

    def fetch_due(self, now: float, limit: int) -> list[Job]:
        """取到期可投递的 PENDING 任务，按 seq 升序。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status = ? AND next_attempt_at <= ?"
                " ORDER BY seq ASC LIMIT ?",
                (JobStatus.PENDING.value, now, limit),
            ).fetchall()
        return [_row_to_job(r) for r in rows]

    def fetch_expired_leases(self, now: float) -> list[Job]:
        """取租约已过期的 LEASED 任务（消费者中断/超时待回收）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status = ? AND lease_expires_at <= ?",
                (JobStatus.LEASED.value, now),
            ).fetchall()
        return [_row_to_job(r) for r in rows]

    def max_seq(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM jobs").fetchone()
        return int(row["m"])

    def get_offset(self, consumer_group: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT offset FROM offsets WHERE consumer_group = ?", (consumer_group,)
            ).fetchone()
        return int(row["offset"]) if row else 0

    def set_offset(self, consumer_group: str, offset: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO offsets (consumer_group, offset) VALUES (?, ?)"
                " ON CONFLICT(consumer_group) DO UPDATE SET offset = excluded.offset",
                (consumer_group, offset),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def _job_to_tuple(job: Job) -> tuple:
    return (
        job.job_id, job.dedup_key, job.payload_hash, job.payload, job.status.value,
        job.attempts, job.max_attempts, job.seq, job.lease_owner, job.lease_expires_at,
        job.next_attempt_at, job.last_error, job.created_at, job.updated_at,
    )
