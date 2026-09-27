"""SQLite-backed durable storage for jobs and consumer checkpoints.

Everything is stored in one local SQLite database (or ``:memory:`` for
tests).  A process restart that reopens the same file therefore sees the
exact same job states and checkpoints.
"""

from __future__ import annotations

import json
import sqlite3
import threading

from .models import Job, JobStatus, TerminalReason

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id           TEXT PRIMARY KEY,
    idempotency_key  TEXT NOT NULL,
    payload          TEXT NOT NULL,
    status           TEXT NOT NULL,
    attempts         INTEGER NOT NULL,
    max_attempts     INTEGER NOT NULL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    visible_at       REAL NOT NULL,
    lease_owner      TEXT,
    lease_token      TEXT,
    lease_expires_at REAL,
    last_error       TEXT,
    terminal_reason  TEXT NOT NULL,
    offset           INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON jobs (idempotency_key);

CREATE TABLE IF NOT EXISTS checkpoints (
    consumer_group TEXT PRIMARY KEY,
    offset         INTEGER NOT NULL
);
"""


class JobStore:
    """Persists jobs and consumer-group checkpoints in a local SQLite file."""

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --- jobs ---------------------------------------------------------
    def insert_job(self, job: Job) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO jobs (
                    job_id, idempotency_key, payload, status, attempts,
                    max_attempts, created_at, updated_at, visible_at,
                    lease_owner, lease_token, lease_expires_at,
                    last_error, terminal_reason, offset
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._to_row(job),
            )

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return self._from_row(row) if row else None

    def get_by_idempotency_key(self, key: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key = ?", (key,)
            ).fetchone()
        return self._from_row(row) if row else None

    def update_job(self, job: Job) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE jobs SET
                    payload = ?, status = ?, attempts = ?, max_attempts = ?,
                    created_at = ?, updated_at = ?, visible_at = ?,
                    lease_owner = ?, lease_token = ?, lease_expires_at = ?,
                    last_error = ?, terminal_reason = ?, offset = ?
                WHERE job_id = ?
                """,
                (
                    json.dumps(job.payload, sort_keys=True),
                    job.status.value,
                    job.attempts,
                    job.max_attempts,
                    job.created_at,
                    job.updated_at,
                    job.visible_at,
                    job.lease_owner,
                    job.lease_token,
                    job.lease_expires_at,
                    job.last_error,
                    job.terminal_reason.value,
                    job.offset,
                    job.job_id,
                ),
            )

    def next_offset(self) -> int:
        """Return the next monotonic consumption offset (starts at 1)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(offset), 0) + 1 AS nxt FROM jobs"
            ).fetchone()
        return int(row["nxt"])

    def list_jobs(self) -> list[Job]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs ORDER BY offset"
            ).fetchall()
        return [self._from_row(r) for r in rows]

    # --- checkpoints ----------------------------------------------------
    def get_checkpoint(self, consumer_group: str) -> int:
        """Committed offset for a group; 0 means 'nothing consumed yet'."""
        with self._lock:
            row = self._conn.execute(
                "SELECT offset FROM checkpoints WHERE consumer_group = ?",
                (consumer_group,),
            ).fetchone()
        return int(row["offset"]) if row else 0

    def set_checkpoint(self, consumer_group: str, offset: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO checkpoints (consumer_group, offset)
                VALUES (?, ?)
                ON CONFLICT(consumer_group) DO UPDATE SET offset = excluded.offset
                """,
                (consumer_group, offset),
            )

    # --- (de)serialisation ------------------------------------------------
    @staticmethod
    def _to_row(job: Job) -> tuple:
        return (
            job.job_id,
            job.idempotency_key,
            json.dumps(job.payload, sort_keys=True),
            job.status.value,
            job.attempts,
            job.max_attempts,
            job.created_at,
            job.updated_at,
            job.visible_at,
            job.lease_owner,
            job.lease_token,
            job.lease_expires_at,
            job.last_error,
            job.terminal_reason.value,
            job.offset,
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> Job:
        return Job(
            job_id=row["job_id"],
            idempotency_key=row["idempotency_key"],
            payload=json.loads(row["payload"]),
            status=JobStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            visible_at=row["visible_at"],
            lease_owner=row["lease_owner"],
            lease_token=row["lease_token"],
            lease_expires_at=row["lease_expires_at"],
            last_error=row["last_error"],
            terminal_reason=TerminalReason(row["terminal_reason"]),
            offset=row["offset"],
        )
