"""Durable job queue: idempotent submit, leasing, retry and checkpoints.

Delivery semantics
------------------
* At-least-once delivery per consumer group.  A job is handed out under a
  lease; the consumer must ``ack`` (success) or ``nack`` (failure) before
  the lease expires, otherwise the job is automatically re-queued and may
  be delivered again.
* A job is leased to exactly one consumer at a time (lease token +
  expiry), so it is never executed concurrently twice.  Once a job has
  reached a terminal state (SUCCEEDED / DEAD) it is never delivered again
  and any late or duplicate ack/nack is rejected with ALREADY_COMPLETED.

Completion semantics (out-of-order completion)
----------------------------------------------
* A valid ack/nack settles the *job itself* immediately, independently of
  earlier offsets: a succeeded job becomes SUCCEEDED (with its result
  queryable) and a definitively failed job becomes DEAD even while jobs
  with smaller offsets are still in flight.
* The per-group checkpoint is the *contiguous completion boundary*: after
  every settlement it advances across every consecutive terminal job from
  the committed offset and stops at the first unfinished "hole".  When the
  hole is later settled, the boundary automatically advances over the
  already-settled jobs behind it -- nothing is re-run.
* The checkpoint therefore never skips a non-terminal job and never moves
  backwards.  Out-of-order, duplicate, or repeated acknowledgements cannot
  corrupt it.
* Legacy callers may still pass ``result_offset`` with ack: it is validated
  for compatibility (regression / mismatch rejected) but no longer drives
  the checkpoint -- the contiguous boundary does.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from .config import QueueConfig
from .models import (
    AckResult,
    Job,
    JobStatus,
    Lease,
    SubmitResult,
    SubmitStatus,
    TerminalReason,
)
from .store import JobStore

_TERMINAL_STATUSES = {JobStatus.SUCCEEDED, JobStatus.DEAD}


class JobQueue:
    """Coordinates the store, retry policy and consumer checkpoints."""

    def __init__(self, config: QueueConfig | None = None) -> None:
        self._config = config or QueueConfig()
        self._store = JobStore(self._config.db_path)
        self._lock = threading.RLock()

    def close(self) -> None:
        self._store.close()

    # ------------------------------------------------------------------
    # submission
    # ------------------------------------------------------------------
    def submit(
        self,
        idempotency_key: str,
        payload: dict,
        *,
        max_attempts: int | None = None,
        now: float | None = None,
    ) -> SubmitResult:
        """Submit a job with idempotent de-duplication.

        * Same key + same payload inside the dedup window -> DUPLICATE.
        * Same key + different payload -> CONFLICT (never overwritten).
        * Key reused after the dedup window expired -> REJECTED, the
          caller must pick a fresh key (no silent overwrite).
        * Invalid arguments -> REJECTED.
        """
        now = self._now(now)
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            return SubmitResult(SubmitStatus.REJECTED, reason="idempotency_key must be a non-empty string")
        if not isinstance(payload, dict):
            return SubmitResult(SubmitStatus.REJECTED, reason="payload must be a dict")
        attempts_limit = max_attempts if max_attempts is not None else self._config.max_attempts
        if not isinstance(attempts_limit, int) or attempts_limit < 1:
            return SubmitResult(SubmitStatus.REJECTED, reason="max_attempts must be an integer >= 1")

        with self._lock:
            existing = self._store.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                if existing.payload != payload:
                    return SubmitResult(
                        SubmitStatus.CONFLICT,
                        job_id=existing.job_id,
                        reason="idempotency_key already exists with a different payload",
                    )
                if now - existing.created_at <= self._config.dedup_window_seconds:
                    return SubmitResult(
                        SubmitStatus.DUPLICATE,
                        job_id=existing.job_id,
                        reason="duplicate submission inside dedup window; existing job returned",
                    )
                return SubmitResult(
                    SubmitStatus.REJECTED,
                    job_id=existing.job_id,
                    reason="idempotency_key already used outside dedup window; choose a new key",
                )

            job = Job(
                job_id=uuid.uuid4().hex,
                idempotency_key=idempotency_key,
                payload=payload,
                status=JobStatus.PENDING,
                attempts=0,
                max_attempts=attempts_limit,
                created_at=now,
                updated_at=now,
                visible_at=now,
                offset=self._store.next_offset(),
            )
            self._store.insert_job(job)
            return SubmitResult(SubmitStatus.ACCEPTED, job_id=job.job_id)

    # ------------------------------------------------------------------
    # consumption
    # ------------------------------------------------------------------
    def lease(
        self,
        consumer_group: str,
        consumer_id: str,
        now: float | None = None,
    ) -> Lease | None:
        """Lease the next deliverable job for a consumer group.

        Expired leases are re-queued first (crash/timeout recovery), then
        the lowest-offset job past the group checkpoint that is due is
        leased to ``consumer_id``.  Returns ``None`` when nothing is due.
        """
        now = self._now(now)
        with self._lock:
            self._requeue_expired_locked(now)
            checkpoint = self._store.get_checkpoint(consumer_group)
            for job in self._store.list_jobs():
                if job.offset <= checkpoint:
                    continue
                if job.status not in (JobStatus.PENDING, JobStatus.FAILED_RETRYABLE):
                    continue
                if job.visible_at > now:
                    continue
                job.status = JobStatus.LEASED
                job.attempts += 1  # one attempt per delivery, counted exactly once
                job.lease_owner = consumer_id
                job.lease_token = uuid.uuid4().hex
                job.lease_expires_at = now + self._config.visibility_timeout_seconds
                job.updated_at = now
                self._store.update_job(job)
                return Lease(job=job, lease_token=job.lease_token, lease_expires_at=job.lease_expires_at)
            return None

    def ack(
        self,
        consumer_group: str,
        consumer_id: str,
        job_id: str,
        lease_token: str,
        result_offset: int | None = None,
        result: Any = None,
        now: float | None = None,
    ) -> AckResult:
        """Acknowledge successful processing.

        The job settles to SUCCEEDED immediately and stores ``result`` --
        regardless of whether smaller-offset jobs have already settled.
        Afterwards the group checkpoint advances to the contiguous
        completion boundary (see module docstring).

        ``result_offset`` is accepted for backwards compatibility and, when
        given, is still validated: a value at or below the committed
        checkpoint is CHECKPOINT_REGRESSION and a value that does not match
        the acked job's offset is CHECKPOINT_OUT_OF_ORDER.  It never drives
        the checkpoint by itself.
        """
        now = self._now(now)
        with self._lock:
            job = self._store.get_job(job_id)
            if job is None:
                return AckResult.NOT_FOUND
            # an already settled job can never be acknowledged again, not
            # even by a late consumer holding an old token
            if job.status in _TERMINAL_STATUSES:
                return AckResult.ALREADY_COMPLETED
            lease_check = self._check_lease(job, consumer_id, lease_token, now)
            if lease_check is not AckResult.OK:
                return lease_check

            # legacy offset argument: validated for compatibility only
            if result_offset is not None:
                checkpoint = self._store.get_checkpoint(consumer_group)
                if result_offset <= checkpoint:
                    return AckResult.CHECKPOINT_REGRESSION
                if result_offset != job.offset:
                    return AckResult.CHECKPOINT_OUT_OF_ORDER
                max_offset = self._store.next_offset() - 1
                if result_offset > max_offset:
                    return AckResult.CHECKPOINT_OUT_OF_ORDER

            job.status = JobStatus.SUCCEEDED
            job.terminal_reason = TerminalReason.SUCCEEDED
            job.last_error = None
            job.result = result
            self._clear_lease(job, now)
            self._store.update_job(job)
            self._advance_checkpoint_locked(consumer_group)
            return AckResult.OK

    def nack(
        self,
        consumer_id: str,
        job_id: str,
        lease_token: str,
        error: str,
        *,
        retryable: bool = True,
        consumer_group: str | None = None,
        now: float | None = None,
    ) -> AckResult:
        """Report a processing failure.

        Retryable failures re-enter the queue with bounded exponential
        backoff until ``max_attempts`` deliveries happened, then the job
        goes DEAD with reason RETRIES_EXHAUSTED.  Non-retryable failures
        go DEAD immediately with reason NON_RETRYABLE.

        Reaching a terminal state settles the job immediately; when
        ``consumer_group`` is supplied the group checkpoint advances to the
        contiguous completion boundary, so a definitively failed job never
        blocks later jobs forever.
        """
        now = self._now(now)
        with self._lock:
            job = self._store.get_job(job_id)
            if job is None:
                return AckResult.NOT_FOUND
            if job.status in _TERMINAL_STATUSES:
                return AckResult.ALREADY_COMPLETED
            lease_check = self._check_lease(job, consumer_id, lease_token, now)
            if lease_check is not AckResult.OK:
                return lease_check

            job.last_error = error
            if not retryable:
                job.status = JobStatus.DEAD
                job.terminal_reason = TerminalReason.NON_RETRYABLE
            elif job.attempts >= job.max_attempts:
                job.status = JobStatus.DEAD
                job.terminal_reason = TerminalReason.RETRIES_EXHAUSTED
            else:
                job.status = JobStatus.FAILED_RETRYABLE
                job.terminal_reason = TerminalReason.NONE
                job.visible_at = now + self._backoff(job.attempts)
            self._clear_lease(job, now)
            self._store.update_job(job)
            if consumer_group is not None and job.status in _TERMINAL_STATUSES:
                self._advance_checkpoint_locked(consumer_group)
            return AckResult.OK

    # ------------------------------------------------------------------
    # inspection / maintenance
    # ------------------------------------------------------------------
    def get_job(self, job_id: str) -> Job | None:
        return self._store.get_job(job_id)

    def list_jobs(self) -> list[Job]:
        return self._store.list_jobs()

    def get_checkpoint(self, consumer_group: str) -> int:
        return self._store.get_checkpoint(consumer_group)

    def requeue_expired_leases(self, now: float | None = None) -> int:
        """Re-queue jobs whose lease expired without ack/nack.

        Called automatically by :meth:`lease`; exposed for tests and for
        operators who want to trigger recovery explicitly.
        """
        now = self._now(now)
        with self._lock:
            return self._requeue_expired_locked(now)

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    def _requeue_expired_locked(self, now: float) -> int:
        count = 0
        for job in self._store.list_jobs():
            if job.status is JobStatus.LEASED and job.lease_expires_at is not None and job.lease_expires_at <= now:
                job.status = JobStatus.PENDING
                job.visible_at = now
                self._clear_lease(job, now)
                self._store.update_job(job)
                count += 1
        return count

    def _advance_checkpoint_locked(self, consumer_group: str) -> int:
        """Move the checkpoint to the contiguous terminal-job boundary.

        Walks jobs in offset order from the committed offset + 1 and
        advances while jobs are terminal (SUCCEEDED / DEAD); stops at the
        first unfinished hole.  The value only ever grows, so concurrent,
        out-of-order or duplicate settlements can never regress it.
        """
        checkpoint = self._store.get_checkpoint(consumer_group)
        boundary = checkpoint
        for job in self._store.list_jobs():
            if job.offset <= checkpoint:
                continue
            if job.status not in _TERMINAL_STATUSES:
                break  # first unfinished job is the hole; boundary stops here
            boundary = job.offset
        if boundary > checkpoint:
            self._store.set_checkpoint(consumer_group, boundary)
        return boundary

    def _check_lease(self, job: Job, consumer_id: str, lease_token: str, now: float) -> AckResult:
        if job.status is not JobStatus.LEASED:
            return AckResult.NOT_LEASED
        if job.lease_expires_at is not None and job.lease_expires_at <= now:
            return AckResult.NOT_LEASED
        if job.lease_owner != consumer_id or job.lease_token != lease_token:
            return AckResult.LEASE_MISMATCH
        return AckResult.OK

    def _clear_lease(self, job: Job, now: float) -> None:
        job.lease_owner = None
        job.lease_token = None
        job.lease_expires_at = None
        job.updated_at = now

    def _backoff(self, attempts: int) -> float:
        delay = self._config.backoff_base_seconds * (2 ** (attempts - 1))
        return min(delay, self._config.backoff_max_seconds)

    @staticmethod
    def _now(now: float | None) -> float:
        return time.time() if now is None else now
