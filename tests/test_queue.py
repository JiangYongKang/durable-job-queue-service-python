"""Behavioural tests for the durable job queue.

Each test logs its key inputs and the decision basis (expected vs actual
status / reason) so failures are explainable from the log alone.
"""

from __future__ import annotations

import logging
import threading

import pytest

from jobqueue import JobQueue, QueueConfig
from jobqueue.models import (
    AckResult,
    JobStatus,
    SubmitStatus,
    TerminalReason,
)

logger = logging.getLogger("test.queue")

T0 = 1_000.0


@pytest.fixture()
def queue() -> JobQueue:
    q = JobQueue(
        QueueConfig(
            db_path=":memory:",
            dedup_window_seconds=60,
            max_attempts=3,
            backoff_base_seconds=10,
            backoff_max_seconds=100,
            visibility_timeout_seconds=5,
        )
    )
    yield q
    q.close()


# ----------------------------------------------------------------------
# duplicate submission / idempotency
# ----------------------------------------------------------------------
class TestIdempotentSubmit:
    def test_duplicate_same_payload_returns_existing_job(self, queue: JobQueue):
        first = queue.submit("order-1", {"amount": 10}, now=T0)
        logger.info("input: key=order-1 payload={'amount': 10} -> %s", first.status)
        assert first.status is SubmitStatus.ACCEPTED

        dup = queue.submit("order-1", {"amount": 10}, now=T0 + 1)
        logger.info(
            "input: same key+payload inside window=%ss -> status=%s job_id=%s (expect DUPLICATE, same id)",
            60, dup.status, dup.job_id,
        )
        assert dup.status is SubmitStatus.DUPLICATE
        assert dup.job_id == first.job_id
        assert len(queue.list_jobs()) == 1, "duplicate must not create a second job"

    def test_same_key_different_payload_is_conflict(self, queue: JobQueue):
        first = queue.submit("order-2", {"amount": 10}, now=T0)
        conflict = queue.submit("order-2", {"amount": 99}, now=T0 + 1)
        logger.info(
            "input: key=order-2 payload changed 10 -> 99 -> status=%s reason=%r (expect CONFLICT)",
            conflict.status, conflict.reason,
        )
        assert conflict.status is SubmitStatus.CONFLICT
        assert conflict.job_id == first.job_id
        job = queue.get_job(first.job_id)
        assert job.payload == {"amount": 10}, "conflict must never overwrite the stored payload"

    def test_key_reuse_after_window_is_rejected(self, queue: JobQueue):
        queue.submit("order-3", {"amount": 1}, now=T0)
        late = queue.submit("order-3", {"amount": 1}, now=T0 + 61)
        logger.info(
            "input: same key after window=60s elapsed -> status=%s reason=%r (expect REJECTED)",
            late.status, late.reason,
        )
        assert late.status is SubmitStatus.REJECTED
        assert len(queue.list_jobs()) == 1

    @pytest.mark.parametrize(
        "key,payload,max_attempts",
        [("", {"a": 1}, None), ("  ", {"a": 1}, None), ("k", "not-a-dict", None), ("k", {"a": 1}, 0)],
        ids=["empty-key", "blank-key", "non-dict-payload", "zero-max-attempts"],
    )
    def test_invalid_submissions_are_rejected(self, queue: JobQueue, key, payload, max_attempts):
        result = queue.submit(key, payload, max_attempts=max_attempts, now=T0)
        logger.info(
            "input: key=%r payload=%r max_attempts=%r -> status=%s reason=%r (expect REJECTED)",
            key, payload, max_attempts, result.status, result.reason,
        )
        assert result.status is SubmitStatus.REJECTED
        assert result.reason
        assert queue.list_jobs() == []


# ----------------------------------------------------------------------
# retry and terminal states
# ----------------------------------------------------------------------
class TestRetry:
    def _submit_and_lease(self, queue: JobQueue, key: str, now: float):
        queue.submit(key, {"task": key}, now=now)
        return queue.lease("g", "c1", now=now)

    def test_retryable_failure_uses_bounded_backoff(self, queue: JobQueue):
        lease = self._submit_and_lease(queue, "job-a", T0)
        assert queue.nack("c1", lease.job.job_id, lease.lease_token, "boom", now=T0) is AckResult.OK
        job = queue.get_job(lease.job.job_id)
        logger.info(
            "attempts=%d -> backoff=%ss (base=10 * 2^0), visible_at=%s, status=%s",
            job.attempts, job.visible_at - T0, job.visible_at, job.status,
        )
        assert job.status is JobStatus.FAILED_RETRYABLE
        assert job.visible_at == T0 + 10
        assert queue.lease("g", "c1", now=T0 + 9) is None, "must not be visible before backoff elapses"
        assert queue.lease("g", "c1", now=T0 + 10) is not None

    def test_retry_exhaustion_goes_dead_and_stays_queryable(self, queue: JobQueue):
        # visibility_timeout=5s: nack 1s after each lease, i.e. inside the lease.
        # backoff schedule with base=10: 10s, 20s, then DEAD at attempt 3.
        lease = self._submit_and_lease(queue, "job-b", T0)
        job_id = lease.job.job_id
        clock = T0
        for round_no in range(1, 4):  # max_attempts=3
            job = queue.get_job(job_id)
            logger.info(
                "round=%d now=%s attempts=%d/%d status=%s",
                round_no, clock, job.attempts, job.max_attempts, job.status,
            )
            assert job.attempts == round_no, "each delivery must count exactly one attempt"
            result = queue.nack("c1", job_id, lease.lease_token, f"boom-{round_no}", now=clock + 1)
            assert result is AckResult.OK
            job = queue.get_job(job_id)
            clock = job.visible_at  # jump to the moment the job becomes visible again
            lease = queue.lease("g", "c1", now=clock)

        job = queue.get_job(job_id)
        logger.info(
            "final: status=%s terminal_reason=%s last_error=%r (expect DEAD / RETRIES_EXHAUSTED)",
            job.status, job.terminal_reason, job.last_error,
        )
        assert lease is None, "dead job must never be delivered again"
        assert job.status is JobStatus.DEAD
        assert job.terminal_reason is TerminalReason.RETRIES_EXHAUSTED
        assert job.attempts == 3, "attempts must not accumulate beyond deliveries"
        assert job.last_error == "boom-3"

    def test_non_retryable_failure_goes_dead_immediately(self, queue: JobQueue):
        lease = self._submit_and_lease(queue, "job-c", T0)
        queue.nack("c1", lease.job.job_id, lease.lease_token, "bad input", retryable=False, now=T0)
        job = queue.get_job(lease.job.job_id)
        logger.info(
            "non-retryable failure -> status=%s terminal_reason=%s (expect DEAD / NON_RETRYABLE)",
            job.status, job.terminal_reason,
        )
        assert job.status is JobStatus.DEAD
        assert job.terminal_reason is TerminalReason.NON_RETRYABLE
        assert job.attempts == 1


# ----------------------------------------------------------------------
# checkpoint management
# ----------------------------------------------------------------------
class TestCheckpoint:
    def test_ack_advances_checkpoint_and_success_is_terminal(self, queue: JobQueue):
        queue.submit("cp-1", {}, now=T0)
        lease = queue.lease("g", "c1", now=T0)
        result = queue.ack("g", "c1", lease.job.job_id, lease.lease_token, result_offset=1, now=T0)
        logger.info("ack offset=1 checkpoint=0 -> %s (expect OK)", result)
        assert result is AckResult.OK
        assert queue.get_checkpoint("g") == 1
        job = queue.get_job(lease.job.job_id)
        assert job.status is JobStatus.SUCCEEDED
        assert job.terminal_reason is TerminalReason.SUCCEEDED

    def test_checkpoint_regression_is_rejected(self, queue: JobQueue):
        queue.submit("cp-2", {}, now=T0)
        queue.submit("cp-3", {}, now=T0)
        l1 = queue.lease("g", "c1", now=T0)
        assert queue.ack("g", "c1", l1.job.job_id, l1.lease_token, 1, now=T0) is AckResult.OK
        l2 = queue.lease("g", "c1", now=T0)
        result = queue.ack("g", "c1", l2.job.job_id, l2.lease_token, 1, now=T0)
        logger.info(
            "ack offset=1 while checkpoint=1 -> %s (expect CHECKPOINT_REGRESSION)",
            result,
        )
        assert result is AckResult.CHECKPOINT_REGRESSION
        assert queue.get_checkpoint("g") == 1, "rejected ack must not move the checkpoint"

    def test_out_of_order_offset_is_rejected(self, queue: JobQueue):
        queue.submit("cp-4", {}, now=T0)
        lease = queue.lease("g", "c1", now=T0)
        result = queue.ack("g", "c1", lease.job.job_id, lease.lease_token, 999, now=T0)
        logger.info(
            "ack offset=999 for job.offset=%d -> %s (expect CHECKPOINT_OUT_OF_ORDER)",
            lease.job.offset, result,
        )
        assert result is AckResult.CHECKPOINT_OUT_OF_ORDER
        assert queue.get_checkpoint("g") == 0

    def test_wrong_lease_token_is_rejected(self, queue: JobQueue):
        queue.submit("cp-5", {}, now=T0)
        lease = queue.lease("g", "c1", now=T0)
        result = queue.ack("g", "c2", lease.job.job_id, "forged-token", 1, now=T0)
        logger.info("ack with forged token/consumer -> %s (expect LEASE_MISMATCH)", result)
        assert result is AckResult.LEASE_MISMATCH
        assert queue.get_job(lease.job.job_id).status is JobStatus.LEASED


# ----------------------------------------------------------------------
# crash recovery / redelivery
# ----------------------------------------------------------------------
class TestRedelivery:
    def test_interrupted_consumer_triggers_redelivery(self, queue: JobQueue):
        queue.submit("crash-1", {}, now=T0)
        lease = queue.lease("g", "c1", now=T0)
        logger.info(
            "c1 leased job %s, lease expires at %s; c1 crashes without ack/nack",
            lease.job.job_id, lease.lease_expires_at,
        )
        # still leased: no concurrent delivery before the timeout
        assert queue.lease("g", "c2", now=T0 + 4) is None
        # after the visibility timeout the job is re-queued and redelivered
        requeued = queue.requeue_expired_leases(now=T0 + 5)
        logger.info("after timeout: requeued=%d (expect 1)", requeued)
        assert requeued == 1
        job = queue.get_job(lease.job.job_id)
        assert job.status is JobStatus.PENDING
        assert job.attempts == 1, "requeue must not double-count the attempt"

        lease2 = queue.lease("g", "c2", now=T0 + 5)
        assert lease2 is not None and lease2.job.job_id == lease.job.job_id
        assert lease2.job.attempts == 2, "redelivery counts as the next attempt"
        # the stale lease from the crashed consumer is now useless
        stale = queue.ack("g", "c1", lease.job.job_id, lease.lease_token, 1, now=T0 + 6)
        logger.info("stale ack from crashed c1 -> %s (expect LEASE_MISMATCH)", stale)
        assert stale is AckResult.LEASE_MISMATCH

    def test_concurrent_consumers_never_get_same_job(self, queue: JobQueue):
        for i in range(20):
            queue.submit(f"conc-{i}", {"i": i}, now=T0)
        leased_ids: list[str] = []
        lock = threading.Lock()

        def worker(group: str, consumer: str):
            while True:
                lease = queue.lease(group, consumer, now=T0)
                if lease is None:
                    return
                with lock:
                    leased_ids.append(lease.job.job_id)
                queue.ack(group, consumer, lease.job.job_id, lease.lease_token,
                        lease.job.offset, now=T0)

        threads = [threading.Thread(target=worker, args=("g", f"c{n}")) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        logger.info("leased=%d unique=%d (expect equal)", len(leased_ids), len(set(leased_ids)))
        assert len(leased_ids) == 20
        assert len(set(leased_ids)) == 20, "no job may be executed by two consumers"
        assert queue.get_checkpoint("g") == 20


# ----------------------------------------------------------------------
# durability across restarts
# ----------------------------------------------------------------------
class TestRestart:
    def test_state_survives_reopen(self, tmp_path):
        db = str(tmp_path / "queue.db")
        q1 = JobQueue(QueueConfig(db_path=db, max_attempts=3))
        q1.submit("persist-1", {"v": 1}, now=T0)
        lease = q1.lease("g", "c1", now=T0)
        q1.ack("g", "c1", lease.job.job_id, lease.lease_token, 1, now=T0)
        q1.submit("persist-2", {"v": 2}, now=T0)
        q1.close()
        logger.info("process 'restarted': reopening db file %s", db)

        q2 = JobQueue(QueueConfig(db_path=db, max_attempts=3))
        jobs = {j.idempotency_key: j for j in q2.list_jobs()}
        logger.info(
            "after restart: jobs=%s checkpoint=%s",
            {k: v.status.value for k, v in jobs.items()}, q2.get_checkpoint("g"),
        )
        assert jobs["persist-1"].status is JobStatus.SUCCEEDED
        assert jobs["persist-2"].status is JobStatus.PENDING
        assert q2.get_checkpoint("g") == 1
        # duplicate submit after restart still deduplicates
        dup = q2.submit("persist-1", {"v": 1}, now=T0 + 1)
        assert dup.status is SubmitStatus.DUPLICATE
        # next lease is the pending job at offset 2, not the succeeded one
        lease2 = q2.lease("g", "c1", now=T0 + 1)
        assert lease2.job.idempotency_key == "persist-2"
        q2.close()
