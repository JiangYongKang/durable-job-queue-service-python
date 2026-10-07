"""Behavioural tests for out-of-order completion and frontier checkpoints.

Covers the concurrent-consumption completion semantics:
* a finished job reaches its terminal state immediately, regardless of
  unfinished earlier-offset jobs, and stays queryable (with its result);
* the checkpoint is the contiguous completion frontier: it stops at the
  first unfinished hole and auto-extends once the hole is filled;
* late confirmations from stale (pre-redelivery) leases are recognised
  and rejected, while retried acks from the completing lease are
  idempotent no-ops;
* a confirmed-complete job is never redelivered.

Each test logs its key inputs and the decision basis (expected vs actual
status / frontier) so failures are explainable from the log alone.
"""

from __future__ import annotations

import logging
import random
import threading

import pytest

from jobqueue import JobQueue, QueueConfig
from jobqueue.models import (
    AckResult,
    JobStatus,
    TerminalReason,
)

logger = logging.getLogger("test.out_of_order")

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


def _submit_n(queue: JobQueue, n: int, now: float = T0) -> list[str]:
    keys = [f"oo-{i}" for i in range(1, n + 1)]
    for key in keys:
        queue.submit(key, {"task": key}, now=now)
    return keys


def _lease_all(queue: JobQueue, group: str, n: int, now: float = T0) -> dict[int, object]:
    """Lease the next n jobs for the group, keyed by job offset."""
    leases = {}
    for i in range(n):
        lease = queue.lease(group, f"c{i}", now=now)
        assert lease is not None, f"expected job #{i + 1} to be leasable"
        leases[lease.job.offset] = lease
    return leases


# ----------------------------------------------------------------------
# out-of-order completion: immediate terminal state per job
# ----------------------------------------------------------------------
class TestOutOfOrderCompletion:
    def test_later_job_finishes_immediately_despite_earlier_hole(self, queue: JobQueue):
        _submit_n(queue, 3)
        leases = _lease_all(queue, "g", 3)

        # offset 3 finishes first, while offsets 1-2 are still in flight
        r3 = queue.ack("g", "c2", leases[3].job.job_id, leases[3].lease_token, 3,
                       result={"output": "done-3"}, now=T0)
        job3 = queue.get_job(leases[3].job.job_id)
        logger.info(
            "input: ack offset=3 while offsets 1-2 in flight -> ack=%s status=%s "
            "terminal_reason=%s result=%s checkpoint=%d "
            "(expect OK / SUCCEEDED / result queryable / frontier stuck at 0)",
            r3, job3.status, job3.terminal_reason, job3.result, queue.get_checkpoint("g"),
        )
        assert r3 is AckResult.OK
        assert job3.status is JobStatus.SUCCEEDED, "finished job must not linger in a middle state"
        assert job3.terminal_reason is TerminalReason.SUCCEEDED
        assert job3.result == {"output": "done-3"}, "processing result must be queryable"
        assert queue.get_checkpoint("g") == 0, "frontier must stop before the hole at offset 1"

        # offset 2 finishes next: terminal too, frontier still blocked by 1
        r2 = queue.ack("g", "c1", leases[2].job.job_id, leases[2].lease_token, 2, now=T0)
        logger.info(
            "input: ack offset=2 while offset 1 in flight -> ack=%s checkpoint=%d "
            "(expect OK / frontier still 0)",
            r2, queue.get_checkpoint("g"),
        )
        assert r2 is AckResult.OK
        assert queue.get_job(leases[2].job.job_id).status is JobStatus.SUCCEEDED
        assert queue.get_checkpoint("g") == 0

        # the hole at offset 1 is filled: frontier auto-joins 2 and 3
        r1 = queue.ack("g", "c0", leases[1].job.job_id, leases[1].lease_token, 1, now=T0)
        logger.info(
            "input: ack offset=1 (hole filled) -> ack=%s checkpoint=%d "
            "(expect OK / frontier auto-advances to 3 without re-running 2,3)",
            r1, queue.get_checkpoint("g"),
        )
        assert r1 is AckResult.OK
        assert queue.get_checkpoint("g") == 3

    def test_dead_job_counts_as_completed_and_lets_frontier_pass(self, queue: JobQueue):
        _submit_n(queue, 3)
        leases = _lease_all(queue, "g", 3)

        # offset 2 succeeds out of order
        assert queue.ack("g", "c1", leases[2].job.job_id, leases[2].lease_token, 2, now=T0) is AckResult.OK
        # offset 1 fails non-retryably -> DEAD is a final conclusion, hole filled
        r_dead = queue.nack("c0", leases[1].job.job_id, leases[1].lease_token,
                            "poison", retryable=False, now=T0)
        logger.info(
            "input: nack offset=1 non-retryable while offset=2 already SUCCEEDED -> "
            "nack=%s checkpoint=%d (expect OK / frontier passes DEAD hole and joins offset 2)",
            r_dead, queue.get_checkpoint("g"),
        )
        assert r_dead is AckResult.OK
        assert queue.get_job(leases[1].job.job_id).status is JobStatus.DEAD
        assert queue.get_checkpoint("g") == 2

        # offset 3 then completes normally and extends the frontier
        assert queue.ack("g", "c2", leases[3].job.job_id, leases[3].lease_token, 3, now=T0) is AckResult.OK
        assert queue.get_checkpoint("g") == 3

    def test_frontier_advances_for_all_registered_groups(self, queue: JobQueue):
        _submit_n(queue, 2)
        g1 = _lease_all(queue, "g1", 2)
        # g2 gets no delivery (jobs are leased to g1) but is registered
        assert queue.lease("g2", "c9", now=T0) is None

        # g1 completes offset 2 out of order: the hole at 1 blocks everyone
        assert queue.ack("g1", "c1", g1[2].job.job_id, g1[2].lease_token, 2, now=T0) is AckResult.OK
        logger.info(
            "input: g1 acks offset=2 with hole at 1 -> checkpoint(g1)=%d checkpoint(g2)=%d "
            "(expect both 0)",
            queue.get_checkpoint("g1"), queue.get_checkpoint("g2"),
        )
        assert queue.get_checkpoint("g1") == 0
        assert queue.get_checkpoint("g2") == 0

        # g1 fills the hole: the frontier auto-extends for every group,
        # including g2 which never acked anything itself
        assert queue.ack("g1", "c0", g1[1].job.job_id, g1[1].lease_token, 1, now=T0) is AckResult.OK
        logger.info(
            "input: g1 acks offset=1 (hole filled) -> checkpoint(g1)=%d checkpoint(g2)=%d "
            "(expect both auto-advance to 2 over the already-finished offset 2)",
            queue.get_checkpoint("g1"), queue.get_checkpoint("g2"),
        )
        assert queue.get_checkpoint("g1") == 2
        assert queue.get_checkpoint("g2") == 2


# ----------------------------------------------------------------------
# duplicate / late confirmations
# ----------------------------------------------------------------------
class TestConfirmationSafety:
    def test_retried_ack_from_completing_lease_is_idempotent(self, queue: JobQueue):
        _submit_n(queue, 2)
        leases = _lease_all(queue, "g", 2)
        first = queue.ack("g", "c0", leases[1].job.job_id, leases[1].lease_token, 1,
                          result={"output": "r1"}, now=T0)
        assert first is AckResult.OK
        before = queue.get_checkpoint("g")

        # same consumer retries the same ack (e.g. response was lost)
        retry = queue.ack("g", "c0", leases[1].job.job_id, leases[1].lease_token, 1, now=T0)
        job = queue.get_job(leases[1].job.job_id)
        logger.info(
            "input: duplicate ack from completing lease -> ack=%s status=%s result=%s "
            "checkpoint %d -> %d (expect OK, state and frontier untouched)",
            retry, job.status, job.result, before, queue.get_checkpoint("g"),
        )
        assert retry is AckResult.OK
        assert job.status is JobStatus.SUCCEEDED
        assert job.result == {"output": "r1"}, "idempotent re-ack must not overwrite the result"
        assert queue.get_checkpoint("g") == before

    def test_foreign_confirmation_of_completed_job_is_rejected(self, queue: JobQueue):
        _submit_n(queue, 1)
        lease = queue.lease("g", "c1", now=T0)
        assert queue.ack("g", "c1", lease.job.job_id, lease.lease_token, 1, now=T0) is AckResult.OK

        foreign = queue.ack("g", "c2", lease.job.job_id, "forged-token", 1, now=T0)
        logger.info(
            "input: ack of SUCCEEDED job with foreign token -> %s (expect LEASE_MISMATCH, no state change)",
            foreign,
        )
        assert foreign is AckResult.LEASE_MISMATCH
        assert queue.get_job(lease.job.job_id).status is JobStatus.SUCCEEDED
        assert queue.get_checkpoint("g") == 1

    def test_late_ack_from_stale_lease_after_redelivery_is_rejected(self, queue: JobQueue):
        _submit_n(queue, 1)
        stale = queue.lease("g", "c1", now=T0)
        logger.info(
            "c1 leased job %s (token A, expires %s); c1 goes silent past the timeout",
            stale.job.job_id, stale.lease_expires_at,
        )
        assert queue.requeue_expired_leases(now=T0 + 5) == 1
        fresh = queue.lease("g", "c2", now=T0 + 5)
        assert fresh is not None and fresh.lease_token != stale.lease_token

        done = queue.ack("g", "c2", fresh.job.job_id, fresh.lease_token, 1,
                         result={"output": "by-c2"}, now=T0 + 6)
        assert done is AckResult.OK

        # the crashed consumer resurfaces and confirms with its stale token
        late = queue.ack("g", "c1", stale.job.job_id, stale.lease_token, 1, now=T0 + 7)
        job = queue.get_job(stale.job.job_id)
        logger.info(
            "input: late ack from stale lease after c2 completed -> ack=%s status=%s "
            "result=%s checkpoint=%d (expect LEASE_MISMATCH, c2's result preserved)",
            late, job.status, job.result, queue.get_checkpoint("g"),
        )
        assert late is AckResult.LEASE_MISMATCH
        assert job.status is JobStatus.SUCCEEDED
        assert job.result == {"output": "by-c2"}, "stale confirmation must not corrupt the final result"
        assert job.attempts == 2, "no extra delivery may be counted"
        assert queue.get_checkpoint("g") == 1

    def test_completed_job_is_never_redelivered(self, queue: JobQueue):
        _submit_n(queue, 1)
        lease = queue.lease("g", "c1", now=T0)
        assert queue.ack("g", "c1", lease.job.job_id, lease.lease_token, 1, now=T0) is AckResult.OK

        # even after the original lease would have expired, and after an
        # explicit recovery sweep, the finished job stays out of circulation
        requeued = queue.requeue_expired_leases(now=T0 + 100)
        again = queue.lease("g", "c2", now=T0 + 100)
        logger.info(
            "input: lease after completion + recovery sweep -> requeued=%d lease=%s "
            "(expect 0 / None: a confirmed job is never delivered twice)",
            requeued, again,
        )
        assert requeued == 0
        assert again is None

    def test_terminal_states_cannot_be_overturned(self, queue: JobQueue):
        _submit_n(queue, 2)
        leases = _lease_all(queue, "g", 2)
        assert queue.ack("g", "c0", leases[1].job.job_id, leases[1].lease_token, 1, now=T0) is AckResult.OK
        assert queue.nack("c1", leases[2].job.job_id, leases[2].lease_token,
                          "poison", retryable=False, now=T0) is AckResult.OK

        late_nack = queue.nack("c0", leases[1].job.job_id, leases[1].lease_token, "too late", now=T0)
        late_ack = queue.ack("g", "c1", leases[2].job.job_id, leases[2].lease_token, 2, now=T0)
        logger.info(
            "input: nack on SUCCEEDED job -> %s; ack on DEAD job -> %s "
            "(expect NOT_LEASED both: terminal results are final)",
            late_nack, late_ack,
        )
        assert late_nack is AckResult.NOT_LEASED
        assert late_ack is AckResult.NOT_LEASED
        assert queue.get_job(leases[1].job.job_id).status is JobStatus.SUCCEEDED
        assert queue.get_job(leases[2].job.job_id).status is JobStatus.DEAD
        assert queue.get_checkpoint("g") == 2


# ----------------------------------------------------------------------
# concurrent stress: out-of-order acks from many consumers
# ----------------------------------------------------------------------
class TestConcurrentOutOfOrder:
    def test_concurrent_consumers_complete_out_of_order(self, queue: JobQueue):
        n_jobs, n_consumers = 60, 8
        _submit_n(queue, n_jobs)
        executions: list[str] = []
        errors: list[str] = []
        lock = threading.Lock()

        def worker(consumer: str):
            while True:
                lease = queue.lease("g", consumer, now=T0)
                if lease is None:
                    return
                with lock:
                    executions.append(lease.job.job_id)
                # randomised "processing time" shuffles the ack order
                time_jitter = random.random() * 0.002
                threading.Event().wait(time_jitter)
                rc = queue.ack("g", consumer, lease.job.job_id, lease.lease_token,
                               lease.job.offset, result={"by": consumer}, now=T0)
                if rc is not AckResult.OK:
                    with lock:
                        errors.append(f"{consumer}: ack {lease.job.offset} -> {rc}")

        threads = [threading.Thread(target=worker, args=(f"c{n}",)) for n in range(n_consumers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        statuses = [queue.get_job(jid).status for jid in executions]
        logger.info(
            "input: %d jobs / %d consumers, randomised ack order -> executions=%d unique=%d "
            "ack_errors=%s all_succeeded=%s checkpoint=%d "
            "(expect 60/60 unique, no errors, all SUCCEEDED, frontier=60)",
            n_jobs, n_consumers, len(executions), len(set(executions)),
            errors, all(s is JobStatus.SUCCEEDED for s in statuses), queue.get_checkpoint("g"),
        )
        assert errors == [], f"no ack may fail under out-of-order completion: {errors}"
        assert len(executions) == n_jobs
        assert len(set(executions)) == n_jobs, "no job may be executed twice"
        assert all(s is JobStatus.SUCCEEDED for s in statuses)
        assert queue.get_checkpoint("g") == n_jobs, "frontier must reach the end once all holes are filled"

    def test_concurrent_mixed_outcomes_still_converge(self, queue: JobQueue):
        n_jobs, n_consumers = 40, 6
        _submit_n(queue, n_jobs)
        dead_ids: set[str] = set()
        lock = threading.Lock()

        def worker(consumer: str):
            while True:
                lease = queue.lease("g", consumer, now=T0)
                if lease is None:
                    return
                threading.Event().wait(random.random() * 0.002)
                if lease.job.offset % 7 == 0:  # deterministic poison subset
                    rc = queue.nack(consumer, lease.job.job_id, lease.lease_token,
                                    "poison", retryable=False, now=T0)
                    with lock:
                        dead_ids.add(lease.job.job_id)
                else:
                    rc = queue.ack("g", consumer, lease.job.job_id, lease.lease_token,
                                   lease.job.offset, now=T0)
                assert rc is AckResult.OK, f"terminal transition must be accepted, got {rc}"

        threads = [threading.Thread(target=worker, args=(f"c{n}",)) for n in range(n_consumers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        jobs = queue.list_jobs()
        terminal = sum(1 for j in jobs if j.status in (JobStatus.SUCCEEDED, JobStatus.DEAD))
        logger.info(
            "input: %d jobs, offsets %%7==0 go DEAD, rest SUCCEEDED -> terminal=%d dead=%d "
            "checkpoint=%d (expect all terminal, frontier=%d: DEAD counts as completed)",
            n_jobs, terminal, len(dead_ids), queue.get_checkpoint("g"), n_jobs,
        )
        assert terminal == n_jobs
        assert len(dead_ids) == len([j for j in jobs if j.status is JobStatus.DEAD])
        assert queue.get_checkpoint("g") == n_jobs
