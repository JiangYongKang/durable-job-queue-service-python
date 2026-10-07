"""Behavioural tests for the durable job queue.

Each test logs its key inputs and the decision basis (expected vs actual
status / reason) so failures are explainable from the log alone.
"""

from __future__ import annotations

import logging
import threading
import time

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
# out-of-order completion / contiguous checkpoint boundary
# ----------------------------------------------------------------------
class TestOutOfOrderCompletion:
    def _submit_n(self, queue: JobQueue, n: int, prefix: str = "oo") -> None:
        for i in range(1, n + 1):
            queue.submit(f"{prefix}-{i}", {"i": i}, now=T0)

    def test_later_offset_succeeds_immediately_even_with_hole_ahead(self, queue: JobQueue):
        # lease offsets 1..3 in order, then complete offset 2 while 1 is still in flight
        self._submit_n(queue, 3)
        l1 = queue.lease("g", "c1", now=T0)
        l2 = queue.lease("g", "c2", now=T0)
        l3 = queue.lease("g", "c3", now=T0)
        assert (l1.job.offset, l2.job.offset, l3.job.offset) == (1, 2, 3)

        result = queue.ack(
            "g", "c2", l2.job.job_id, l2.lease_token,
            result={"output": "done-2"}, now=T0 + 1,
        )
        logger.info(
            "input: ack offset=2 while offset=1 still LEASED -> %s; status=%s result=%r",
            result, queue.get_job(l2.job.job_id).status, queue.get_job(l2.job.job_id).result,
        )
        assert result is AckResult.OK, "a job must settle the moment its processing finishes"
        settled = queue.get_job(l2.job.job_id)
        assert settled.status is JobStatus.SUCCEEDED
        assert settled.terminal_reason is TerminalReason.SUCCEEDED
        assert settled.result == {"output": "done-2"}, "success result must be queryable immediately"
        cp = queue.get_checkpoint("g")
        logger.info("checkpoint after offset=2 settled (offset=1 open hole) = %s (expect 0)", cp)
        assert cp == 0, "checkpoint must stop before the unfinished hole at offset 1"

        # completing offset 3 as well still cannot cross the hole
        result3 = queue.ack("g", "c3", l3.job.job_id, l3.lease_token, now=T0 + 1)
        logger.info(
            "input: ack offset=3 with hole at 1 -> %s checkpoint=%s (expect OK / 0)",
            result3, queue.get_checkpoint("g"),
        )
        assert result3 is AckResult.OK
        assert queue.get_checkpoint("g") == 0

    def test_filling_the_hole_joins_already_completed_batch_without_rerun(self, queue: JobQueue):
        self._submit_n(queue, 4)
        leases = [queue.lease("g", f"c{i}", now=T0) for i in range(1, 5)]
        # complete offsets 2,3,4 out of order first; offset 1 stays open
        for lease in leases[1:]:
            assert queue.ack(
                "g", lease.job.lease_owner, lease.job.job_id, lease.lease_token, now=T0 + 1
            ) is AckResult.OK
        assert queue.get_checkpoint("g") == 0
        completed_before_hole = {
            j.offset: j.status for j in queue.list_jobs() if j.status is JobStatus.SUCCEEDED
        }
        logger.info(
            "settled offsets while hole=1 open: %s, checkpoint=%s",
            sorted(completed_before_hole), queue.get_checkpoint("g"),
        )

        # now the straggler completes offset 1: the boundary must jump to 4
        result = queue.ack("g", "c1", leases[0].job.job_id, leases[0].lease_token, now=T0 + 2)
        cp = queue.get_checkpoint("g")
        logger.info(
            "input: hole offset=1 settled -> ack=%s checkpoint=%s (expect OK / 4); "
            "already-settled jobs must not re-run",
            result, cp,
        )
        assert result is AckResult.OK
        assert cp == 4, "checkpoint must auto-advance over the pre-settled batch up to 4"
        for j in queue.list_jobs():
            assert j.status is JobStatus.SUCCEEDED
            assert j.attempts == 1, "joining the boundary must never redrive completed jobs"

    def test_dead_job_at_hole_also_joins_boundary(self, queue: JobQueue):
        self._submit_n(queue, 3)
        l1 = queue.lease("g", "c1", now=T0)
        l2 = queue.lease("g", "c2", now=T0)
        l3 = queue.lease("g", "c3", now=T0)
        # offset 2 dies (non-retryable) and offset 3 succeeds while 1 is still in flight
        assert queue.nack(
            "c2", l2.job.job_id, l2.lease_token, "poison",
            retryable=False, consumer_group="g", now=T0 + 1,
        ) is AckResult.OK
        assert queue.ack("g", "c3", l3.job.job_id, l3.lease_token, now=T0 + 1) is AckResult.OK
        logger.info(
            "offset2=DEAD, offset3=SUCCEEDED, hole offset1 open -> checkpoint=%s (expect 0)",
            queue.get_checkpoint("g"),
        )
        assert queue.get_checkpoint("g") == 0
        dead = queue.get_job(l2.job.job_id)
        assert dead.status is JobStatus.DEAD
        assert dead.terminal_reason is TerminalReason.NON_RETRYABLE

        # settling offset 1 advances over the DEAD job to 3
        assert queue.ack("g", "c1", l1.job.job_id, l1.lease_token, now=T0 + 2) is AckResult.OK
        logger.info("hole settled -> checkpoint=%s (expect 3: terminal includes DEAD)", queue.get_checkpoint("g"))
        assert queue.get_checkpoint("g") == 3
        assert queue.lease("g", "c9", now=T0 + 2) is None, "dead job must never be redelivered"

    def test_retryable_failure_keeps_hole_open_until_settled(self, queue: JobQueue):
        self._submit_n(queue, 2)
        l1 = queue.lease("g", "c1", now=T0)
        l2 = queue.lease("g", "c2", now=T0)
        # offset 1 fails retryably (backoff base=10s), offset 2 already done
        assert queue.nack("c1", l1.job.job_id, l1.lease_token, "transient", now=T0) is AckResult.OK
        assert queue.ack("g", "c2", l2.job.job_id, l2.lease_token, now=T0) is AckResult.OK
        logger.info(
            "offset1=FAILED_RETRYABLE(visible_at=%s), offset2=SUCCEEDED -> checkpoint=%s (expect 0)",
            queue.get_job(l1.job.job_id).visible_at, queue.get_checkpoint("g"),
        )
        assert queue.get_checkpoint("g") == 0

        # retry offset 1 once the backoff elapses; then the boundary joins to 2
        again = queue.lease("g", "c3", now=T0 + 10)
        assert again is not None and again.job.offset == 1
        assert queue.ack("g", "c3", again.job.job_id, again.lease_token, now=T0 + 10) is AckResult.OK
        logger.info(
            "offset1 retry succeeded (attempts=%d) -> checkpoint=%s (expect 2)",
            queue.get_job(l1.job.job_id).attempts, queue.get_checkpoint("g"),
        )
        assert queue.get_checkpoint("g") == 2

    def test_late_ack_after_redelivery_is_rejected(self, queue: JobQueue):
        queue.submit("late-1", {}, now=T0)
        first = queue.lease("g", "c1", now=T0)
        old_token = first.lease_token
        logger.info(
            "c1 holds lease token=%s expires_at=%s then stalls past timeout",
            old_token, first.lease_expires_at,
        )
        assert queue.requeue_expired_leases(now=T0 + 5) == 1
        second = queue.lease("g", "c2", now=T0 + 5)
        assert second is not None and second.job.job_id == first.job.job_id
        assert second.lease_token != old_token
        logger.info("redelivered to c2 with new token=%s (attempt=%d)", second.lease_token, second.job.attempts)

        # new consumer settles it
        assert queue.ack("g", "c2", second.job.job_id, second.lease_token, now=T0 + 6) is AckResult.OK
        # the zombie consumer finally comes back and confirms with its stale token
        late = queue.ack("g", "c1", first.job.job_id, old_token, now=T0 + 7)
        logger.info(
            "input: late ack from old c1 token=%s after redelivery+settle -> %s (expect ALREADY_COMPLETED)",
            old_token, late,
        )
        assert late is AckResult.ALREADY_COMPLETED
        job = queue.get_job(first.job.job_id)
        assert job.status is JobStatus.SUCCEEDED
        assert job.attempts == 2, "the rejected late ack must not count another attempt"
        assert queue.lease("g", "c4", now=T0 + 8) is None, "a completed job can never be leased again"

    def test_duplicate_ack_on_settled_job_is_rejected_and_checkpoint_steady(self, queue: JobQueue):
        self._submit_n(queue, 2)
        l1 = queue.lease("g", "c1", now=T0)
        l2 = queue.lease("g", "c2", now=T0)
        assert queue.ack("g", "c1", l1.job.job_id, l1.lease_token, now=T0) is AckResult.OK
        assert queue.ack("g", "c2", l2.job.job_id, l2.lease_token, now=T0) is AckResult.OK
        assert queue.get_checkpoint("g") == 2

        # replay the exact same successful confirmation twice
        for replay in (1, 2):
            dup = queue.ack("g", "c1", l1.job.job_id, l1.lease_token, now=T0 + replay)
            logger.info(
                "input: duplicate ack replay #%d for already-succeeded offset=1 -> %s (expect ALREADY_COMPLETED)",
                replay, dup,
            )
            assert dup is AckResult.ALREADY_COMPLETED
        assert queue.get_checkpoint("g") == 2, "replayed acks must not move the checkpoint"
        assert queue.get_job(l1.job.job_id).attempts == 1

    def test_legacy_result_offset_still_validated(self, queue: JobQueue):
        self._submit_n(queue, 3)
        l1 = queue.lease("g", "c1", now=T0)
        l2 = queue.lease("g", "c2", now=T0)
        # wrong offset for the acked job: rejected and job stays LEASED
        bad = queue.ack("g", "c1", l1.job.job_id, l1.lease_token, result_offset=2, now=T0)
        logger.info(
            "input: legacy ack job.offset=1 with result_offset=2 -> %s (expect CHECKPOINT_OUT_OF_ORDER)",
            bad,
        )
        assert bad is AckResult.CHECKPOINT_OUT_OF_ORDER
        assert queue.get_job(l1.job.job_id).status is JobStatus.LEASED
        assert queue.get_checkpoint("g") == 0
        # correct legacy ack on offset 1
        assert queue.ack("g", "c1", l1.job.job_id, l1.lease_token, result_offset=1, now=T0) is AckResult.OK
        assert queue.get_checkpoint("g") == 1
        # re-ack offset 1 with a legacy offset <= checkpoint: regression rejected
        dup = queue.ack("g", "c2", l2.job.job_id, l2.lease_token, result_offset=1, now=T0)
        logger.info(
            "input: legacy ack offset=2 job with result_offset=1 <= checkpoint=1 -> %s (expect CHECKPOINT_REGRESSION)",
            dup,
        )
        assert dup is AckResult.CHECKPOINT_REGRESSION
        assert queue.get_checkpoint("g") == 1


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

    @pytest.mark.parametrize("round_no", range(20))
    def test_concurrent_out_of_order_completion_is_stable(self, queue: JobQueue, round_no: int):
        """Many consumers settle jobs in arbitrary order; the boundary must
        always end at N, every job must be SUCCEEDED with exactly one attempt.

        Repeated 20 times so a race has no place to hide.
        """
        n_jobs = 30
        n_consumers = 8
        for i in range(1, n_jobs + 1):
            queue.submit(f"par-{round_no}-{i}", {"i": i}, now=T0)

        settle_order: list[int] = []
        order_lock = threading.Lock()
        barrier = threading.Barrier(n_consumers)

        def worker(consumer: str) -> None:
            barrier.wait()  # maximise overlap between consumers
            while True:
                lease = queue.lease("g", consumer, now=T0)
                if lease is None:
                    return
                offset = lease.job.offset
                # deliberately process *out of offset order*: higher offsets
                # pause less, so later jobs tend to settle before earlier ones
                time.sleep((n_jobs - offset) * 0.0005)
                result = queue.ack(
                    "g", consumer, lease.job.job_id, lease.lease_token,
                    result={"by": consumer, "offset": offset}, now=T0 + 1,
                )
                assert result is AckResult.OK, f"settle offset={offset} -> {result}"
                with order_lock:
                    settle_order.append(offset)
                # checkpoint must never pass an unsettled offset nor regress
                cp = queue.get_checkpoint("g")
                assert 0 <= cp <= n_jobs
                for job in queue.list_jobs():
                    if job.offset > cp:
                        break
                    assert job.status is JobStatus.SUCCEEDED, (
                        f"checkpoint={cp} but offset={job.offset} is {job.status}"
                    )

        threads = [threading.Thread(target=worker, args=(f"c{n}",)) for n in range(n_consumers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        jobs = queue.list_jobs()
        statuses = {j.offset: j.status for j in jobs}
        attempts = {j.offset: j.attempts for j in jobs}
        cp = queue.get_checkpoint("g")
        logger.info(
            "round=%02d input: %d jobs / %d consumers; settle order head=%s -> "
            "checkpoint=%s all_succeeded=%s attempts_set=%s (expect %s / True / {1})",
            round_no, n_jobs, n_consumers, settle_order[:8], cp,
            all(s is JobStatus.SUCCEEDED for s in statuses.values()),
            sorted(set(attempts.values())), n_jobs,
        )
        assert len(settle_order) == n_jobs
        assert len(set(settle_order)) == n_jobs, "each job must be settled exactly once"
        assert all(a == 1 for a in attempts.values()), "no job may be executed twice"
        assert all(s is JobStatus.SUCCEEDED for s in statuses.values())
        assert cp == n_jobs, "after every job settles, the contiguous boundary reaches N"

    @pytest.mark.parametrize("round_no", range(20))
    def test_stale_ack_racing_redelivery_never_overwrites_result(self, queue: JobQueue, round_no: int):
        """The original (timed-out) consumer and the new consumer confirm at
        the same time.  Exactly one confirmation may win; the settled result
        and checkpoint stay intact.  Repeated 20 times.
        """
        queue.submit(f"zombie-{round_no}", {"round": round_no}, now=T0)
        first = queue.lease("g", "c-old", now=T0)
        old_token = first.lease_token
        assert queue.requeue_expired_leases(now=T0 + 5) == 1
        second = queue.lease("g", "c-new", now=T0 + 5)
        assert second is not None and second.lease_token != old_token

        outcomes: list[AckResult] = []
        outcomes_lock = threading.Lock()
        start = threading.Barrier(2)

        def stale_confirm() -> None:
            start.wait()
            r = queue.ack(
                "g", "c-old", first.job.job_id, old_token,
                result={"by": "stale"}, now=T0 + 6,
            )
            with outcomes_lock:
                outcomes.append(r)

        def fresh_confirm() -> None:
            start.wait()
            r = queue.ack(
                "g", "c-new", second.job.job_id, second.lease_token,
                result={"by": "fresh"}, now=T0 + 6,
            )
            with outcomes_lock:
                outcomes.append(r)

        t1 = threading.Thread(target=stale_confirm)
        t2 = threading.Thread(target=fresh_confirm)
        t1.start(); t2.start()
        t1.join(); t2.join()

        job = queue.get_job(first.job.job_id)
        logger.info(
            "round=%02d input: stale+fresh ack racing after redelivery -> outcomes=%s "
            "status=%s result=%r checkpoint=%s (expect one OK, one rejection; SUCCEEDED/fresh/1)",
            round_no, sorted(o.value for o in outcomes), job.status, job.result,
            queue.get_checkpoint("g"),
        )
        assert outcomes.count(AckResult.OK) == 1
        loser = [o for o in outcomes if o is not AckResult.OK]
        assert loser == [AckResult.LEASE_MISMATCH] or loser == [AckResult.ALREADY_COMPLETED], (
            f"the stale confirmation must be rejected, got {loser}"
        )
        assert job.status is JobStatus.SUCCEEDED
        assert job.result == {"by": "fresh"}, "the stale confirmation must never overwrite the result"
        assert job.attempts == 2
        assert queue.get_checkpoint("g") == 1

    @pytest.mark.parametrize("round_no", range(10))
    def test_concurrent_mixed_success_and_dead_joins_boundary(self, queue: JobQueue, round_no: int):
        """Half the jobs succeed and half die non-retryably under concurrent
        consumers; both terminal kinds count towards the boundary.
        """
        n_jobs = 16
        for i in range(1, n_jobs + 1):
            queue.submit(f"mix-{round_no}-{i}", {"i": i}, now=T0)
        n_consumers = 6
        settle_kind: dict[int, str] = {}
        kind_lock = threading.Lock()
        start = threading.Barrier(n_consumers)

        def worker(consumer: str) -> None:
            start.wait()
            while True:
                lease = queue.lease("g", consumer, now=T0)
                if lease is None:
                    return
                offset = lease.job.offset
                time.sleep((n_jobs - offset) * 0.0003)  # induce out-of-order settlement
                if offset % 2 == 0:
                    r = queue.nack(
                        consumer, lease.job.job_id, lease.lease_token, "poison",
                        retryable=False, consumer_group="g", now=T0 + 1,
                    )
                    kind = "dead"
                else:
                    r = queue.ack(
                        "g", consumer, lease.job.job_id, lease.lease_token, now=T0 + 1,
                    )
                    kind = "succeeded"
                assert r is AckResult.OK, f"offset={offset} settle -> {r}"
                with kind_lock:
                    settle_kind[offset] = kind

        threads = [threading.Thread(target=worker, args=(f"c{n}",)) for n in range(n_consumers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        jobs = {j.offset: j for j in queue.list_jobs()}
        cp = queue.get_checkpoint("g")
        dead_reasons = {o: j.terminal_reason for o, j in jobs.items() if j.status is JobStatus.DEAD}
        logger.info(
            "round=%02d input: %d jobs (even=non-retryable DEAD, odd=SUCCEEDED) -> "
            "settled=%d checkpoint=%s dead_reasons=%s (expect %d / %s / all NON_RETRYABLE)",
            round_no, n_jobs, len(settle_kind), cp,
            {o: r.value for o, r in sorted(dead_reasons.items())}, n_jobs, n_jobs,
        )
        assert len(settle_kind) == n_jobs
        assert cp == n_jobs, "DEAD jobs are terminal too and must not block the boundary"
        for offset, job in jobs.items():
            if offset % 2 == 0:
                assert job.status is JobStatus.DEAD
                assert job.terminal_reason is TerminalReason.NON_RETRYABLE
            else:
                assert job.status is JobStatus.SUCCEEDED
        assert queue.lease("g", "c-final", now=T0 + 2) is None, "nothing terminal is delivered again"


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
