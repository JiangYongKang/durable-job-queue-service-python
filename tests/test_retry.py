"""失败重试：有界退避、最大尝试次数、终态可区分、计数不重复累加。"""
import logging

from jobqueue.models import JobStatus

logger = logging.getLogger("jobqueue.test")


def _run_one_attempt(queue, clock, consumer, error="boom"):
    jobs = queue.lease(consumer)
    assert len(jobs) == 1
    queue.fail(jobs[0].job_id, consumer, retryable=True, error=error)
    return jobs[0].job_id


def test_retry_exhaustion_enters_dead_state(queue, clock):
    r = queue.submit("job-retry", "p", max_attempts=3)
    t0 = clock()
    for i in range(1, 4):
        job_id = _run_one_attempt(queue, clock, f"c{i}")
        job = queue.get(job_id)
        logger.info("第 %d 次失败: attempts=%d status=%s next_attempt_at-now=%.1f",
                    i, job.attempts, job.status, job.next_attempt_at - clock())
        if i < 3:
            assert job.status == JobStatus.PENDING
            assert job.attempts == i, "每次失败只累加一次 attempts"
            # 有界退避：base=1s，第 i 次退避 2^(i-1) 秒
            assert job.next_attempt_at - t0 - sum(2 ** (k - 1) for k in range(1, i)) == 2 ** (i - 1)
            clock.advance(clock() + 2 ** (i - 1))  # 等退避到期
        else:
            assert job.status == JobStatus.DEAD, "重试耗尽必须进入终态 DEAD"
            assert "次数耗尽" in job.last_error
    dead = queue.get(r.job_id)
    logger.info("终态: status=%s last_error=%s", dead.status, dead.last_error)
    assert dead.attempts == 3
    assert queue.lease("c9") == [], "DEAD 任务不得再被投递"


def test_backoff_is_bounded(queue, clock):
    r = queue.submit("job-cap", "p", max_attempts=10)
    for i in range(1, 10):
        _run_one_attempt(queue, clock, f"c{i}")
        job = queue.get(r.job_id)
        delay = job.next_attempt_at - clock()
        logger.info("第 %d 次失败退避 %.1fs（上限 60s）", i, delay)
        assert delay <= 60.0, "退避必须有上界"
        clock.advance(clock() + delay)


def test_non_retryable_failure_goes_dead_immediately(queue):
    r = queue.submit("job-fatal", "p", max_attempts=5)
    queue.lease("c1")
    queue.fail(r.job_id, "c1", retryable=False, error="bad payload schema")
    job = queue.get(r.job_id)
    logger.info("不可重试失败: status=%s attempts=%d last_error=%s",
                job.status, job.attempts, job.last_error)
    assert job.status == JobStatus.DEAD
    assert job.attempts == 1, "不可重试失败不得浪费剩余次数继续重试"
    assert "不可重试" in job.last_error


def test_ack_is_terminal_and_idempotent_guard(queue):
    r = queue.submit("job-ack", "p")
    queue.lease("c1")
    queue.ack(r.job_id, "c1")
    job = queue.get(r.job_id)
    logger.info("确认后状态: %s attempts=%d", job.status, job.attempts)
    assert job.status == JobStatus.SUCCEEDED
    assert queue.lease("c2") == [], "SUCCEEDED 任务不得再投递"
