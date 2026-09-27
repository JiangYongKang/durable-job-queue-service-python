"""处理中中断后的重新投递：租约过期回收，且同一任务不得被并发执行两次。"""
import logging
import threading

from jobqueue.models import JobStatus
from jobqueue.queue import QueueError

logger = logging.getLogger("jobqueue.test")


def test_crash_before_ack_triggers_redelivery(queue, clock):
    r = queue.submit("job-crash", "p")
    leased = queue.lease("consumer-A")
    assert leased[0].status == JobStatus.LEASED
    logger.info("consumer-A 拿到租约后崩溃（未 ack/fail），租约 30s 后过期")
    clock.advance(clock() + 31.0)  # 越过租约有效期
    assert queue.reap_expired_leases() == 1
    job = queue.get(r.job_id)
    logger.info("回收后: status=%s attempts=%d（中断不计失败次数）", job.status, job.attempts)
    assert job.status == JobStatus.PENDING
    assert job.attempts == 0, "未确认的中断不得累加 attempts"
    redelivered = queue.lease("consumer-B")
    assert [j.job_id for j in redelivered] == [r.job_id], "中断任务必须被重新投递"
    queue.ack(r.job_id, "consumer-B")
    assert queue.get(r.job_id).status == JobStatus.SUCCEEDED


def test_no_concurrent_double_execution(queue):
    queue.submit("job-race", "p")
    first = queue.lease("c1", limit=1)
    second = queue.lease("c2", limit=1)
    logger.info("c1 租到 %s，c2 同期租到 %s", [j.job_id for j in first],
                [j.job_id for j in second])
    assert len(first) == 1 and second == [], "租约有效期内不得投递给第二个消费者"


def test_lease_race_under_threads(queue):
    """多线程并发抢同一任务：只有一个消费者能拿到。"""
    queue.submit("job-thread-race", "p")
    winners: list[str] = []

    def grab(cid: str) -> None:
        if queue.lease(cid, limit=1):
            winners.append(cid)

    threads = [threading.Thread(target=grab, args=(f"c{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    logger.info("8 个并发消费者中拿到任务的: %s", winners)
    assert len(winners) == 1, "同一任务不得被并发执行两次"


def test_stale_owner_cannot_ack_after_requeue(queue, clock):
    """租约过期被回收并投给新消费者后，旧持有者的 ack 必须被拒绝。"""
    r = queue.submit("job-stale", "p")
    queue.lease("old")
    clock.advance(clock() + 31.0)
    queue.reap_expired_leases()
    queue.lease("new")
    try:
        queue.ack(r.job_id, "old")
        raise AssertionError("旧持有者 ack 应被拒绝")
    except QueueError as e:
        logger.info("旧持有者 ack 被拒: %s", e)
    queue.ack(r.job_id, "new")
    assert queue.get(r.job_id).status == JobStatus.SUCCEEDED


def test_job_state_survives_process_restart(queue, clock, tmp_path):
    """进程重启（换 Storage 实例重连同一文件库）后任务状态可恢复。"""
    from jobqueue.queue import JobQueue
    from jobqueue.storage import JobStorage

    db = str(tmp_path / "restart.db")
    q1 = JobQueue(JobStorage(db), clock=clock, lease_seconds=30.0)
    r = q1.submit("job-restart", "p")
    q1.lease("c1")
    q1.fail(r.job_id, "c1", retryable=True, error="boom")
    logger.info("重启前: status=%s attempts=%d", q1.get(r.job_id).status, q1.get(r.job_id).attempts)

    q2 = JobQueue(JobStorage(db), clock=clock, lease_seconds=30.0)  # 模拟重启
    job = q2.get(r.job_id)
    logger.info("重启后: status=%s attempts=%d last_error=%s", job.status, job.attempts, job.last_error)
    assert job.status == JobStatus.PENDING and job.attempts == 1
    clock.advance(clock() + 2.0)  # 等退避到期
    assert q2.lease("c2")[0].job_id == r.job_id, "重启后任务应可继续被消费"
    # 重启后 seq 继续递增，不复用
    r2 = q2.submit("job-after-restart", "p")
    assert q2.get(r2.job_id).seq == job.seq + 1
