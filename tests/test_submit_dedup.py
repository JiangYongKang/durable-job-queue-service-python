"""重复提交与非法提交：结果与原因必须可区分，不得静默覆盖。"""
import logging

from jobqueue.models import SubmitResult

logger = logging.getLogger("jobqueue.test")


def test_duplicate_submit_is_deduplicated(queue):
    r1 = queue.submit("order-1", '{"amount": 100}')
    r2 = queue.submit("order-1", '{"amount": 100}')
    logger.info("首次提交: %s(%s) 重复提交: %s(%s)", r1.result, r1.job_id, r2.result, r2.job_id)
    assert r1.result == SubmitResult.CREATED
    assert r2.result == SubmitResult.DUPLICATE
    assert r2.job_id == r1.job_id, "去重命中必须返回原任务 ID"
    assert "幂等去重" in r2.reason
    assert queue.lease("c1", limit=10).__len__() == 1, "重复提交不得产生第二条任务"


def test_same_key_different_payload_is_conflict(queue):
    r1 = queue.submit("order-2", "v1")
    r2 = queue.submit("order-2", "v2")
    logger.info("已有参数 hash=%s 新参数冲突判定: %s", r1.job_id, r2.reason)
    assert r2.result == SubmitResult.CONFLICT
    assert r2.job_id == r1.job_id
    assert "拒绝覆盖" in r2.reason
    assert queue.get(r1.job_id).payload == "v1", "原任务不得被静默覆盖"


def test_invalid_submit_is_rejected(queue):
    for key, payload, attempts in [("", "p", 3), ("  ", "p", 3), ("k", "", 3), ("k", "p", 0)]:
        r = queue.submit(key, payload, max_attempts=attempts)
        logger.info("非法输入 key=%r payload=%r max_attempts=%d -> %s: %s",
                    key, payload, attempts, r.result, r.reason)
        assert r.result == SubmitResult.REJECTED
        assert r.job_id is None and r.reason


def test_dedup_window_expiry_allows_resubmit(queue, clock):
    r1 = queue.submit("order-3", "p")
    clock.advance(10_000.0 + 3601.0)  # 越过 3600s 去重窗口
    r2 = queue.submit("order-3", "p")
    logger.info("窗口外再提交: %s 新任务 %s", r2.result, r2.job_id)
    assert r2.result == SubmitResult.CREATED
    assert r2.job_id != r1.job_id
