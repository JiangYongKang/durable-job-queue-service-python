"""消费位点：仅校验通过后可推进，回退/越界/乱序明确拒绝。"""
import logging

import pytest

from jobqueue.offset import OffsetError, OffsetManager
from jobqueue.storage import JobStorage

logger = logging.getLogger("jobqueue.test")


@pytest.fixture()
def offsets():
    return OffsetManager(JobStorage(":memory:"), "group-a")


def test_unverified_commit_rejected(offsets):
    with pytest.raises(OffsetError, match="未校验"):
        offsets.commit(1, verified=False)
    logger.info("未校验提交 seq=1 被拒，当前位点=%d", offsets.current)
    assert offsets.current == 0


def test_offset_regression_rejected(offsets):
    offsets.commit(1, verified=True)
    offsets.commit(2, verified=True)
    for bad in (2, 1):
        with pytest.raises(OffsetError, match="回退"):
            offsets.commit(bad, verified=True)
        logger.info("回退提交 seq=%d 被拒，当前位点=%d", bad, offsets.current)
    assert offsets.current == 2


def test_out_of_order_and_gap_rejected(offsets):
    offsets.commit(1, verified=True)
    with pytest.raises(OffsetError, match="乱序"):
        offsets.commit(3, verified=True)  # 跳过 seq=2
    logger.info("越界提交 seq=3 被拒（期望 2），当前位点=%d", offsets.current)
    assert offsets.current == 1
    assert offsets.commit(2, verified=True) == 2, "补齐 seq=2 后应正常推进"


def test_offset_survives_restart(tmp_path):
    db = str(tmp_path / "q.db")
    om1 = OffsetManager(JobStorage(db), "g")
    om1.commit(1, verified=True)
    om1.commit(2, verified=True)
    om2 = OffsetManager(JobStorage(db), "g")  # 模拟进程重启后重连同一库
    logger.info("重启后恢复位点=%d", om2.current)
    assert om2.current == 2
    with pytest.raises(OffsetError):
        om2.commit(2, verified=True)
