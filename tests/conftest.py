import logging

import pytest

from jobqueue.queue import JobQueue
from jobqueue.storage import JobStorage

logger = logging.getLogger("jobqueue.test")


@pytest.fixture()
def clock():
    """可控时钟，避免测试依赖真实时间。"""
    now = [10_000.0]

    def _now() -> float:
        return now[0]

    def _advance(value: float) -> None:
        now[0] = value

    _now.advance = _advance  # type: ignore[attr-defined]
    return _now


@pytest.fixture()
def queue(clock):
    return JobQueue(
        JobStorage(":memory:"),
        clock=clock,
        dedup_window_seconds=3600.0,
        base_backoff_seconds=1.0,
        max_backoff_seconds=60.0,
        lease_seconds=30.0,
    )
