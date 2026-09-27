"""消费位点管理：仅允许结果校验通过后的单调推进。"""
from __future__ import annotations

import threading

from jobqueue.storage import JobStorage


class OffsetError(Exception):
    """位点提交非法（未校验 / 回退 / 越界 / 乱序）。"""


class OffsetManager:
    """约束：
    - commit 必须 verified=True（结果校验通过），否则拒绝；
    - 位点只能严格单调推进到 current + 1：小于等于 current 为回退/重复提交，
      大于 current + 1 为乱序/越界跳号，均明确拒绝并抛 OffsetError；
    - 位点持久化在存储层，进程重启后从上次提交处恢复。
    """

    def __init__(self, storage: JobStorage, consumer_group: str) -> None:
        if not consumer_group:
            raise ValueError("consumer_group 不能为空")
        self._storage = storage
        self._group = consumer_group
        self._lock = threading.Lock()

    @property
    def current(self) -> int:
        return self._storage.get_offset(self._group)

    def commit(self, seq: int, *, verified: bool) -> int:
        if not verified:
            raise OffsetError(f"结果未校验通过，拒绝推进位点到 {seq}")
        with self._lock:
            current = self._storage.get_offset(self._group)
            if seq <= current:
                raise OffsetError(
                    f"位点回退/重复提交：提交 {seq} <= 当前 {current}，已拒绝"
                )
            if seq > current + 1:
                raise OffsetError(
                    f"位点乱序/越界：提交 {seq}，期望 {current + 1}，已拒绝"
                )
            self._storage.set_offset(self._group, seq)
            return seq
