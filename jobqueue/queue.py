"""任务队列：幂等提交、租约投递、有界退避重试与终态管理。"""
from __future__ import annotations

import hashlib
import threading
import time
import uuid

from jobqueue.models import Job, JobStatus, SubmitOutcome, SubmitResult
from jobqueue.storage import JobStorage


class QueueError(Exception):
    """队列操作非法（如 ack 非本人租约）。"""


class JobQueue:
    """语义约定：
    - 提交：dedup_key 在去重窗口内唯一；同 key 同参数 -> DUPLICATE；同 key 不同参数 -> CONFLICT；
      非法输入（空 key / 空 payload / max_attempts < 1）-> REJECTED。绝不静默覆盖。
    - 投递：lease 将 PENDING 且到期的任务置为 LEASED，绑定 consumer 与租约超时；
      同一任务在租约有效期内不会被其他消费者拿到（不并发双执行）。
    - 确认：ack 仅租约持有者可调，置 SUCCEEDED（终态）。
    - 失败：retryable 且 attempts < max_attempts -> 指数退避后回 PENDING；
      否则置 DEAD（终态，可查询，last_error 记录原因）。attempts 只在 fail 时 +1，不重复累加。
    - 中断恢复：租约过期（消费者崩溃/超时）由 reap_expired_leases 回收为 PENDING 重新投递。
    """

    def __init__(
        self,
        storage: JobStorage,
        *,
        dedup_window_seconds: float = 3600.0,
        base_backoff_seconds: float = 0.1,
        max_backoff_seconds: float = 10.0,
        lease_seconds: float = 30.0,
        clock=time.time,
    ) -> None:
        self._storage = storage
        self._dedup_window = dedup_window_seconds
        self._base_backoff = base_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._lease_seconds = lease_seconds
        self._clock = clock
        self._lock = threading.RLock()

    # ---- 提交与幂等去重 ----

    def submit(self, dedup_key: str, payload: str, *, max_attempts: int = 3) -> SubmitOutcome:
        if not dedup_key or not dedup_key.strip():
            return SubmitOutcome(SubmitResult.REJECTED, reason="dedup_key 为空，非法提交")
        if payload is None or payload == "":
            return SubmitOutcome(SubmitResult.REJECTED, reason="payload 为空，非法提交")
        if max_attempts < 1:
            return SubmitOutcome(SubmitResult.REJECTED, reason="max_attempts 必须 >= 1")

        payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        now = self._clock()
        with self._lock:
            existing = self._storage.get_by_dedup_key(
                dedup_key, window_start=now - self._dedup_window
            )
            if existing is not None:
                if existing.payload_hash == payload_hash:
                    return SubmitOutcome(
                        SubmitResult.DUPLICATE,
                        job_id=existing.job_id,
                        reason="去重窗口内相同 dedup_key 与参数，命中幂等去重，未重复入队",
                    )
                return SubmitOutcome(
                    SubmitResult.CONFLICT,
                    job_id=existing.job_id,
                    reason="dedup_key 已存在但参数不一致，拒绝覆盖已有任务",
                )
            job = Job(
                job_id=uuid.uuid4().hex,
                dedup_key=dedup_key,
                payload_hash=payload_hash,
                payload=payload,
                max_attempts=max_attempts,
                seq=self._storage.max_seq() + 1,
                next_attempt_at=now,
                created_at=now,
                updated_at=now,
            )
            self._storage.insert_job(job)
        return SubmitOutcome(SubmitResult.CREATED, job_id=job.job_id, reason="新建任务入队")

    # ---- 租约投递 ----

    def lease(self, consumer_id: str, *, limit: int = 1) -> list[Job]:
        now = self._clock()
        leased: list[Job] = []
        with self._lock:
            for job in self._storage.fetch_due(now, limit):
                job.status = JobStatus.LEASED
                job.lease_owner = consumer_id
                job.lease_expires_at = now + self._lease_seconds
                job.updated_at = now
                self._storage.update_job(job)
                leased.append(job)
        return leased

    # ---- 确认 / 失败 ----

    def _take_leased(self, job_id: str, consumer_id: str) -> Job:
        job = self._storage.get_by_id(job_id)
        if job is None:
            raise QueueError(f"任务不存在: {job_id}")
        if job.status is not JobStatus.LEASED or job.lease_owner != consumer_id:
            raise QueueError(
                f"任务 {job_id} 当前状态 {job.status.value}，非消费者 {consumer_id} 持有的租约"
            )
        return job

    def ack(self, job_id: str, consumer_id: str) -> None:
        with self._lock:
            job = self._take_leased(job_id, consumer_id)
            job.status = JobStatus.SUCCEEDED
            job.lease_owner = None
            job.lease_expires_at = None
            job.updated_at = self._clock()
            self._storage.update_job(job)

    def fail(self, job_id: str, consumer_id: str, *, retryable: bool, error: str) -> None:
        with self._lock:
            job = self._take_leased(job_id, consumer_id)
            now = self._clock()
            job.attempts += 1  # 每次失败只累加一次
            job.last_error = error
            job.lease_owner = None
            job.lease_expires_at = None
            job.updated_at = now
            if retryable and job.attempts < job.max_attempts:
                job.status = JobStatus.PENDING
                job.next_attempt_at = now + self._backoff(job.attempts)
            else:
                job.status = JobStatus.DEAD
                job.last_error = (
                    f"{'可重试但次数耗尽' if retryable else '不可重试失败'}: {error}"
                )
            self._storage.update_job(job)

    def _backoff(self, attempts: int) -> float:
        """有界指数退避：base * 2^(attempts-1)，封顶 max_backoff。"""
        return min(self._base_backoff * (2 ** (attempts - 1)), self._max_backoff)

    # ---- 中断恢复 ----

    def reap_expired_leases(self) -> int:
        """回收过期租约：任务回 PENDING 立即重新投递，attempts 不累加（未确认的失败不计数）。"""
        now = self._clock()
        reaped = 0
        with self._lock:
            for job in self._storage.fetch_expired_leases(now):
                job.status = JobStatus.PENDING
                job.lease_owner = None
                job.lease_expires_at = None
                job.next_attempt_at = now
                job.updated_at = now
                self._storage.update_job(job)
                reaped += 1
        return reaped

    def get(self, job_id: str) -> Job | None:
        return self._storage.get_by_id(job_id)
