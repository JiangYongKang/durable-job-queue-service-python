"""核心数据模型与状态枚举（骨架：仅类型定义）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class JobStatus(str, enum.Enum):
    """任务状态机：PENDING -> LEASED -> SUCCEEDED / FAILED(可重试回PENDING) / DEAD。"""

    PENDING = "PENDING"
    LEASED = "LEASED"
    SUCCEEDED = "SUCCEEDED"
    DEAD = "DEAD"  # 终态：重试耗尽或不可重试失败


class SubmitResult(str, enum.Enum):
    """提交结果：可区分 新建/去重命中/参数冲突/非法提交。"""

    CREATED = "CREATED"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"
    REJECTED = "REJECTED"


class FailureKind(str, enum.Enum):
    """失败类别：决定是否可重试。"""

    RETRYABLE = "RETRYABLE"
    NON_RETRYABLE = "NON_RETRYABLE"


@dataclass
class Job:
    job_id: str
    dedup_key: str
    payload_hash: str
    payload: str
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    seq: int = 0  # 队列内单调递增序号，即位点
    lease_owner: str | None = None
    lease_expires_at: float | None = None
    next_attempt_at: float = 0.0
    last_error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0


@dataclass
class SubmitOutcome:
    result: SubmitResult
    job_id: str | None = None
    reason: str = ""
