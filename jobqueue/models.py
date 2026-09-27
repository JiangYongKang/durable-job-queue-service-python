"""Core type definitions for the durable job queue."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class JobStatus(str, enum.Enum):
    """Lifecycle states of a job."""

    PENDING = "pending"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    FAILED_RETRYABLE = "failed_retryable"  # waiting for next retry
    DEAD = "dead"  # terminal: retries exhausted or non-retryable failure


class SubmitStatus(str, enum.Enum):
    """Outcome of a submit call."""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"  # same idempotency key + same payload within window
    CONFLICT = "conflict"  # same idempotency key but different payload
    REJECTED = "rejected"  # invalid submission (bad args)


class AckResult(str, enum.Enum):
    """Outcome of ack/nack or checkpoint advance."""

    OK = "ok"
    NOT_FOUND = "not_found"
    NOT_LEASED = "not_leased"  # job is not currently leased (or lease expired)
    LEASE_MISMATCH = "lease_mismatch"  # wrong lease token / consumer
    CHECKPOINT_REGRESSION = "checkpoint_regression"  # offset <= committed
    CHECKPOINT_OUT_OF_ORDER = "checkpoint_out_of_order"  # gaps / unknown offset


class TerminalReason(str, enum.Enum):
    """Why a job reached a terminal state."""

    NONE = "none"
    SUCCEEDED = "succeeded"
    RETRIES_EXHAUSTED = "retries_exhausted"
    NON_RETRYABLE = "non_retryable"


@dataclass
class Job:
    """A durable job record."""

    job_id: str
    idempotency_key: str
    payload: dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    created_at: float = 0.0
    updated_at: float = 0.0
    visible_at: float = 0.0  # next time the job may be leased
    lease_owner: str | None = None
    lease_token: str | None = None
    lease_expires_at: float | None = None
    last_error: str | None = None
    terminal_reason: TerminalReason = TerminalReason.NONE
    offset: int = 0  # monotonic consumption offset assigned at enqueue time


@dataclass
class SubmitResult:
    status: SubmitStatus
    job_id: str | None = None
    reason: str = ""


@dataclass
class Lease:
    """A leased job handed to a consumer."""

    job: Job
    lease_token: str
    lease_expires_at: float
