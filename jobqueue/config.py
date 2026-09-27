"""Configuration knobs for the durable job queue."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QueueConfig:
    """Tunable behaviour of the queue.

    All durations are in seconds.
    """

    db_path: str = ":memory:"
    dedup_window_seconds: float = 3600.0
    max_attempts: int = 3
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    visibility_timeout_seconds: float = 30.0
