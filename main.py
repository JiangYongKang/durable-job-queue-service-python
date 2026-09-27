"""HTTP API for the durable local job queue.

Run locally:  uv run python main.py  (serves on 127.0.0.1:8000)
State is stored in a local SQLite file (see QUEUE_DB_PATH), so job state
and consumer checkpoints survive process restarts.
"""

from __future__ import annotations

import os
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from jobqueue import JobQueue, QueueConfig
from jobqueue.models import AckResult, SubmitStatus

queue = JobQueue(
    QueueConfig(
        db_path=os.environ.get("QUEUE_DB_PATH", "jobqueue.db"),
        dedup_window_seconds=float(os.environ.get("QUEUE_DEDUP_WINDOW_SECONDS", "3600")),
        max_attempts=int(os.environ.get("QUEUE_MAX_ATTEMPTS", "3")),
        backoff_base_seconds=float(os.environ.get("QUEUE_BACKOFF_BASE_SECONDS", "1")),
        backoff_max_seconds=float(os.environ.get("QUEUE_BACKOFF_MAX_SECONDS", "60")),
        visibility_timeout_seconds=float(os.environ.get("QUEUE_VISIBILITY_TIMEOUT_SECONDS", "30")),
    )
)
app = FastAPI(title="durable-job-queue-service")


class SubmitRequest(BaseModel):
    idempotency_key: str
    payload: dict[str, Any]
    max_attempts: int | None = Field(default=None, ge=1)


class LeaseRequest(BaseModel):
    consumer_group: str
    consumer_id: str


class AckRequest(BaseModel):
    consumer_group: str
    consumer_id: str
    lease_token: str
    result_offset: int


class NackRequest(BaseModel):
    consumer_id: str
    lease_token: str
    error: str
    retryable: bool = True


def _job_view(job) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "idempotency_key": job.idempotency_key,
        "payload": job.payload,
        "status": job.status.value,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "offset": job.offset,
        "last_error": job.last_error,
        "terminal_reason": job.terminal_reason.value,
        "visible_at": job.visible_at,
        "lease_owner": job.lease_owner,
        "lease_expires_at": job.lease_expires_at,
    }


@app.post("/jobs", status_code=200)
def submit_job(req: SubmitRequest):
    result = queue.submit(req.idempotency_key, req.payload, max_attempts=req.max_attempts)
    body = {"status": result.status.value, "job_id": result.job_id, "reason": result.reason}
    if result.status is SubmitStatus.ACCEPTED:
        return body
    if result.status is SubmitStatus.DUPLICATE:
        return body  # 200: idempotent hit, existing job returned
    if result.status is SubmitStatus.CONFLICT:
        raise HTTPException(status_code=409, detail=body)
    raise HTTPException(status_code=422, detail=body)  # REJECTED


@app.get("/jobs")
def list_jobs():
    return {"jobs": [_job_view(j) for j in queue.list_jobs()]}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = queue.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return _job_view(job)


@app.post("/leases")
def lease_job(req: LeaseRequest):
    lease = queue.lease(req.consumer_group, req.consumer_id)
    if lease is None:
        return {"lease": None}
    return {
        "lease": {
            "job": _job_view(lease.job),
            "lease_token": lease.lease_token,
            "lease_expires_at": lease.lease_expires_at,
        }
    }


@app.post("/jobs/{job_id}/ack")
def ack_job(job_id: str, req: AckRequest):
    result = queue.ack(req.consumer_group, req.consumer_id, job_id, req.lease_token, req.result_offset)
    if result is AckResult.OK:
        return {"result": result.value}
    raise HTTPException(status_code=409, detail={"result": result.value})


@app.post("/jobs/{job_id}/nack")
def nack_job(job_id: str, req: NackRequest):
    result = queue.nack(req.consumer_id, job_id, req.lease_token, req.error, retryable=req.retryable)
    if result is AckResult.OK:
        return {"result": result.value}
    raise HTTPException(status_code=409, detail={"result": result.value})


@app.get("/checkpoints/{consumer_group}")
def get_checkpoint(consumer_group: str):
    return {"consumer_group": consumer_group, "offset": queue.get_checkpoint(consumer_group)}


@app.post("/maintenance/requeue-expired")
def requeue_expired():
    return {"requeued": queue.requeue_expired_leases()}


@app.get("/")
def read_root():
    return {"service": "durable-job-queue", "docs": "/docs"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
