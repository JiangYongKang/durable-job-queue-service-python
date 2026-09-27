"""本地可运行的持久化任务队列服务（FastAPI 入口）。

全部状态落盘到本地 SQLite 文件（默认 ./jobqueue.db，可用 JOBQUEUE_DB 覆盖），
不依赖任何外部服务。进程重启后任务与消费位点自动恢复。
"""
from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from jobqueue.offset import OffsetError, OffsetManager
from jobqueue.queue import JobQueue, QueueError
from jobqueue.storage import JobStorage

DB_PATH = os.environ.get("JOBQUEUE_DB", "./jobqueue.db")
CONSUMER_GROUP = "default"

storage = JobStorage(DB_PATH)
queue = JobQueue(storage)
offsets = OffsetManager(storage, CONSUMER_GROUP)

app = FastAPI(title="durable-job-queue-service")


class SubmitRequest(BaseModel):
    dedup_key: str
    payload: str
    max_attempts: int = Field(default=3, ge=1)


class FailRequest(BaseModel):
    consumer_id: str
    retryable: bool = True
    error: str = ""


class CommitRequest(BaseModel):
    seq: int
    verified: bool


@app.post("/jobs", status_code=201)
def submit(req: SubmitRequest):
    outcome = queue.submit(req.dedup_key, req.payload, max_attempts=req.max_attempts)
    return {"result": outcome.result.value, "job_id": outcome.job_id, "reason": outcome.reason}


@app.post("/jobs/lease")
def lease(consumer_id: str, limit: int = 1):
    queue.reap_expired_leases()  # 投递前顺带回收中断消费者的过期租约
    jobs = queue.lease(consumer_id, limit=limit)
    return {"jobs": [{"job_id": j.job_id, "seq": j.seq, "payload": j.payload,
                      "attempts": j.attempts} for j in jobs]}


@app.post("/jobs/{job_id}/ack")
def ack(job_id: str, consumer_id: str):
    try:
        queue.ack(job_id, consumer_id)
    except QueueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {"status": "SUCCEEDED"}


@app.post("/jobs/{job_id}/fail")
def fail(job_id: str, req: FailRequest):
    try:
        queue.fail(job_id, req.consumer_id, retryable=req.retryable, error=req.error)
    except QueueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    job = queue.get(job_id)
    return {"status": job.status.value, "attempts": job.attempts, "last_error": job.last_error}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = queue.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {"job_id": job.job_id, "seq": job.seq, "status": job.status.value,
            "attempts": job.attempts, "max_attempts": job.max_attempts,
            "last_error": job.last_error}


@app.get("/offset")
def get_offset():
    return {"consumer_group": CONSUMER_GROUP, "offset": offsets.current}


@app.post("/offset/commit")
def commit_offset(req: CommitRequest):
    try:
        new_offset = offsets.commit(req.seq, verified=req.verified)
    except OffsetError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {"offset": new_offset}


@app.get("/")
def read_root():
    return {"service": "durable-job-queue", "db": DB_PATH}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
