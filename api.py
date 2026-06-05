import threading
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
import sqlite3

from db import init_db, insert_task, get_task, get_recoverable_tasks, update_task
from tasks_states_logging import (
    task_queue, dead_letter_queue, producer, worker,
    recover_tasks_on_startup, Num_Worker, STOP
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

worker_threads = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    recover_tasks_on_startup()
    for i in range(Num_Worker):
        t = threading.Thread(target=worker, args=(i,), daemon=False)
        t.start()
        worker_threads.append(t)
    logging.info(f"Started {Num_Worker} workers")
    yield
    logging.info("Shutting down workers...")
    for _ in range(Num_Worker):
        task_queue.put((0,STOP))
    for t in worker_threads:
        t.join()
    logging.info("All workers stopped")


app = FastAPI(
    title="Task Queue API — Sumit Todwal",
    description="""
Production-grade distributed task queue built from scratch in Python.

**Features:** Persistent state · Concurrent workers · Priority scheduling · 
Retry with exponential backoff · Dead Letter Queue · Crash recovery · Graceful shutdown

**GitHub:** https://github.com/Sumit-Todwal/Task_Queue  
**Built by:** Sumit Todwal · https://linkedin.com/in/sumit-todwal-810905350/
    """,
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/")
def root():
    return RedirectResponse(url="/docs")

class TaskRequest(BaseModel):
    task_id: str
    max_retries: int = 3
    priority : int = 1


class TaskResponse(BaseModel):
    task_id: str
    status: str
    retries: int
    max_retries: int
    priority: int = 1


@app.post("/tasks", response_model=TaskResponse, status_code=201)
def enqueue_task(request: TaskRequest):
    existing = get_task(request.task_id)
    if existing:
        raise HTTPException(status_code=409, detail=f"Task '{request.task_id}' already exists")
    producer(request.task_id, request.max_retries, request.priority)
    task = get_task(request.task_id)
    return TaskResponse(
        task_id=task["id"],
        status=task["status"],
        retries=task["retries"],
        max_retries=task["max_retries"],
        priority = task["priority"]
    )


@app.get("/tasks/{task_id}", response_model=TaskResponse)
def get_task_status(task_id: str):
    task = get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
    return TaskResponse(
        task_id=task["id"],
        status=task["status"],
        retries=task["retries"],
        max_retries=task["max_retries"],
        priority = task["priority"]
    )


@app.get("/stats")
def get_stats():
    from db import get_connection
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT status, COUNT(*) FROM tasks GROUP BY status
    """)
    rows = cursor.fetchall()
    stats = {row[0]: row[1] for row in rows}
    return {
        "pending":   stats.get("PENDING", 0),
        "running":   stats.get("RUNNING", 0),
        "completed": stats.get("COMPLETED", 0),
        "failed":    stats.get("FAILED", 0),
        "dead":      stats.get("DEAD", 0),
        "rejected":  stats.get("REJECTED", 0),
        "queue_size": task_queue.qsize(),
        "dlq_size":   dead_letter_queue.qsize()
    }


@app.post("/tasks/dlq/replay", status_code=200)
def replay_dlq():
    from db import get_connection
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM tasks WHERE status = 'DEAD'")
    dead_tasks = [row[0] for row in cursor.fetchall()]
    if not dead_tasks:
        return {"replayed": 0, "message": "No dead tasks to replay"}
    for task_id in dead_tasks:
        task = get_task(task_id)
        task["status"] = "PENDING"
        task["retries"] = 0
        update_task(task)
        task_queue.put((task["priority"], task_id))
    logging.info(f"DLQ REPLAY | Re-queued {len(dead_tasks)} dead tasks")
    return {"replayed": len(dead_tasks), "message": f"Re-queued {len(dead_tasks)} tasks"}



class BulkTaskRequest(BaseModel):
    tasks: list[TaskRequest]

@app.post("/tasks/bulk", status_code=201)
def enqueue_bulk_tasks(request: BulkTaskRequest):
    results = []
    for t in request.tasks:
        existing = get_task(t.task_id)
        if existing:
            results.append({"task_id": t.task_id, "status": "SKIPPED", "reason": "already exists"})
            continue
        producer(t.task_id, t.max_retries,t.priority)
        task = get_task(t.task_id)
        results.append({"task_id": task["id"], "status": task["status"]})
    return {"enqueued": len([r for r in results if r["status"] == "PENDING"]), "results": results}