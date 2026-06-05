import threading
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.responses import HTMLResponse
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


@app.get("/", response_class=HTMLResponse)
def root():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Task Queue API — Sumit Todwal</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f0f0f; color: #e0e0e0; min-height: 100vh; }
        .hero { padding: 80px 40px 60px; max-width: 900px; margin: 0 auto; }
        .badge { display: inline-block; background: #1a1a2e; color: #7c6af7; border: 1px solid #7c6af7; padding: 4px 12px; border-radius: 20px; font-size: 13px; margin-bottom: 24px; }
        h1 { font-size: 48px; font-weight: 700; color: #ffffff; line-height: 1.1; margin-bottom: 16px; }
        h1 span { color: #7c6af7; }
        .subtitle { font-size: 18px; color: #888; max-width: 600px; line-height: 1.6; margin-bottom: 40px; }
        .btn-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 80px; }
        .btn { padding: 12px 24px; border-radius: 8px; font-size: 15px; font-weight: 500; text-decoration: none; transition: all 0.2s; }
        .btn-primary { background: #7c6af7; color: white; }
        .btn-primary:hover { background: #6b58e8; }
        .btn-secondary { background: transparent; color: #e0e0e0; border: 1px solid #333; }
        .btn-secondary:hover { border-color: #666; }
        .features { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; margin-bottom: 60px; }
        .feature { background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 12px; padding: 24px; transition: border-color 0.2s; }
        .feature:hover { border-color: #7c6af7; }
        .feature-icon { font-size: 28px; margin-bottom: 12px; }
        .feature h3 { font-size: 16px; font-weight: 600; color: #fff; margin-bottom: 8px; }
        .feature p { font-size: 14px; color: #888; line-height: 1.5; }
        .stats { display: flex; gap: 40px; flex-wrap: wrap; margin-bottom: 60px; padding: 32px; background: #1a1a1a; border-radius: 12px; border: 1px solid #2a2a2a; }
        .stat-num { font-size: 36px; font-weight: 700; color: #7c6af7; }
        .stat-label { font-size: 13px; color: #888; margin-top: 4px; }
        .endpoints { margin-bottom: 60px; }
        .endpoints h2 { font-size: 24px; font-weight: 600; color: #fff; margin-bottom: 20px; }
        .endpoint { display: flex; align-items: center; gap: 12px; padding: 14px 16px; background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px; margin-bottom: 8px; }
        .method { font-size: 12px; font-weight: 700; padding: 3px 8px; border-radius: 4px; min-width: 52px; text-align: center; }
        .get { background: #0d3b2e; color: #4ade80; }
        .post { background: #1e2d4a; color: #60a5fa; }
        .path { font-family: monospace; font-size: 14px; color: #e0e0e0; }
        .desc { font-size: 13px; color: #666; margin-left: auto; }
        .footer { border-top: 1px solid #2a2a2a; padding: 32px 40px; max-width: 900px; margin: 0 auto; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 16px; }
        .footer-links { display: flex; gap: 20px; }
        .footer-links a { color: #888; text-decoration: none; font-size: 14px; }
        .footer-links a:hover { color: #7c6af7; }
        .footer p { font-size: 14px; color: #888; }
        .live { display: inline-flex; align-items: center; gap: 6px; }
        .live-dot { width: 8px; height: 8px; background: #4ade80; border-radius: 50%; animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
    </style>
</head>
<body>
    <div class="hero">
        <div class="badge">Production-grade · No external dependencies</div>
        <h1>Distributed <span>Task Queue</span><br>from scratch</h1>
        <p class="subtitle">A concurrent task queue engine built in Python implementing the same core patterns used by Celery and RQ — without hiding complexity behind a library.</p>
        <div class="btn-row">
            <a href="/docs" class="btn btn-primary">Explore Live API</a>
            <a href="https://github.com/Sumit-Todwal/Task_Queue" class="btn btn-secondary" target="_blank">View on GitHub</a>
            <a href="https://linkedin.com/in/sumit-todwal-810905350/" class="btn btn-secondary" target="_blank">Sumit Todwal</a>
        </div>

        <div class="stats">
            <div>
                <div class="stat-num">50</div>
                <div class="stat-label">Concurrent workers</div>
            </div>
            <div>
                <div class="stat-num">3x</div>
                <div class="stat-label">Retry with backoff</div>
            </div>
            <div>
                <div class="stat-num">1000</div>
                <div class="stat-label">Max queue size</div>
            </div>
            <div>
                <div class="stat-num live"><span class="live-dot"></span>Live</div>
                <div class="stat-label">Deployed on Railway</div>
            </div>
        </div>

        <div class="features">
            <div class="feature">
                <div class="feature-icon">🗄️</div>
                <h3>Persistent State</h3>
                <p>SQLite as source of truth. Task state survives crashes and restarts with automatic recovery.</p>
            </div>
            <div class="feature">
                <div class="feature-icon">🔄</div>
                <h3>Crash Recovery</h3>
                <p>On startup, PENDING and RUNNING tasks are automatically re-queued and resumed.</p>
            </div>
            <div class="feature">
                <div class="feature-icon">⚡</div>
                <h3>Priority Scheduling</h3>
                <p>PriorityQueue ensures urgent tasks are processed before lower priority work.</p>
            </div>
            <div class="feature">
                <div class="feature-icon">↩️</div>
                <h3>Retry + Backoff</h3>
                <p>Failed tasks retry up to 3 times with exponential backoff and jitter.</p>
            </div>
            <div class="feature">
                <div class="feature-icon">💀</div>
                <h3>Dead Letter Queue</h3>
                <p>Exhausted tasks move to DLQ for inspection. Replay them with one API call.</p>
            </div>
            <div class="feature">
                <div class="feature-icon">🛑</div>
                <h3>Graceful Shutdown</h3>
                <p>SIGINT handler sends poison pills to workers. All tasks finish before exit.</p>
            </div>
        </div>

        <div class="endpoints">
            <h2>API Endpoints</h2>
            <div class="endpoint">
                <span class="method post">POST</span>
                <span class="path">/tasks</span>
                <span class="desc">Enqueue a single task</span>
            </div>
            <div class="endpoint">
                <span class="method post">POST</span>
                <span class="path">/tasks/bulk</span>
                <span class="desc">Enqueue multiple tasks at once</span>
            </div>
            <div class="endpoint">
                <span class="method get">GET</span>
                <span class="path">/tasks/{task_id}</span>
                <span class="desc">Get task status</span>
            </div>
            <div class="endpoint">
                <span class="method get">GET</span>
                <span class="path">/stats</span>
                <span class="desc">Live system snapshot</span>
            </div>
            <div class="endpoint">
                <span class="method post">POST</span>
                <span class="path">/tasks/dlq/replay</span>
                <span class="desc">Replay all dead tasks</span>
            </div>
        </div>
    </div>

    <div class="footer">
        <p>Built by Sumit Todwal · B.Tech CSE · SKIT Jaipur</p>
        <div class="footer-links">
            <a href="/docs">API Docs</a>
            <a href="https://github.com/Sumit-Todwal/Task_Queue" target="_blank">GitHub</a>
            <a href="https://linkedin.com/in/sumit-todwal-810905350/" target="_blank">LinkedIn</a>
        </div>
    </div>
</body>
</html>
"""

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