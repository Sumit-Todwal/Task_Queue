[![CI](https://github.com/Sumit-Todwal/Task_Queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Sumit-Todwal/Task_Queue/actions/workflows/ci.yml)

**Live API:** https://taskqueue-production.up.railway.app

# Distributed Task Queue System

A task queue engine built from scratch in Python — no external libraries — implementing the same core patterns used by production systems like Celery and RQ: persistent task state, concurrent worker threads, automatic retries, a Dead Letter Queue, crash recovery, and graceful shutdown.

Built progressively across three layers to demonstrate how each concept adds production-readiness.

---

## Why this exists

Most applications need background jobs — sending emails, generating reports, processing uploads. Running these synchronously blocks the user. A task queue decouples the *producer* (whoever creates the job) from the *worker* (whoever executes it), letting both operate independently at their own pace.

This project builds that system from first principles, without hiding complexity behind a library.

---

## Architecture

```
Producer(s)
    │
    ▼
┌─────────────────────────────────┐
│        Task Queue (in-memory)   │  ← bounded, MAX_QUEUE_SIZE = 1000
│        queue.Queue              │
└─────────────────────────────────┘
    │           │           │
    ▼           ▼           ▼
Worker-0    Worker-1  ... Worker-49     (50 concurrent threads)
    │
    ├── on success  →  status: COMPLETED
    ├── on failure  →  status: FAILED → retry (up to 3x) → status: DEAD → DLQ
    └── on queue full  →  status: REJECTED
         │
         ▼
┌─────────────────────────────────┐
│     SQLite (tasks.db)           │  ← source of truth for all task state
│     id | status | retries | max_retries
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│     Dead Letter Queue (DLQ)     │  ← failed tasks for manual inspection
│     queue.Queue (MAX = 500)     │
└─────────────────────────────────┘
```

### Task state machine

```
PENDING → RUNNING → COMPLETED
                 ↘
                  FAILED → PENDING  (retry, up to max_retries)
                         ↘
                          DEAD → DLQ  (retries exhausted)

[Queue full] → REJECTED
```

---

## Project structure

```
Task_Queue/
├── basic_implementation.py      # Layer 1: single-threaded, in-memory queue
├── Concurrent_task_queue.py     # Layer 2: multi-threaded, 3 concurrent workers
├── db.py                        # SQLite persistence layer (CRUD for task state)
├── tasks_states_logging.py      # Layer 3: production-grade system (see below)
├── check_db.py                  # Dev utility: inspect current task states
└── reset_db.py                  # Dev utility: wipe and reinitialize the database
```

---

## The three layers explained

### Layer 1 — `basic_implementation.py`
The simplest possible task queue. A single producer enqueues jobs; a single worker processes them one at a time. Good for understanding the producer-consumer pattern before adding complexity.

### Layer 2 — `Concurrent_task_queue.py`
Introduces real multithreading. 3 daemon worker threads pull from a shared `queue.Queue` in parallel, each processing independently. Demonstrates how `task_queue.join()` and `task_done()` coordinate thread completion.

### Layer 3 — `tasks_states_logging.py`
The production-grade implementation. Key features:

**Persistent state** — SQLite is the source of truth. Only task IDs live in the in-memory queue; workers fetch full state from the DB on each pickup. This means task state survives crashes.

**Crash recovery** — On startup, the system queries for any tasks in `PENDING` or `RUNNING` state and re-enqueues them. A task stuck in `RUNNING` means a worker died mid-execution — the recovery treats it as incomplete and retries it.

**Retry logic** — Each task has a configurable `max_retries` (default: 3). On failure, the worker increments the retry counter, persists it, and re-enqueues the task ID. Retries stop during shutdown to avoid looping.

**Dead Letter Queue** — Tasks that exhaust all retries are marked `DEAD` and moved to an in-memory DLQ for manual inspection. If the DLQ itself is full, a `CRITICAL` log is emitted — the failure is never silent.

**Bounded queues** — Both the main queue (`MAX_QUEUE_SIZE = 1000`) and DLQ (`MAX_DLQ_SIZE = 500`) are size-limited. Tasks submitted when the queue is full are immediately marked `REJECTED` in the DB.

**Graceful shutdown** — A `SIGINT` handler (Ctrl+C) sets a global `SHUTDOWN` flag and pushes one `STOP` sentinel per worker thread (the poison pill pattern). Each worker exits cleanly after finishing its current task. Workers are non-daemon threads so the OS cannot kill them mid-execution.

**Structured logging** — Every state transition is logged with timestamp, worker ID, task ID, and status using Python's `logging` module.

---

## Key design decisions

**Why store only task IDs in the queue, not full task objects?**
In-memory state is lost on crash. By keeping SQLite as the source of truth and only queuing IDs, every worker always reads the latest persisted state. It also prevents stale data if task configuration changes between enqueue and execution.

**Why `INSERT OR IGNORE` in the persistence layer?**
Makes inserts idempotent. If the program crashes after writing to the DB but before processing, the recovery path re-inserts the same task ID. Without `OR IGNORE`, the primary key constraint would raise an exception.

**Why `threading` and not `multiprocessing`?**
Task queues are I/O-bound workloads (network calls, DB writes, file operations). Python's GIL is released during I/O, so threads achieve genuine parallelism for these cases. Multiprocessing would add IPC complexity without benefit for this workload profile.

**Why SQLite and not Redis?**
Zero external dependencies, runs in-process, and fully supports the persistence and crash recovery needed here. A production upgrade path would replace the in-memory queue with Redis (atomic LPUSH/RPOP) and the DB with PostgreSQL, but SQLite makes the concepts clear without infrastructure setup.

---

## Running it

No external dependencies required — uses Python standard library only.

```bash
# Clone the repo
git clone https://github.com/Sumit-Todwal/Task_Queue.git
cd Task_Queue

# Initialize the database
python db.py

# Run the full production system (50 workers, 2000 tasks)
python tasks_states_logging.py

# In another terminal, watch task states in real time
python check_db.py

# Reset everything and start fresh
python reset_db.py
```

To test graceful shutdown, press `Ctrl+C` while `tasks_states_logging.py` is running. Workers finish their current task and exit cleanly. Re-run the script — it will automatically recover and resume incomplete tasks.

---
## Sample output

Running `python tasks_states_logging.py` with 50 workers and 2000 tasks produces output like this:

```
10:42:01 | INFO | RECOVERY | FOUND 0 unfinished tasks
10:42:01 | INFO | PRODUCER | Task-0 | ENQUEUED
10:42:01 | INFO | PRODUCER | Task-1 | ENQUEUED
...
10:42:01 | INFO | PRODUCER | Task-1999 | ENQUEUED
10:42:01 | INFO | worker 0 | Task-0 | RUNNING
10:42:01 | INFO | worker 1 | Task-1 | RUNNING
10:42:01 | INFO | worker 2 | Task-2 | RUNNING
10:42:03 | INFO | worker 0 | Task-0 | COMPLETED
10:42:03 | INFO | worker 1 | Task-1 | RETRYING(1)
10:42:03 | INFO | worker 2 | Task-2 | COMPLETED
10:42:05 | INFO | worker 1 | Task-1 | RETRYING(2)
10:42:09 | INFO | worker 1 | Task-1 | Moved_to_DLQ
```

**To see crash recovery in action:**
1. Run `python tasks_states_logging.py` and press `Ctrl+C` mid-way
2. Run it again — you'll see `RECOVERY | FOUND N unfinished tasks` and it resumes automatically

```
10:43:15 | INFO | Graceful shutdown requested
10:43:15 | INFO | Worker-3 | Stopped
10:43:15 | INFO | Worker-7 | Stopped
...

# Re-run:
10:44:02 | INFO | RECOVERY | FOUND 47 unfinished tasks
10:44:02 | INFO | worker 0 | Task-312 | RUNNING
```

## What I'd build next

- **REST API layer** (Flask/FastAPI) — `POST /tasks`, `GET /tasks/{id}`, `POST /tasks/dlq/replay` — to turn this into a usable service rather than a script
- **Exponential backoff with jitter** — `time.sleep(2 ** task['retries'] + random.random())` before retrying, to avoid hammering a failing downstream dependency
- **Task prioritization** — replace `queue.Queue` with `queue.PriorityQueue`; add a `priority` column to the schema
- **Prometheus metrics** — queue depth, worker throughput, failure rate, DLQ size
- **Task timeout detection** — a watchdog that marks `RUNNING` tasks older than N seconds as failed (handles silent worker hangs)

---

## Concepts demonstrated

`Producer-consumer pattern` · `Thread-safe queues` · `Python GIL and I/O-bound concurrency` · `Daemon vs non-daemon threads` · `Poison pill shutdown pattern` · `SQLite persistence` · `Idempotent writes` · `At-least-once delivery` · `Dead Letter Queue` · `Crash recovery` · `SIGINT signal handling` · `Structured logging`