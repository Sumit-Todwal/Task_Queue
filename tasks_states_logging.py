from queue import Queue
from threading import Thread
import time
import random
import logging
from db import insert_task
from db import update_task, get_task, get_recoverable_tasks
import signal
import sys
from queue import Full

SHUTDOWN_MODE = "Graceful"
SHUTDOWN = False
STOP = object()
Num_Worker = 50
MAX_QUEUE_SIZE = 1000
MAX_DLQ_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)


task_queue = Queue(maxsize = MAX_QUEUE_SIZE)
dead_letter_queue = Queue(maxsize = MAX_DLQ_SIZE)


def handle_shutdown(signum, frame):
    global SHUTDOWN
    if SHUTDOWN_MODE == "Graceful":
        logging.info("Graceful shutdown requested")
        SHUTDOWN = True
        for _ in range(Num_Worker):
            task_queue.put(STOP)
    else:
        logging.info("Forceful shutdown requested")
        sys.exit(1)

signal.signal(signal.SIGINT, handle_shutdown)



def recover_tasks_on_startup():
    recoverable_tasks = get_recoverable_tasks()
    logging.info(f"RECOVERY | FOUND {len(recoverable_tasks)} unfinished tasks")
    for task_id in recoverable_tasks:
        task_queue.put(task_id)

    return len(recoverable_tasks) > 0

def producer(task_id):
    task = {
        "id": task_id,
        "status": "PENDING",
        "retries" : 0,
        "max_retries" : 3
    }
    insert_task(task)      # DB is the main source of truth now for maintaining persistence.
    try:
        task_queue.put(task_id, block = False)    # Now on task_queue only contain task_id
        logging.info(f"PRODUCER | {task['id']} | ENQUEUED")
    except Full:
        task["status"] = "REJECTED"
        update_task(task)
        logging.warning(f"PRODUCER | {task['id']} | QUEUE FULL -> REJECTED")


def log(worker_id, task_id, status):
    logging.info(f"worker {worker_id} | {task_id} | {status}")


def worker(worker_id):
    while True:
        try :
            task_id = task_queue.get()
            if task_id is STOP:
                task_queue.task_done()
                break
        except Exception as e:
            logging.error(f"Worker {worker_id} | Failed to get task from queue | {e}")
            continue
        
        task = get_task(task_id)
        if task == None:
            task_queue.task_done()
            continue
        try:
            task["status"] = "RUNNING"
            update_task(task)
            log(worker_id,task["id"],"RUNNING")

            time.sleep(2)

            if random.random() < 0.2:
                raise Exception("Random failure")

            task["status"] = "COMPLETED"
            update_task(task)
            log(worker_id,task["id"],"COMPLETED")

        except Exception as e:
            task["retries"] += 1
            task["status"] = "FAILED"
            update_task(task)

            if task["retries"] <= task["max_retries"] and not SHUTDOWN:
                log(worker_id,task["id"],f"RETRYING({task['retries']})")
                task["status"] = "PENDING"
                update_task(task)
                task_queue.put(task_id)
            else :
                task["status"] = "DEAD"
                update_task(task)
                log(worker_id,task["id"],"Moved_to_DLQ")
                try:
                    dead_letter_queue.put(task_id, block = False)
                except:
                    logging.critical(f"DLQ FULL | Task {task_id} LOST")

        finally:
            task_queue.task_done()
    logging.info(f"Worker-{worker_id} | Stopped")


IS_RECOVERY_MODE = recover_tasks_on_startup()

if not IS_RECOVERY_MODE:
    for i in range(2000):
        producer(f"Task-{i}")

for i in range(Num_Worker):
    Thread(target=worker, args=(i,), daemon=False).start()


task_queue.join()
print("All tasks processed")