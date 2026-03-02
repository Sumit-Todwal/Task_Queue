from queue import Queue
from threading import Thread
import time

task_queue = Queue()

def producer(task):
    task_queue.put(task)
    print(f"Produced task : {task}")

def worker(worker_id):
    while True:
        task = task_queue.get()
        print(f"Worker {worker_id} processing {task}")
        time.sleep(2)
        task_queue.task_done()

for i in range(6):
    producer(f"Task-{i}")


for i in range(3):
    # This line introduces concurrency in the code.
    # Multiple threads are created, each running the worker() function.
    # A unique worker_id is passed using the loop variable `i`.
    # daemon=True makes these threads background threads that automatically
    # terminate when the main program exits (not when tasks finish).
    # The start() call is what actually begins concurrent execution
    # by scheduling the threads to run independently.
    Thread(target=worker , args=(i,), daemon= True).start()

task_queue.join()
print("All tasks Completed")