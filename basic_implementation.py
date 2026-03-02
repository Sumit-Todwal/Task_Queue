from queue import Queue
import time

task_queue = Queue()


def producer(task):
    print(f"produced task : {task}")
    task_queue.put(task)

def worker():
    while not task_queue.empty():
        task = task_queue.get()
        print(f"processing task : {task}")
        time.sleep(1)
        task_queue.task_done()


producer("Send email")
producer("Generate Report")
producer("Resize image")

worker()