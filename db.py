import sqlite3
import threading

DB_NAME = "tasks.db"
_local = threading.local()

def get_connection():
    if not hasattr(_local, 'conn'):
        _local.conn = sqlite3.connect(DB_NAME)
    return _local.conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY,
                   status TEXT,
                   retries INTEGER,
                   max_retries INTEGER)""")
    
    conn.commit()

def insert_task(task):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""INSERT OR IGNORE into tasks (id, status, retries, max_retries) VALUES(?,?,?,?) """,(task["id"],
                                                                                                task["status"],
                                                                                                task["retries"],
                                                                                                task["max_retries"]))
    conn.commit()


def get_recoverable_tasks():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""Select id from tasks where status IN ('PENDING','RUNNING')""")
    rows = cursor.fetchall()

    return [row[0] for row in rows]

def get_task(task_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, status, retries, max_retries FROM tasks where id = ?",(task_id,))
    row = cursor.fetchone()


    if row is None:
        return None
    
    return {
        "id" : row[0],
        "status" : row[1],
        "retries" : row[2],
        "max_retries" : row[3]
    }

def update_task(task):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""UPDATE tasks SET status = ?, retries = ? Where id = ? """,(
        task["status"],
        task["retries"],
        task["id"]
    ))

    conn.commit()


if __name__ == "__main__" :
    init_db()
    print("Database initialized")