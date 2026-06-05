import pytest
import sqlite3
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import db

@pytest.fixture(autouse=True)
def use_temp_db(tmp_path):
    db.DB_NAME = str(tmp_path / "test_tasks.db")
    db._local = db.threading.local()
    db.init_db()
    yield
    conn = db.get_connection()
    conn.close()


def make_task(task_id="Task-1", status="PENDING", retries=0, max_retries=3,priority=1):
    return {"id": task_id, "status": status, "retries": retries, "max_retries": max_retries,"priority" : priority}


def test_insert_and_get_task():
    task = make_task()
    db.insert_task(task)
    result = db.get_task("Task-1")
    assert result["id"] == "Task-1"
    assert result["status"] == "PENDING"
    assert result["retries"] == 0


def test_update_task_status():
    task = make_task()
    db.insert_task(task)
    task["status"] = "COMPLETED"
    task["retries"] = 1
    db.update_task(task)
    result = db.get_task("Task-1")
    assert result["status"] == "COMPLETED"
    assert result["retries"] == 1


def test_get_recoverable_tasks():
    db.insert_task(make_task("Task-1", "PENDING"))
    db.insert_task(make_task("Task-2", "RUNNING"))
    db.insert_task(make_task("Task-3", "COMPLETED"))
    recoverable = db.get_recoverable_tasks()
    assert "Task-1" in recoverable
    assert "Task-2" in recoverable
    assert "Task-3" not in recoverable


def test_insert_is_idempotent():
    task = make_task()
    db.insert_task(task)
    db.insert_task(task)  # second insert should be silently ignored
    result = db.get_task("Task-1")
    assert result is not None  # no exception, no duplicate


def test_get_task_returns_none_for_missing():
    result = db.get_task("nonexistent-task")
    assert result is None