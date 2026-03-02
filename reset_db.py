import sqlite3

conn = sqlite3.connect("tasks.db")
cursor = conn.cursor()
cursor.execute("DELETE FROM tasks")
conn.commit()
conn.close()

print("DB cleared")