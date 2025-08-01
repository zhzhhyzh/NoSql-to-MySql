import json
import mysql.connector

# === CONFIGURATION ===
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "",  # Change this
    "database": "sem"      # Change to your database name
}

JSON_FILE = "app.json"  # Ensure this is in the same folder as the script

# === DATABASE CONNECTION ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# === HELPER FUNCTION ===
def insert(table, data):
    if not data:
        return
    keys = data[0].keys()
    columns = ', '.join(keys)
    placeholders = ', '.join(['%s'] * len(keys))
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    for entry in data:
        values = tuple(entry.get(k) for k in keys)
        cursor.execute(sql, values)

# === LOAD JSON ===
with open(JSON_FILE, 'r', encoding='utf-8') as f:
    json_data = json.load(f)

# === INSERT INTO TABLES ===
try:
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

    insert('time_slot', json_data.get('time_slot', []))
    insert('classroom', json_data.get('classroom', []))
    insert('department', json_data.get('department', []))
    insert('course', json_data.get('course', []))
    insert('student', json_data.get('student', []))
    insert('instructor', json_data.get('instructor', []))
    insert('section', json_data.get('section', []))
    insert('teaches', json_data.get('teaches', []))
    insert('prereq', json_data.get('prereq', []))
    insert('takes', json_data.get('takes', []))
    insert('advisor', json_data.get('advisor', []))
    conn.commit()
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    print("Data inserted successfully!")
except Exception as e:
    print("Error:", e)
    conn.rollback()
finally:
    cursor.close()
    conn.close()
