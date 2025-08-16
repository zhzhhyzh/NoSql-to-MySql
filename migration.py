import os
import base64
import json
import mysql.connector
from typing import List, Dict, Tuple  # Py 3.8 typing
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from dotenv import load_dotenv

# === ENV ===
load_dotenv()

# === CONFIGURATION ===
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "sem"),
}

JSON_FILE = "app.json"

# AES key: 32 bytes (Base64 in .env)
KEY_B64 = os.getenv("APP_AES256_KEY_B64")
if not KEY_B64:
    raise RuntimeError("Set APP_AES256_KEY_B64 in .env (Base64-encoded 32-byte AES key).")
try:
    KEY = base64.b64decode(KEY_B64)
except Exception as e:
    raise RuntimeError(f"APP_AES256_KEY_B64 is not valid Base64: {e}")
if len(KEY) != 32:
    raise RuntimeError("APP_AES256_KEY_B64 must decode to 32 bytes (AES-256).")
AES = AESGCM(KEY)

# === DB CONNECTION ===
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(prepared=True)

# === CRYPTO HELPERS ===
def enc_field(value_str: str, aad: bytes) -> Tuple[bytes, bytes]:
    """
    Encrypt a string with AES-GCM. Returns (ciphertext_with_tag, iv).
    """
    if value_str is None:
        value_str = ""
    iv = os.urandom(12)  # 96-bit nonce for GCM
    ct = AES.encrypt(iv, value_str.encode("utf-8"), aad)
    return ct, iv

def aad_takes(rec: Dict) -> bytes:
    parts = [
        str(rec.get("ID", "")),
        str(rec.get("course_id", "")),
        str(rec.get("sec_id", "")),
        str(rec.get("semester", "")),
        str(rec.get("year", "")),
    ]
    return "|".join(parts).encode("utf-8")

def aad_by_id(rec: Dict) -> bytes:
    return str(rec.get("ID", "")).encode("utf-8")

# === INSERT HELPER (handles varying column sets) ===
BATCH_ROWS = 200  # tune: 100–500 is usually safe

def insert_many(table, rows):
    if not rows:
        return
    # stable union of columns across rows
    all_keys, seen = [], set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); all_keys.append(k)

    cols = ", ".join(f"`{k}`" for k in all_keys)
    placeholders = ", ".join(["%s"] * len(all_keys))
    sql = f"INSERT INTO `{table}` ({cols}) VALUES ({placeholders})"

    # batch by row count
    for i in range(0, len(rows), BATCH_ROWS):
        chunk = rows[i:i+BATCH_ROWS]
        vals = [tuple(r.get(k) for k in all_keys) for r in chunk]
        cursor.executemany(sql, vals)

        
# === LOAD JSON ===
with open(JSON_FILE, "r", encoding="utf-8") as f:
    json_data = json.load(f)

# === BUILD ENCRYPTED ROWS ===
def build_takes_rows(src: List[Dict]) -> List[Dict]:
    out = []
    if not src:
        return out
    for rec in src:
        aad = aad_takes(rec)
        grade_ct, grade_iv = enc_field(rec.get("grade"), aad)
        row = {
            "ID": int(rec["ID"]),
            "course_id": str(rec["course_id"]),
            "sec_id": str(rec["sec_id"]),
            "semester": str(rec["semester"]),
            "year": int(rec["year"]),
            "grade_ct": grade_ct,
            "grade_iv": grade_iv,
        }
        out.append(row)
    return out

# Keep PK/FK plaintext; encrypt everything else (student, instructor)
PRESERVE_STUDENT = {"ID", "dept_name"}
PRESERVE_INSTRUCTOR = {"ID", "dept_name"}

def build_enc_rows(src: List[Dict], preserve_keys: set, aad_builder) -> List[Dict]:
    out = []
    if not src:
        return out
    for rec in src:
        row = {}
        # preserved keys (IDs as ints)
        for k in preserve_keys:
            if k in rec:
                row[k] = int(rec[k]) if k == "ID" and rec[k] is not None else rec[k]
        aad = aad_builder(rec)
        for k, v in rec.items():
            if k in preserve_keys:
                continue
            ct, iv = enc_field("" if v is None else str(v), aad)
            row["{}_ct".format(k)] = ct
            row["{}_iv".format(k)] = iv
        out.append(row)
    return out

student_rows = build_enc_rows(json_data.get("student"), PRESERVE_STUDENT, aad_by_id)
instructor_rows = build_enc_rows(json_data.get("instructor"), PRESERVE_INSTRUCTOR, aad_by_id)
takes_rows = build_takes_rows(json_data.get("takes"))

# Pass-through for other tables
def passthrough_rows(src: List[Dict]) -> List[Dict]:
    return src or []

time_slot_rows = passthrough_rows(json_data.get("time_slot"))
classroom_rows = passthrough_rows(json_data.get("classroom"))
department_rows = passthrough_rows(json_data.get("department"))
course_rows = passthrough_rows(json_data.get("course"))
section_rows = passthrough_rows(json_data.get("section"))
teaches_rows = passthrough_rows(json_data.get("teaches"))
prereq_rows = passthrough_rows(json_data.get("prereq"))
advisor_rows = passthrough_rows(json_data.get("advisor"))

# === EXECUTE INSERTS ===
try:
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

    insert_many("time_slot", time_slot_rows)
    insert_many("classroom", classroom_rows)
    insert_many("department", department_rows)
    insert_many("course", course_rows)

    insert_many("student", student_rows)      # encrypted fields
    insert_many("instructor", instructor_rows)  # encrypted fields

    insert_many("section", section_rows)
    insert_many("teaches", teaches_rows)
    insert_many("prereq", prereq_rows)

    insert_many("takes", takes_rows)          # grade encrypted

    insert_many("advisor", advisor_rows)

    conn.commit()
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
    print("Data inserted successfully with AES-GCM encryption!")
except Exception as e:
    print("Error:", e)
    conn.rollback()
finally:
    cursor.close()
    conn.close()
