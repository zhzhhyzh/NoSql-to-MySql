import os
import csv
import json
from typing import List, Tuple, Dict, Set

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

JSON_FILE = os.getenv("SRC_JSON", "app.json")
MYSQL_CFG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "sem"),
    "compress": True,
}

# How many mismatches to show on screen
PRINT_LIMIT = 25
# Whether to export full mismatch lists to CSV files
WRITE_CSV = True

# JSON field aliases -> canonical keys (advisor uses lowercase in your DDL)
ALIASES: Dict[str, List[str]] = {
    "i_id": ["i_id", "i_ID", "I_ID", "instructor_id", "instructorId"],
    "s_id": ["s_id", "s_ID", "S_ID", "student_id", "studentId"],
}

def norm(v) -> str:
    """Normalize key values for comparison (stringify and trim)."""
    if v is None:
        return ""
    return str(v).strip()

def get_json_advisors(path: str) -> Set[Tuple[str, str]]:
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    if not isinstance(doc, dict) or "advisor" not in doc or not isinstance(doc["advisor"], list):
        print("[WARN] JSON file has no 'advisor' list; returning empty set.")
        return set()

    out: Set[Tuple[str, str]] = set()
    for row in doc["advisor"]:
        if not isinstance(row, dict):
            continue
        # Resolve instructor id
        i_val = None
        for k in ALIASES["i_id"]:
            if k in row:
                i_val = row[k]; break
        # Resolve student id
        s_val = None
        for k in ALIASES["s_id"]:
            if k in row:
                s_val = row[k]; break
        out.add((norm(i_val), norm(s_val)))
    return out

def get_mysql_advisors(cfg: dict) -> Set[Tuple[str, str]]:
    conn = mysql.connector.connect(**cfg)
    cur = conn.cursor()
    # Your DDL uses lowercase column names i_id, s_id
    cur.execute("SELECT i_id, s_id FROM `advisor` ORDER BY i_id, s_id")
    res = {(norm(i), norm(s)) for (i, s) in cur.fetchall()}
    cur.close()
    conn.close()
    return res

def main():
    json_set = get_json_advisors(JSON_FILE)
    mysql_set = get_mysql_advisors(MYSQL_CFG)

    only_in_json  = sorted(list(json_set - mysql_set))
    only_in_mysql = sorted(list(mysql_set - json_set))
    in_both       = len(json_set & mysql_set)

    print("\n=== Advisor PK Comparison ===")
    print(f"JSON rows      : {len(json_set)}")
    print(f"MySQL rows     : {len(mysql_set)}")
    print(f"Intersection   : {in_both}")
    print(f"JSON-only rows : {len(only_in_json)}")
    print(f"MySQL-only rows: {len(only_in_mysql)}")

    if only_in_json:
        print(f"\n-- First {min(PRINT_LIMIT, len(only_in_json))} JSON-only pairs (i_id, s_id) --")
        for t in only_in_json[:PRINT_LIMIT]:
            print(t)

    if only_in_mysql:
        print(f"\n-- First {min(PRINT_LIMIT, len(only_in_mysql))} MySQL-only pairs (i_id, s_id) --")
        for t in only_in_mysql[:PRINT_LIMIT]:
            print(t)

    if WRITE_CSV:
        if only_in_json:
            with open("advisor_only_in_json.csv", "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f); w.writerow(["i_id", "s_id"]); w.writerows(only_in_json)
            print("\nWrote advisor_only_in_json.csv")
        if only_in_mysql:
            with open("advisor_only_in_mysql.csv", "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f); w.writerow(["i_id", "s_id"]); w.writerows(only_in_mysql)
            print("Wrote advisor_only_in_mysql.csv")

    if not only_in_json and not only_in_mysql:
        print("\n✅ Advisor sets match exactly.")

if __name__ == "__main__":
    main()
