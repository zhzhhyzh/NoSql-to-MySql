#Third Step
# Python 3.8+
import os
import json
import binascii
import hashlib
from typing import Dict, List, Tuple, Iterable

from dotenv import load_dotenv
import mysql.connector

# ----------------- CONFIG -----------------
load_dotenv()

JSON_FILE = os.getenv("SRC_JSON", "app.json")

MYSQL_CFG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "sem"),
    "compress": True,
}

TABLES = [
    "time_slot","classroom","department","course","student","instructor",
    "section","teaches","prereq","takes","advisor"
]

# Primary keys (aligned to DDL)
PKS: Dict[str, Tuple[str, ...]] = {
    "time_slot": ("time_slot_id", "day"),
    "classroom": ("building", "room_number"),
    "department": ("dept_name",),
    "course": ("course_id",),
    "student": ("ID",),
    "instructor": ("ID",),
    "section": ("course_id", "sec_id", "semester"),
    "teaches": ("ID", "course_id", "sec_id"),          # DDL PK does NOT include semester/year
    "prereq": ("course_id", "prereq_id"),
    "takes": ("ID", "course_id", "sec_id"),            # DDL PK does NOT include semester/year
    "advisor": ("i_ID", "s_ID"),                       # lowercase per DDL
}

# Foreign keys (child -> parent), aligned to DDL
FK_MAP: List[Tuple[Tuple[str, Tuple[str, ...]], Tuple[str, Tuple[str, ...]]]] = [
    (("student", ("dept_name",)), ("department", ("dept_name",))),
    (("instructor", ("dept_name",)), ("department", ("dept_name",))),
    (("section", ("course_id",)), ("course", ("course_id",))),

    # teaches references instructor(ID) and section(course_id, sec_id)
    (("teaches", ("ID",)), ("instructor", ("ID",))),
    (("teaches", ("course_id", "sec_id")), ("section", ("course_id", "sec_id"))),

    # takes references student(ID) and section(course_id, sec_id)
    (("takes", ("ID",)), ("student", ("ID",))),
    (("takes", ("course_id", "sec_id")), ("section", ("course_id", "sec_id"))),

    # advisor references instructor(ID) and student(ID) using lowercase columns
    (("advisor", ("i_ID",)), ("instructor", ("ID",))),
    (("advisor", ("s_ID",)), ("student", ("ID",))),

    # prereq references course(course_id) twice
    (("prereq", ("course_id",)), ("course", ("course_id",))),
    (("prereq", ("prereq_id",)), ("course", ("course_id",))),
]

ANOMALY_LIMIT = 20
SAMPLE_LIMIT = 10
MYSQL_BATCH = 2000

STUDENT_PRESERVE = {"ID", "dept_name"}
INSTR_PRESERVE   = {"ID", "dept_name"}

# ----------------- LOAD JSON -----------------
def load_json_collections(path: str) -> Dict[str, List[dict]]:
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)
    if isinstance(doc, dict):
        # expected shape: {"student":[...], "instructor":[...], ...}
        return {k: v for k, v in doc.items() if isinstance(v, list)}
    elif isinstance(doc, list):
        # single unnamed collection
        return {"root": doc}
    else:
        raise ValueError("Unsupported JSON top-level structure.")
# ----------------- MYSQL -----------------
mysql = mysql.connector.connect(**MYSQL_CFG)
cur = mysql.cursor()

def mysql_count(table: str) -> int:
    cur.execute("SELECT COUNT(1) FROM `{}`".format(table))
    return int(cur.fetchone()[0])

def get_mysql_columns(table: str) -> List[str]:
    cur2 = mysql.cursor()
    cur2.execute("SHOW COLUMNS FROM `{}`".format(table))
    cols = [r[0] for r in cur2.fetchall()]
    cur2.close()
    return cols

def mysql_rows_iter(table: str, pk_cols: Tuple[str, ...]) -> Iterable[Tuple[List[str], Tuple]]:
    """
    Yield rows ordered by PK columns that actually exist in MySQL.
    If none exist, fall back to a single available column to keep determinism.
    """
    cols = get_mysql_columns(table)
    order_cols = [c for c in pk_cols if c in cols]

    if not order_cols:
        # fallback: pick a sensible deterministic column
        fallback = None
        for try_col in ("id", "ID", "pk"):
            if try_col in cols:
                fallback = try_col
                break
        if not fallback and cols:
            fallback = cols[0]
        if fallback:
            print("[WARN] `{}`: none of declared PK {} exist. Using ORDER BY `{}`."
                  .format(table, pk_cols, fallback))
            order_cols = [fallback]
        else:
            print("[WARN] `{}`: empty table or no columns; ordering skipped.".format(table))
            order_cols = []

    order_sql = ", ".join("`{}`".format(c) for c in order_cols)
    sql = "SELECT * FROM `{}`".format(table) + ("" if not order_sql else " ORDER BY " + order_sql)

    cur2 = mysql.cursor()
    cur2.execute(sql)
    all_cols = [d[0] for d in cur2.description]
    while True:
        rows = cur2.fetchmany(MYSQL_BATCH)
        if not rows:
            break
        for r in rows:
            yield all_cols, r
    cur2.close()

def mysql_select_cols_iter(table: str, cols: List[str], order_by: Tuple[str, ...]) -> Iterable[Tuple]:
    ob = ", ".join("`{}`".format(c) for c in order_by)
    sel = ", ".join("`{}`".format(c) for c in cols)
    cur2 = mysql.cursor()
    cur2.execute("SELECT {} FROM `{}` ORDER BY {}".format(sel, table, ob))
    while True:
        rows = cur2.fetchmany(MYSQL_BATCH)
        if not rows: break
        for r in rows:
            yield r
    cur2.close()

# ----------------- CHECKSUMS -----------------
def _to_str(v):
    return "" if v is None else str(v)

def checksum_json_by_pk(rows: List[dict], pk_cols: Tuple[str, ...]) -> str:
    # Sort by PK tuple for deterministic order
    def keyf(r):
        return tuple(_to_str(r.get(k)) for k in pk_cols)
    h = hashlib.sha256()
    for r in sorted(rows, key=keyf):
        key = "|".join(_to_str(r.get(k)) for k in pk_cols)
        h.update((key + "\n").encode("utf-8"))
    return h.hexdigest()

def checksum_mysql_by_pk(table: str, pk_cols: Tuple[str, ...]) -> str:
    """
    SHA-256 over canonical strings of (existing) PK tuple values.
    Warns and uses subset if some PK columns are missing.
    Falls back to hashing full rows if none of the PK columns exist.
    """
    table_cols = get_mysql_columns(table)
    effective_pk = tuple(c for c in pk_cols if c in table_cols)

    if len(effective_pk) != len(pk_cols):
        missing = tuple(c for c in pk_cols if c not in table_cols)
        print("[WARN] `{}`: missing PK columns {}. Using subset {} for checksum."
              .format(table, missing, effective_pk if effective_pk else "(none)"))

    h = hashlib.sha256()
    for cols, row in mysql_rows_iter(table, effective_pk):
        if effective_pk:
            key = "|".join(_to_str(row[cols.index(k)]) for k in effective_pk)
        else:
            # last resort: hash all columns in row order
            key = "|".join(_to_str(v) for v in row)
        h.update((key + "\n").encode("utf-8"))
    return h.hexdigest()

# ----------------- REPORTING -----------------
def print_header(title: str):
    print("\n" + "="*len(title))
    print(title)
    print("="*len(title))

def show_top10_takes(json_rows: List[dict]):
    print_header("TOP 10: takes (JSON plaintext vs MySQL ciphertext)")

    # detect actual 'year' column name in MySQL: year / yr / year_ ...
    takes_cols = set(get_mysql_columns("takes"))
    year_col = None
    for cand in ("year", "year_", "yr", "acad_year"):
        if cand in takes_cols:
            year_col = cand
            break
    if not year_col:
        print("[WARN] `takes`: year-like column not found; using JSON 'year' only where present.")

    # sort JSON deterministically
    def k(r):
        return (_to_str(r.get("ID")), _to_str(r.get("course_id")), _to_str(r.get("sec_id")),
                _to_str(r.get("semester")), int(r.get("year") or 0))
    jrows = sorted(json_rows, key=k)[:SAMPLE_LIMIT]

    for doc in jrows:
        key_vals = [doc.get("ID"), doc.get("course_id"), doc.get("sec_id"), doc.get("semester")]
        where = "ID=%s AND course_id=%s AND sec_id=%s AND semester=%s"
        params = key_vals.copy()

        # add year predicate if we detected a column
        if year_col:
            where += " AND `{}`=%s".format(year_col)
            params.append(doc.get("year"))

        sql = "SELECT grade_ct, grade_iv FROM `takes` WHERE " + where + " LIMIT 1"
        cur.execute(sql, tuple(params))
        r = cur.fetchone()

        key_display = (doc.get("ID"), doc.get("course_id"), doc.get("sec_id"),
                       doc.get("semester"), doc.get("year"))
        if r:
            grade_ct, grade_iv = r
            iv_hex = binascii.hexlify(grade_iv).decode("ascii") if grade_iv is not None else "(NULL)"
            print("- Key={} | JSON.grade={} | MYSQL.grade_ct_len={} | iv={}".format(
                key_display, doc.get("grade"), 0 if grade_ct is None else len(grade_ct), iv_hex))
        else:
            print("- Key={} | JSON.grade={} | MYSQL: row not found!".format(key_display, doc.get("grade")))

def show_top10_person(table: str, json_rows: List[dict], preserve_keys: set):
    print_header("TOP 10: {} (JSON plaintext vs MySQL encrypted)".format(table))
    jrows = sorted(json_rows, key=lambda r: _to_str(r.get("ID")))[:SAMPLE_LIMIT]
    # discover encrypted columns: *_ct/*_iv
    cur2 = mysql.cursor()
    cur2.execute("SHOW COLUMNS FROM `{}`".format(table))
    cols = [r[0] for r in cur2.fetchall()]
    cur2.close()
    enc_fields = sorted({c[:-3] for c in cols if c.endswith("_ct") and c[:-3] not in preserve_keys})

    for doc in jrows:
        idv = doc.get("ID")
        cur.execute("SELECT * FROM `{}` WHERE ID=%s".format(table), (idv,))
        row = cur.fetchone()
        if not row:
            print("- ID={} | MYSQL: row not found!".format(idv))
            continue
        cur3 = mysql.cursor()
        cur3.execute("SHOW COLUMNS FROM `{}`".format(table))
        mcols = [r[0] for r in cur3.fetchall()]
        cur3.close()
        row_map = dict(zip(mcols, row))

        preview = []
        # show a few encrypted fields to keep output short
        for base in enc_fields[:4]:
            jval = doc.get(base)
            ct = row_map.get("{}_ct".format(base))
            iv = row_map.get("{}_iv".format(base))
            iv_hex = binascii.hexlify(iv).decode("ascii") if isinstance(iv, (bytes, bytearray)) else "(NULL)"
            preview.append("{}: JSON='{}' | ct_len={} | iv={}".format(
                base, jval, 0 if ct is None else len(ct), iv_hex
            ))
        pstr = " ; ".join(preview) if preview else "(no encrypted fields detected)"
        print("- ID={} | {}".format(idv, pstr))

def find_pk_duplicates(table: str, pk_cols: Tuple[str, ...]) -> List[Tuple]:
    cols_in_db = get_mysql_columns(table)
    effective = [c for c in pk_cols if c in cols_in_db]
    if not effective:
        print("[WARN] `{}`: cannot check PK duplicates; none of {} exist.".format(table, pk_cols))
        return []
    cols_sql = ", ".join("`{}`".format(c) for c in effective)
    sql = "SELECT {}, COUNT(*) c FROM `{}` GROUP BY {} HAVING c>1 LIMIT {}".format(
        cols_sql, table, cols_sql, ANOMALY_LIMIT
    )
    cur.execute(sql)
    return cur.fetchall()

def fk_orphans(child: str, child_cols: Tuple[str, ...], parent: str, parent_cols: Tuple[str, ...]) -> List[Tuple]:
    child_db_cols = set(get_mysql_columns(child))
    parent_db_cols = set(get_mysql_columns(parent))

    c_eff = [c for c in child_cols if c in child_db_cols]
    p_eff = [p for p in parent_cols if p in parent_db_cols]

    if len(c_eff) != len(child_cols) or len(p_eff) != len(parent_cols):
        print("[WARN] FK `{}`({}) -> `{}`({}): dropping missing columns; using child={} parent={}"
              .format(child, child_cols, parent, parent_cols, tuple(c_eff), tuple(p_eff)))

    if not c_eff or not p_eff or len(c_eff) != len(p_eff):
        print("[WARN] FK `{}` -> `{}`: skipped (no usable column pairing)".format(child, parent))
        return []

    on = " AND ".join("C.`{}` = P.`{}`".format(cc, pc) for cc, pc in zip(c_eff, p_eff))
    sel_child = ", ".join("C.`{}`".format(c) for c in c_eff)
    notnull = " OR ".join("C.`{}` IS NOT NULL".format(c) for c in c_eff)
    parentnull = " OR ".join("P.`{}` IS NULL".format(pc) for pc in p_eff)

    sql = """
    SELECT {sel}
    FROM `{child}` C
    LEFT JOIN `{parent}` P ON {on}
    WHERE ({notnull}) AND ({parentnull})
    LIMIT {lim}
    """.format(sel=sel_child, child=child, parent=parent, on=on,
               notnull=notnull, parentnull=parentnull, lim=ANOMALY_LIMIT)
    cur.execute(sql)
    return cur.fetchall()

# ----------------- MAIN -----------------
def main():
    # Load JSON collections
    src = load_json_collections(JSON_FILE)

    print_header("ROW COUNTS (JSON file vs MySQL)")
    for t in TABLES:
        jrows = src.get(t, [])
        my = mysql_count(t)
        mc = len(jrows)
        status = "OK" if mc == my else "MISMATCH"
        print("- {}: JSON={} | MySQL={} -> {}".format(t, mc, my, status))

    print_header("CHECKSUMS by Primary Key (JSON vs MySQL)")
    for t in TABLES:
        pk = PKS.get(t)
        if not pk:
            print("- {}: skipped (no PK defined)".format(t))
            continue
        jrows = src.get(t, [])
        j_sum = checksum_json_by_pk(jrows, pk)
        m_sum = checksum_mysql_by_pk(t, pk)
        status = "OK" if j_sum == m_sum else "DIFF"
        print("- {}: SHA256(JSON)={} | SHA256(MySQL)={} -> {}".format(t, j_sum, m_sum, status))

    # Before vs after (no decryption)
    if "takes" in src:
        show_top10_takes(src["takes"])
    if "student" in src:
        show_top10_person("student", src["student"], STUDENT_PRESERVE)
    if "instructor" in src:
        show_top10_person("instructor", src["instructor"], INSTR_PRESERVE)

    print_header("PRIMARY KEY UNIQUENESS (MySQL)")
    for t in TABLES:
        pk = PKS.get(t)
        if not pk: 
            continue
        dups = find_pk_duplicates(t, pk)
        if dups:
            print("- {}: DUPLICATES found (showing up to {}): {}".format(t, ANOMALY_LIMIT, dups))
        else:
            print("- {}: OK (no duplicates)".format(t))

    print_header("FOREIGN KEY COMPLETENESS (MySQL)")
    for (child, child_cols), (parent, parent_cols) in FK_MAP:
        orphans = fk_orphans(child, child_cols, parent, parent_cols)
        if orphans:
            print("- {} -> {}: ORPHANS found (up to {}): {}".format(child, parent, ANOMALY_LIMIT, orphans))
        else:
            print("- {} -> {}: OK".format(child, parent))

    print_header("DONE")
    print("Integrity check finished. Review mismatches and orphans above.")

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            cur.close(); mysql.close()
        except Exception:
            pass
