import json
from collections import defaultdict, Counter
from itertools import combinations

FILENAME = "app.json"

TOP_ARRAY_KEYS = [
    "time_slot","classroom","department","course","instructor","section",
    "teaches","student","takes","advisor","prereq"
]

POSSIBLE_ID_FIELDS = ["id", "ID", "_id", "s_ID"]  # order = preference

# ---------- helpers ----------
def jtype(v):
    if v is None: return "null"
    if isinstance(v, bool): return "bool"
    if isinstance(v, int) and not isinstance(v, bool): return "int"
    if isinstance(v, float): return "float"
    if isinstance(v, str): return "str"
    if isinstance(v, list): return "array"
    if isinstance(v, dict): return "object"
    return type(v).__name__

def merge_type_stats(stats, v):
    t = jtype(v)
    stats["types"][t] += 1
    if v is None:
        stats["nulls"] += 1
    if t == "str":
        stats["max_len"] = max(stats["max_len"], len(v))
    if t == "array":
        stats["has_array"] = True
    if t == "object":
        stats["has_object"] = True

def guess_mysql_type(field_stats):
    """Very simple mapping. Tune as needed."""
    tcounts = field_stats["types"]
    # if it ever appears as object/array, we will split to child table anyway
    # Pick dominant scalar type (ignores null)
    scalars = {k:v for k,v in tcounts.items() if k in {"bool","int","float","str"}}
    if not scalars:
        return "JSON"  # fallback
    dom = max(scalars, key=scalars.get)
    if dom == "bool":
        return "TINYINT(1)"
    if dom == "int":
        return "BIGINT"  # safer default
    if dom == "float":
        return "DOUBLE"
    if dom == "str":
        ml = max(1, field_stats["max_len"])
        if ml <= 255: return f"VARCHAR({max(8, ml)})"
        if ml <= 4000: return "TEXT"
        return "LONGTEXT"
    return "TEXT"

def normalize_name(name):
    return name.strip().replace(" ", "_").replace("-", "_")

def is_unique(records, field):
    seen = set()
    for r in records:
        if field not in r or r[field] is None:
            return False
        v = r[field]
        if v in seen:
            return False
        seen.add(v)
    return True

def candidate_pk(records, table_name):
    # Prefer common id field names
    for f in POSSIBLE_ID_FIELDS + [f"{table_name}_id", f"{table_name}Id"]:
        if all(isinstance(r, dict) for r in records) and any(f in r for r in records):
            if is_unique(records, f):
                return f, False  # existing field, not surrogate
    # As a last resort, try single-field uniqueness scan over small tables
    fields = set()
    for r in records:
        fields.update(r.keys())
        if len(fields) > 50: break  # avoid too wide scans
    for f in fields:
        if is_unique(records, f):
            return f, False
    # Give up: use surrogate
    return f"{table_name}_pk", True

# ---------- main analysis ----------
def analyze_tables(doc):
    """
    Returns:
      tables = {
        table_name: {
          "records": [...],
          "fields": {col: stats},
          "pk": (name, is_surrogate),
          "children": { child_table_name: {"path": field_name, "kind": "array"|"object", "samples": [...] } }
        }
      }
    """
    tables = {}

    if isinstance(doc, dict):
        for key in TOP_ARRAY_KEYS:
            if isinstance(doc.get(key), list):
                tname = normalize_name(key)
                tables[tname] = {"records": doc[key]}
    elif isinstance(doc, list):
        # assume the top-level list itself is the main table (unnamed)
        tables["root"] = {"records": doc}
    else:
        raise ValueError("Unsupported JSON shape")

    # profile fields
    for tname, tinfo in tables.items():
        recs = [r for r in tinfo["records"] if isinstance(r, dict)]
        fields = defaultdict(lambda: {"types": Counter(), "nulls": 0, "max_len": 0, "has_array": False, "has_object": False})
        children = {}  # child tables by nested field
        for r in recs:
            for k, v in r.items():
                k2 = normalize_name(k)
                merge_type_stats(fields[k2], v)
                if jtype(v) == "array":
                    children[f"{tname}_{k2}_items"] = {"path": k2, "kind": "array"}
                elif jtype(v) == "object":
                    children[f"{tname}_{k2}"] = {"path": k2, "kind": "object"}

        # detect PK
        pk_name, is_sur = candidate_pk(recs, tname)

        tinfo["fields"] = fields
        tinfo["pk"] = (normalize_name(pk_name), is_sur)
        tinfo["children"] = children

    return tables



# ---------- run ----------
def main():
    with open(FILENAME, "r", encoding="utf-8") as f:
        doc = json.load(f)

    tables = analyze_tables(doc)

    # Report summary
    print(f"Detected {len(tables)} top-level table(s): {', '.join(tables.keys())}")
    for tname, tinfo in tables.items():
        pk, is_sur = tinfo["pk"]
        print(f"\n=== TABLE: {tname} ===")
        print(f"Records: {len(tinfo['records'])}")
        print(f"PK: {pk} ({'surrogate' if is_sur else 'from data'})")
        print("Columns:")
        for col, stats in sorted(tinfo["fields"].items()):
            tdesc = ", ".join(f"{k}:{v}" for k,v in stats["types"].most_common())
            print(f"  - {col}: {tdesc}, nulls={stats['nulls']}, max_str_len={stats['max_len']}, nested_array={stats['has_array']}, nested_object={stats['has_object']}")
        if tinfo["children"]:
            print("Nested -> child tables:")
            for cname, meta in tinfo["children"].items():
                print(f"  * {cname}  (from field '{meta['path']}', kind={meta['kind']})")

if __name__ == "__main__":
    main()
