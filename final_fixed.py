
import sqlite3
import os
import csv

# ========= Configuration =========
missed_db_path = "missed_gmc.sqlite"
sp_db_path      = "sp_doctors.sqlite"
gp_db_path      = "gp_doctors.sqlite"

output_db_path  = "combined_gmc.sqlite"
output_csv_path = "combined_gmc_full.csv"
# =================================

def get_columns(conn, db_alias, table):
    """
    Return ordered list of column names for db_alias.table.
    Uses a zero-row SELECT to read cursor.description reliably.
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {db_alias}.{table} LIMIT 0;")
    return [d[0] for d in (cur.description or [])]

def list_tables(conn, db_alias):
    cur = conn.cursor()
    cur.execute(f"SELECT name FROM {db_alias}.sqlite_master WHERE type='table' ORDER BY name;")
    return [r[0] for r in cur.fetchall()]

def find_table_with_gmc_number(conn, db_alias):
    for t in list_tables(conn, db_alias):
        cols = get_columns(conn, db_alias, t)
        if "GMC_Number" in cols:
            return t, cols
    raise RuntimeError(f"No table with column 'GMC_Number' found in {db_alias}.")

def union_columns(*column_lists):
    """
    Build ordered union of columns across sources.
    - Ensure 'GMC_Number' is first.
    - Preserve relative order: first-seen order for non-GMC columns.
    """
    seen = set()
    ordered = []
    # First guarantee GMC_Number first (if present anywhere)
    ordered.append("GMC_Number")
    seen.add("GMC_Number")
    for cols in column_lists:
        for c in cols:
            if c not in seen and c != "GMC_Number":
                seen.add(c)
                ordered.append(c)
    return ordered

def create_output_table(conn, columns):
    """
    Create unified output table with GMC_Number as PRIMARY KEY.
    All columns are stored as TEXT for simplicity (avoids type mismatch).
    Adds provenance columns: source_db, source_table.
    """
    cur = conn.cursor()
    cols_def = ["GMC_Number TEXT PRIMARY KEY"]
    for c in columns:
        if c == "GMC_Number":
            continue
        cols_def.append(f'"{c}" TEXT')
    cols_def += ['source_db TEXT', 'source_table TEXT']
    ddl = f"CREATE TABLE IF NOT EXISTS combined_gmc_full ({', '.join(cols_def)});"
    cur.execute(ddl)
    conn.commit()

def build_insert_sql(target_columns, src_alias, src_table, src_columns, source_db_label):
    """
    Build INSERT ... SELECT that:
      - Normalizes GMC_Number
      - Maps existing columns; fills missing ones as NULL
      - Adds provenance
      - Inserts only rows whose GMC_Number not already present
    """
    select_exprs = []
    # GMC_Number normalized
    select_exprs.append(f"UPPER(TRIM(s.GMC_Number)) AS GMC_Number")
    # other columns aligned to target schema
    for c in target_columns:
        if c == "GMC_Number":
            continue
        if c in src_columns:
            select_exprs.append(f"s.\"{c}\" AS \"{c}\"")
        else:
            select_exprs.append(f"NULL AS \"{c}\"")
    # provenance
    select_exprs.append(f"'{source_db_label}' AS source_db")
    select_exprs.append(f"'{src_table}' AS source_table")

    insert_cols = ['"GMC_Number"'] + [f'"{c}"' for c in target_columns if c != "GMC_Number"] + ["source_db", "source_table"]
    sql = f"""
        INSERT INTO combined_gmc_full ({', '.join(insert_cols)})
        SELECT {', '.join(select_exprs)}
        FROM {src_alias}."{src_table}" AS s
        WHERE UPPER(TRIM(s.GMC_Number)) NOT IN (
            SELECT GMC_Number FROM combined_gmc_full
        );
    """
    return sql

def export_csv(conn, csv_path, target_columns):
    cur = conn.cursor()
    # Include provenance in CSV
    select_cols = ['"GMC_Number"'] + [f'"{c}"' for c in target_columns if c != "GMC_Number"] + ["source_db", "source_table"]
    cur.execute(f"SELECT {', '.join(select_cols)} FROM combined_gmc_full ORDER BY GMC_Number;")
    rows = cur.fetchall()

    headers = ["GMC_Number"] + [c for c in target_columns if c != "GMC_Number"] + ["source_db", "source_table"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    return len(rows)

def main():
    # Prepare output directory
    out_dir = os.path.dirname(output_db_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    # Open output DB and attach sources
    out_conn = sqlite3.connect(output_db_path)
    cur = out_conn.cursor()
    cur.execute(f"ATTACH DATABASE '{missed_db_path}' AS missed")
    cur.execute(f"ATTACH DATABASE '{sp_db_path}' AS spdb")
    cur.execute(f"ATTACH DATABASE '{gp_db_path}' AS gpdb")

    # Identify source tables + columns
    missed_table, missed_cols = find_table_with_gmc_number(out_conn, "missed")
    sp_table, sp_cols         = find_table_with_gmc_number(out_conn, "spdb")
    gp_table, gp_cols         = find_table_with_gmc_number(out_conn, "gpdb")

    # Build union schema (GMC_Number first)
    target_columns = union_columns(missed_cols, sp_cols, gp_cols)

    # Create output table
    create_output_table(out_conn, target_columns)

    # 1) Insert from missed_gmc.sqlite
    sql1 = build_insert_sql(target_columns, "missed", missed_table, missed_cols, os.path.basename(missed_db_path))
    # 2) Then insert missing from sp_doctors.sqlite
    sql2 = build_insert_sql(target_columns, "spdb",   sp_table,     sp_cols,     os.path.basename(sp_db_path))
    # 3) Then insert missing from gp_doctors.sqlite
    sql3 = build_insert_sql(target_columns, "gpdb",   gp_table,     gp_cols,     os.path.basename(gp_db_path))

    cur.execute("BEGIN")
    cur.execute(sql1)
    cur.execute(sql2)
    cur.execute(sql3)
    out_conn.commit()

    # Export CSV with all columns
    total_rows = export_csv(out_conn, output_csv_path, target_columns)

    # Detach & close
    cur.execute("DETACH DATABASE missed")
    cur.execute("DETACH DATABASE spdb")
    cur.execute("DETACH DATABASE gpdb")
    out_conn.close()

    print("✅ Merge complete.")
    print(f"   • Output DB:  {output_db_path}")
    print(f"   • Output CSV: {output_csv_path}")
    print(f"   • Total rows: {total_rows}")
    print(f"   • Source tables: missed.{missed_table}, spdb.{sp_table}, gpdb.{gp_table}")
    print("   • Schema columns:", ["GMC_Number"] + [c for c in target_columns if c != "GMC_Number"] + ["source_db", "source_table"])

if __name__ == "__main__":
    main()
