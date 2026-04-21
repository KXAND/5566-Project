#!/usr/bin/env python3
import argparse
import json
import re
import sqlite3
from pathlib import Path


def flatten_record(record, parent_key="", sep="__"):
    flat = {}
    for key, value in record.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            flat.update(flatten_record(value, new_key, sep=sep))
        elif isinstance(value, list):
            flat[new_key] = json.dumps(value, ensure_ascii=False)
        else:
            flat[new_key] = value
    return flat


def enrich_asset_columns(flat):
    assets_key = "flash_loan__assets"
    raw_assets = flat.get(assets_key)
    if not raw_assets:
        return

    try:
        assets = json.loads(raw_assets)
    except (TypeError, json.JSONDecodeError):
        return

    if not isinstance(assets, list) or not assets:
        return

    symbols = []
    token_addresses = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        sym = asset.get("symbol")
        addr = asset.get("token_address")
        if sym is not None:
            symbols.append(str(sym))
        if addr is not None:
            token_addresses.append(str(addr))

    if symbols:
        flat["flash_loan__asset_symbol"] = symbols[0]
        flat["flash_loan__asset_symbols"] = ",".join(symbols)
    if token_addresses:
        flat["flash_loan__asset_token_address"] = token_addresses[0]
        flat["flash_loan__asset_token_addresses"] = ",".join(token_addresses)


def sanitize_column_name(name):
    sanitized = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    if not sanitized:
        sanitized = "col"
    if sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    return sanitized


def infer_sqlite_type(values):
    has_text = False
    has_real = False
    has_int = False
    has_bool = False

    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            has_bool = True
        elif isinstance(value, int):
            has_int = True
        elif isinstance(value, float):
            has_real = True
        else:
            has_text = True

    if has_text:
        return "TEXT"
    if has_real:
        return "REAL"
    if has_int or has_bool:
        return "INTEGER"
    return "TEXT"


def normalize_value(value):
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def load_records(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict):
        raise ValueError("JSON 顶层必须是对象。")

    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError('JSON 中未找到 "records" 数组。')

    return records


def build_schema(flat_records):
    all_columns = []
    seen = set()
    for row in flat_records:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                all_columns.append(key)

    name_map = {}
    used = set()
    for original in all_columns:
        base = sanitize_column_name(original)
        candidate = base
        suffix = 1
        while candidate in used:
            suffix += 1
            candidate = f"{base}_{suffix}"
        used.add(candidate)
        name_map[original] = candidate

    column_types = {}
    for original in all_columns:
        values = [row.get(original) for row in flat_records]
        column_types[original] = infer_sqlite_type(values)

    filtered_columns = []
    for original in all_columns:
        if any(row.get(original) is not None for row in flat_records):
            filtered_columns.append(original)

    return filtered_columns, name_map, column_types


def import_to_sqlite(json_path, db_path, table_name):
    records = load_records(json_path)
    if not records:
        raise ValueError("records 数组为空，无法导入。")

    flat_records = []
    for r in records:
        row = flatten_record(r)
        enrich_asset_columns(row)
        flat_records.append(row)
    all_columns, name_map, column_types = build_schema(flat_records)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        quoted_table = f'"{table_name}"'
        cur.execute(f"DROP TABLE IF EXISTS {quoted_table}")

        column_defs = []
        for original in all_columns:
            sqlite_name = name_map[original]
            sqlite_type = column_types[original]
            column_defs.append(f'"{sqlite_name}" {sqlite_type}')

        create_sql = f"CREATE TABLE {quoted_table} ({', '.join(column_defs)})"
        cur.execute(create_sql)

        insert_columns = [name_map[c] for c in all_columns]
        placeholders = ", ".join(["?"] * len(insert_columns))
        quoted_columns = ", ".join([f'"{c}"' for c in insert_columns])
        insert_sql = f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"

        rows_to_insert = []
        for row in flat_records:
            values = [normalize_value(row.get(col)) for col in all_columns]
            rows_to_insert.append(values)

        cur.executemany(insert_sql, rows_to_insert)
        conn.commit()
    finally:
        conn.close()

    return len(records), len(all_columns)


def main():
    parser = argparse.ArgumentParser(
        description="将 JSON 的 records 数组逐条导入 SQLite，表头包含全部字段。"
    )
    parser.add_argument("json_file", help="输入 JSON 文件路径")
    parser.add_argument(
        "--db",
        dest="db_file",
        default=None,
        help="输出 SQLite 文件路径（默认与 JSON 同名 .sqlite）",
    )
    parser.add_argument(
        "--table",
        dest="table_name",
        default="transfer_records",
        help="目标表名（默认: transfer_records）",
    )
    args = parser.parse_args()

    json_path = Path(args.json_file).expanduser().resolve()
    if not json_path.exists():
        raise FileNotFoundError(f"找不到文件: {json_path}")

    db_path = (
        Path(args.db_file).expanduser().resolve()
        if args.db_file
        else json_path.with_suffix(".sqlite")
    )

    inserted, columns = import_to_sqlite(str(json_path), str(db_path), args.table_name)
    print(f"导入完成: {inserted} 条记录 -> {db_path}")
    print(f"表名: {args.table_name}，字段数: {columns}")


if __name__ == "__main__":
    main()