#!/usr/bin/env python3
import argparse
import json
import re
import sqlite3
from pathlib import Path


def flatten_stats(stats_dict, parent_key="", sep="__"):
    """扁平化嵌套的统计JSON结构"""
    flat = {}
    for key, value in stats_dict.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            flat.update(flatten_stats(value, new_key, sep=sep))
        elif isinstance(value, list):
            flat[new_key] = json.dumps(value, ensure_ascii=False)
        else:
            flat[new_key] = value
    return flat


def sanitize_column_name(name):
    """清理列名"""
    sanitized = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    if not sanitized:
        sanitized = "col"
    if sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    return sanitized


def infer_sqlite_type(values):
    """推断SQLite数据类型"""
    has_text = False
    has_real = False
    has_int = False

    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            has_int = True
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
    if has_int:
        return "INTEGER"
    return "TEXT"


def normalize_value(value):
    """规范化值"""
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def import_stats_to_sqlite(json_path, db_path, table_name="stats"):
    """将统计JSON导入到SQLite
    
    支持两种格式：
    1. 含 'records' 数组的标准格式
    2. 直接的统计对象（如 by_protocol, token_analysis 等）
    """
    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict):
        raise ValueError("JSON 顶层必须是对象。")

    # 检查是否有 records 数组（兼容旧格式）
    if "records" in payload and isinstance(payload["records"], list):
        records = payload["records"]
    else:
        # 直接作为统计数据导入：为每个顶层键创建一条记录
        records = []
        for key, value in payload.items():
            if isinstance(value, dict):
                record = {"section": key}
                record.update(flatten_stats(value, sep="_"))
                records.append(record)
            elif isinstance(value, list):
                # 数组类型作为单独记录
                for idx, item in enumerate(value):
                    record = {"section": key, "index": idx}
                    if isinstance(item, dict):
                        record.update(flatten_stats(item, sep="_"))
                    else:
                        record["value"] = item
                    records.append(record)
            else:
                # 标量值作为单独记录
                records.append({
                    "section": key,
                    "value": value
                })

    if not records:
        raise ValueError("无法从JSON提取记录。")

    # 扁平化所有记录
    flat_records = []
    for r in records:
        if isinstance(r, dict):
            row = flatten_stats(r)
        else:
            row = {"data": str(r)}
        flat_records.append(row)

    # 构建schema
    all_columns = []
    seen = set()
    for row in flat_records:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                all_columns.append(key)

    # 映射列名并推断类型
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

    # 创建数据库
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        quoted_table = f'"{table_name}"'
        cur.execute(f"DROP TABLE IF EXISTS {quoted_table}")

        # 创建表
        column_defs = []
        for original in all_columns:
            sqlite_name = name_map[original]
            sqlite_type = column_types[original]
            column_defs.append(f'"{sqlite_name}" {sqlite_type}')

        create_sql = f"CREATE TABLE {quoted_table} ({', '.join(column_defs)})"
        cur.execute(create_sql)

        # 插入数据
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
        description="将统计 JSON 导入 SQLite。支持嵌套的统计数据结构。"
    )
    parser.add_argument("json_file", help="输入 JSON 文件路径")
    parser.add_argument(
        "--db",
        default=None,
        help="输出 SQLite 文件路径（默认：JSON 文件同目录，后缀改为 .sqlite）",
    )
    parser.add_argument("--table", default="stats", help="SQLite 表名")
    args = parser.parse_args()

    json_path = Path(args.json_file).expanduser().resolve()
    if not json_path.exists():
        raise FileNotFoundError(f"JSON 文件不存在: {json_path}")

    db_path = Path(args.db).expanduser().resolve() if args.db else json_path.with_suffix(".sqlite")

    print(f"导入中: {json_path.name} -> {db_path.name}")
    inserted, columns = import_stats_to_sqlite(str(json_path), str(db_path), args.table)
    print(f"成功: {inserted} 条记录，{columns} 个字段")
    print(f"数据库位置: {db_path}")


if __name__ == "__main__":
    main()
