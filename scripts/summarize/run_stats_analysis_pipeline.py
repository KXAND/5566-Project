#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

from scripts.summarize.import_stats_to_sqlite import import_stats_to_sqlite
from scripts.summarize.analyze_stats_with_deepseek import (
    analyze_stats,
    fetch_all_rows,
    log,
    write_analysis_report,
)


def run_stats_pipeline(
    json_file,
    table,
    model,
    request_timeout,
    db_path,
    output_path,
):
    """运行完整的统计分析流程"""
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise EnvironmentError("未检测到 DEEPSEEK_API_KEY，请先在环境变量中设置。")

    json_path = Path(json_file).expanduser().resolve()
    if not json_path.exists():
        raise FileNotFoundError(f"JSON 文件不存在: {json_path}")

    db_path = Path(db_path or json_path.with_suffix(".sqlite")).expanduser().resolve()
    output_path = Path(
        output_path or json_path.with_name(f"{json_path.stem}_llm_analysis.md")
    ).expanduser().resolve()

    # 步骤1: 导入JSON到SQLite
    print(f"[1/2] 导入 JSON -> SQLite: {db_path.name}")
    inserted, columns = import_stats_to_sqlite(str(json_path), str(db_path), table)
    print(f"导入完成: {inserted} 条记录, {columns} 个字段\n")

    # 步骤2: LLM分析
    print(f"[2/2] 读取表并调用 DeepSeek: {table}\n")
    analysis_result = analyze_stats(
        str(db_path),
        table,
        api_key=api_key,
        model=model,
        request_timeout=request_timeout,
    )

    write_analysis_report(str(output_path), str(db_path), table, model, analysis_result)

    print("\n完成。")
    print(f"SQLite: {db_path}")
    print(f"报告: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="一键导入统计 JSON 并用 DeepSeek 进行分析"
    )
    parser.add_argument("json_file", help="输入 JSON 文件路径 (统计数据)")
    parser.add_argument("--table", default="stats", help="SQLite 表名")
    parser.add_argument("--model", default="deepseek-chat", help="DeepSeek 模型名")
    parser.add_argument("--request-timeout", type=int, default=45, help="API 请求超时秒")
    parser.add_argument("--db", default=None, help="输出 SQLite 文件路径（可选）")
    parser.add_argument("--output", default=None, help="输出报告文件路径（可选）")
    args = parser.parse_args()

    run_stats_pipeline(
        json_file=args.json_file,
        table=args.table,
        model=args.model,
        request_timeout=args.request_timeout,
        db_path=args.db,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
