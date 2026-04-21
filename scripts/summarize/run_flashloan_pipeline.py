#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

from import_records_to_sqlite import import_to_sqlite
from summarize_flashloans_with_deepseek import (
    build_local_stats,
    fetch_rows,
    load_checkpoint,
    log,
    summarize_each_record,
    summarize_segments,
    summarize_global,
    write_report,
)


def choose_json_file(start_dir):
    # Prefer GUI picker on macOS/desktop, fallback to numbered CLI selection.
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        selected = filedialog.askopenfilename(
            title="选择要导入并分析的 JSON 文件",
            initialdir=str(start_dir),
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        root.destroy()
        if selected:
            return Path(selected).expanduser().resolve()
    except Exception:
        pass

    json_files = sorted(start_dir.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"目录中未找到 JSON 文件: {start_dir}")

    print("可选 JSON 文件:")
    for idx, file_path in enumerate(json_files, start=1):
        print(f"  {idx}. {file_path.name}")

    while True:
        choice = input("请输入编号选择文件: ").strip()
        if choice.isdigit():
            i = int(choice)
            if 1 <= i <= len(json_files):
                return json_files[i - 1].resolve()
        print("输入无效，请重新输入编号。")


def run_pipeline(
    json_file,
    table,
    model,
    sleep_seconds,
    workers,
    request_timeout,
    global_sample_size,
    segment_size,
    checkpoint_file,
    no_resume,
    limit,
):
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise EnvironmentError("未检测到 DEEPSEEK_API_KEY，请先在环境变量中设置。")

    json_path = Path(json_file).expanduser().resolve()
    if not json_path.exists():
        raise FileNotFoundError(f"JSON 文件不存在: {json_path}")

    db_path = json_path.with_suffix(".sqlite")
    output_path = json_path.with_name(f"{json_path.stem}_llm_summary.txt")

    print(f"[1/3] 导入 JSON -> SQLite: {db_path.name}")
    inserted, columns = import_to_sqlite(str(json_path), str(db_path), table)
    print(f"导入完成: {inserted} 条记录, {columns} 个字段")

    print(f"[2/3] 读取表并调用 DeepSeek: {table}")
    rows = fetch_rows(str(db_path), table, limit)
    if not rows:
        raise ValueError("导入后未读取到记录，请检查数据。")

    checkpoint_path = checkpoint_file or f"{output_path}.checkpoint.jsonl"
    valid_tx_hashes = {r.get("tx_hash") for r in rows if r.get("tx_hash")}
    existing_results = []
    if not no_resume:
        existing_results = load_checkpoint(checkpoint_path, valid_tx_hashes=valid_tx_hashes)
        if existing_results:
            log(f"[resume] 从 checkpoint 载入 {len(existing_results)} 条已完成记录")
    else:
        log("[resume] 已禁用断点续跑，执行全量重跑")

    log(f"[start] 读取到 {len(rows)} 条记录，开始逐条提炼")
    per_record = summarize_each_record(
        rows,
        api_key=api_key,
        model=model,
        sleep_seconds=sleep_seconds,
        request_timeout=request_timeout,
        existing_results=existing_results,
        checkpoint_path=checkpoint_path,
        workers=max(1, workers),
    )
    stats = build_local_stats(rows, per_record)
    log("[segment] 开始分段汇总")
    segment_summaries = summarize_segments(
        per_record,
        api_key=api_key,
        model=model,
        segment_size=segment_size,
        request_timeout=request_timeout,
    )
    log("[global] 开始全局汇总判断")
    global_summary = summarize_global(
        stats,
        per_record,
        api_key=api_key,
        model=model,
        request_timeout=request_timeout,
        sample_size=global_sample_size,
        segment_summaries=segment_summaries,
    )

    print(f"[3/3] 写出报告: {output_path.name}")
    write_report(
        str(output_path),
        str(db_path),
        table,
        model,
        rows,
        per_record,
        stats,
        global_summary,
        segment_summaries=segment_summaries,
        checkpoint_path=checkpoint_path,
    )

    print("完成。")
    print(f"SQLite: {db_path}")
    print(f"报告: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="交互选择 JSON 文件，一键导入 SQLite 并调用 DeepSeek 做 flashloan 分析。"
    )
    parser.add_argument(
        "--json",
        default=None,
        help="可选：直接指定 JSON 文件路径；不传则弹窗/命令行选择。",
    )
    parser.add_argument("--table", default="transfer_records", help="SQLite 表名")
    parser.add_argument("--model", default="deepseek-chat", help="DeepSeek 模型名")
    parser.add_argument("--sleep", type=float, default=0.1, help="每条调用间隔秒")
    parser.add_argument("--workers", type=int, default=1, help="逐条分析并发线程数，默认 1")
    parser.add_argument("--request-timeout", type=int, default=45, help="API 请求超时秒")
    parser.add_argument("--global-sample-size", type=int, default=40, help="全局汇总样本条数")
    parser.add_argument("--segment-size", type=int, default=500, help="分段汇总每组记录数")
    parser.add_argument("--checkpoint-file", default=None, help="断点续跑文件路径")
    parser.add_argument("--no-resume", action="store_true", help="不读取 checkpoint，强制全量重跑")
    parser.add_argument("--limit", type=int, default=None, help="仅处理前 N 条记录（调试用）")
    args = parser.parse_args()

    start_dir = Path.cwd()
    json_file = Path(args.json).expanduser().resolve() if args.json else choose_json_file(start_dir)

    run_pipeline(
        json_file=json_file,
        table=args.table,
        model=args.model,
        sleep_seconds=args.sleep,
        workers=args.workers,
        request_timeout=args.request_timeout,
        global_sample_size=args.global_sample_size,
        segment_size=args.segment_size,
        checkpoint_file=args.checkpoint_file,
        no_resume=args.no_resume,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()