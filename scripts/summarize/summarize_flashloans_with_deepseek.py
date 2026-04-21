#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import socket
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


def log(message):
    print(message, flush=True)


def deepseek_chat(
    api_key,
    model,
    messages,
    temperature=0.2,
    timeout=45,
    retries=3,
    max_tokens=1200,
):
    url = "https://api.deepseek.com/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    last_error = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8")
                parsed = json.loads(body)
                return parsed["choices"][0]["message"]["content"].strip()
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
            socket.timeout,
            KeyError,
            json.JSONDecodeError,
        ) as exc:
            last_error = exc
            if attempt < retries:
                log(f"[retry] DeepSeek 请求失败，第 {attempt}/{retries} 次，准备重试: {exc}")
                time.sleep(1.5 * attempt)
            else:
                raise RuntimeError(f"DeepSeek API 调用失败: {last_error}") from last_error


def fetch_rows(db_path, table, limit=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        sql = f'SELECT * FROM "{table}"'
        if limit is not None:
            sql += " LIMIT ?"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def safe_json_loads(text):
    text = text.strip()
    if text.startswith("```"):
        parts = text.split("\n")
        if parts and parts[0].startswith("```"):
            parts = parts[1:]
        if parts and parts[-1].startswith("```"):
            parts = parts[:-1]
        text = "\n".join(parts).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start : end + 1]
    return json.loads(text)


def load_checkpoint(checkpoint_path, valid_tx_hashes=None):
    path = Path(checkpoint_path)
    if not path.exists():
        return []

    loaded = []
    seen_tx = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue

            tx = item.get("_tx_hash")
            if not tx or tx in seen_tx:
                continue
            if valid_tx_hashes is not None and tx not in valid_tx_hashes:
                continue

            seen_tx.add(tx)
            loaded.append(item)
    return loaded


def append_checkpoint(checkpoint_path, item):
    path = Path(checkpoint_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


def summarize_one_record(idx, row, api_key, model, request_timeout=45, sleep_seconds=0.0, verbose=True):
    tx = row.get("tx_hash", "unknown")
    if verbose:
        log(f"[record] {idx} start tx={tx}")

    prompt = (
        "你是一名区块链安全分析师。请对下面单条 flashloan 记录做提炼，"
        "输出严格 JSON（不要 markdown 代码块），字段必须包含:\n"
        "one_line_summary: string\n"
        "risk_level: one of [low, medium, high]\n"
        "reason: string\n"
        "key_asset_symbol: string or null\n"
        "suspicious_signals: array of string\n"
        "action_type: string\n"
        "\n记录如下：\n"
        + json.dumps(row, ensure_ascii=False)
    )
    messages = [
        {"role": "system", "content": "你输出必须是可解析 JSON。"},
        {"role": "user", "content": prompt},
    ]

    try:
        raw = deepseek_chat(
            api_key=api_key,
            model=model,
            messages=messages,
            temperature=0.1,
            timeout=request_timeout,
            max_tokens=380,
        )
        try:
            parsed = safe_json_loads(raw)
        except Exception:
            parsed = {
                "one_line_summary": raw,
                "risk_level": "medium",
                "reason": "模型输出非 JSON，降级保存原文。",
                "key_asset_symbol": row.get("flash_loan__asset_symbol"),
                "suspicious_signals": [],
                "action_type": "unknown",
            }
    except Exception as exc:
        parsed = {
            "one_line_summary": f"请求失败，已降级。error={exc}",
            "risk_level": "medium",
            "reason": f"API 调用失败，使用兜底结果。{exc}",
            "key_asset_symbol": row.get("flash_loan__asset_symbol"),
            "suspicious_signals": ["api_call_failed"],
            "action_type": "unknown",
        }

    parsed["_row_index"] = idx
    parsed["_tx_hash"] = row.get("tx_hash")

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    if verbose:
        log(f"[record] {idx} done")
    return parsed


def summarize_each_record(
    rows,
    api_key,
    model,
    sleep_seconds=0.1,
    request_timeout=45,
    existing_results=None,
    checkpoint_path=None,
    workers=1,
):
    results = list(existing_results) if existing_results else []
    processed_tx = {x.get("_tx_hash") for x in results if x.get("_tx_hash")}
    skipped = 0
    total = len(rows)

    pending = []
    for idx, row in enumerate(rows, start=1):
        tx = row.get("tx_hash", "unknown")
        if tx in processed_tx:
            skipped += 1
            continue
        pending.append((idx, row))

    if skipped:
        log(f"[resume] 已跳过 {skipped} 条已完成记录")

    if workers <= 1:
        for idx, row in pending:
            parsed = summarize_one_record(
                idx,
                row,
                api_key=api_key,
                model=model,
                request_timeout=request_timeout,
                sleep_seconds=sleep_seconds,
                verbose=True,
            )
            results.append(parsed)
            processed_tx.add(parsed["_tx_hash"])
            if checkpoint_path:
                append_checkpoint(checkpoint_path, parsed)
    else:
        lock = threading.Lock()
        done = 0
        total_pending = len(pending)
        log(f"[parallel] 启动并发分析: workers={workers}, pending={total_pending}")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(
                    summarize_one_record,
                    idx,
                    row,
                    api_key,
                    model,
                    request_timeout,
                    sleep_seconds,
                    False,
                ): (idx, row.get("tx_hash", "unknown"))
                for idx, row in pending
            }

            for future in as_completed(future_map):
                idx, tx = future_map[future]
                try:
                    parsed = future.result()
                except Exception as exc:
                    parsed = {
                        "_row_index": idx,
                        "_tx_hash": tx,
                        "one_line_summary": f"线程执行失败，已降级。error={exc}",
                        "risk_level": "medium",
                        "reason": f"线程执行异常，使用兜底结果。{exc}",
                        "key_asset_symbol": None,
                        "suspicious_signals": ["thread_failed"],
                        "action_type": "unknown",
                    }

                with lock:
                    results.append(parsed)
                    processed_tx.add(parsed["_tx_hash"])
                    if checkpoint_path:
                        append_checkpoint(checkpoint_path, parsed)
                    done += 1
                    if done % 20 == 0 or done == total_pending:
                        log(f"[parallel] 完成进度 {done}/{total_pending}")

    results.sort(key=lambda x: x.get("_row_index", 10**12))
    return results


def build_local_stats(rows, per_record):
    symbol_counter = Counter()
    provider_counter = Counter()
    risk_counter = Counter()

    for row in rows:
        symbol = row.get("flash_loan__asset_symbol") or "UNKNOWN"
        provider = row.get("flash_loan__provider") or "UNKNOWN"
        symbol_counter[symbol] += 1
        provider_counter[provider] += 1

    for item in per_record:
        risk_counter[item.get("risk_level", "unknown")] += 1

    return {
        "total_records": len(rows),
        "top_symbols": symbol_counter.most_common(10),
        "provider_distribution": provider_counter.most_common(10),
        "risk_distribution": risk_counter,
    }


def summarize_segments(per_record, api_key, model, segment_size=500, request_timeout=45):
    if segment_size <= 0:
        raise ValueError("segment_size 必须大于 0")

    segments = []
    total = len(per_record)
    if total == 0:
        return segments

    for seg_idx, start in enumerate(range(0, total, segment_size), start=1):
        chunk = per_record[start : start + segment_size]
        risk_counter = Counter()
        action_counter = Counter()
        symbol_counter = Counter()
        sample_lines = []

        for item in chunk:
            risk_counter[item.get("risk_level", "unknown")] += 1
            action_counter[item.get("action_type", "unknown")] += 1
            symbol_counter[item.get("key_asset_symbol") or "UNKNOWN"] += 1
            if len(sample_lines) < 12:
                sample_lines.append(item.get("one_line_summary") or "")

        segment_payload = {
            "segment_id": seg_idx,
            "start_index": start + 1,
            "end_index": start + len(chunk),
            "count": len(chunk),
            "risk_distribution": dict(risk_counter),
            "top_actions": action_counter.most_common(8),
            "top_symbols": symbol_counter.most_common(8),
            "sample_summaries": sample_lines,
        }

        prompt = (
            "你是链上风控分析师。请基于这一个分段的数据给出精炼总结，输出纯文本中文，包含:\n"
            "1) 分段结论\n"
            "2) 异常点\n"
            "3) 建议监控\n"
            "输入:\n"
            + json.dumps(segment_payload, ensure_ascii=False)
        )
        messages = [
            {"role": "system", "content": "你是专业的区块链审计与风控分析专家。"},
            {"role": "user", "content": prompt},
        ]

        log(f"[segment] 开始分段汇总 {seg_idx} ({start + 1}-{start + len(chunk)})")
        try:
            summary_text = deepseek_chat(
                api_key=api_key,
                model=model,
                messages=messages,
                temperature=0.2,
                timeout=request_timeout,
                max_tokens=520,
            )
        except RuntimeError as exc:
            summary_text = (
                f"分段汇总失败，已使用本地统计兜底。error={exc}\n"
                f"risk_distribution={dict(risk_counter)}\n"
                f"top_actions={action_counter.most_common(5)}\n"
                f"top_symbols={symbol_counter.most_common(5)}"
            )

        segments.append(
            {
                "segment_id": seg_idx,
                "start_index": start + 1,
                "end_index": start + len(chunk),
                "count": len(chunk),
                "risk_distribution": dict(risk_counter),
                "top_actions": action_counter.most_common(8),
                "top_symbols": symbol_counter.most_common(8),
                "summary": summary_text,
            }
        )

    return segments


def summarize_global(
    stats,
    per_record,
    api_key,
    model,
    request_timeout=45,
    sample_size=40,
    segment_summaries=None,
):
    def _build_payload(curr_sample_size, summary_len):
        sampled = per_record[: min(curr_sample_size, len(per_record))]
        slimmed = []
        for item in sampled:
            text = item.get("one_line_summary") or ""
            if isinstance(text, str) and len(text) > summary_len:
                text = text[:summary_len] + "..."
            slimmed.append(
                {
                    "tx_hash": item.get("_tx_hash"),
                    "risk_level": item.get("risk_level"),
                    "action_type": item.get("action_type"),
                    "key_asset_symbol": item.get("key_asset_symbol"),
                    "summary": text,
                    "suspicious_signals": item.get("suspicious_signals"),
                }
            )

        return {
            "stats": {
                "total_records": stats["total_records"],
                "top_symbols": stats["top_symbols"],
                "provider_distribution": stats["provider_distribution"],
                "risk_distribution": dict(stats["risk_distribution"]),
            },
            "segment_summaries": segment_summaries or [],
            "sampled_record_insights": slimmed,
        }

    # Progressively degrade input size if global summary requests time out.
    attempts = [
        (sample_size, 1200, 240),
        (max(10, min(sample_size, 25)), 900, 160),
        (10, 700, 120),
    ]

    last_error = None
    for curr_sample_size, max_tokens, summary_len in attempts:
        payload = _build_payload(curr_sample_size, summary_len)
        prompt = (
            "你是链上风控负责人。请基于给定统计与样本记录，给出 flashloan 汇总判断。"
            "输出纯文本中文，包含以下小节:\n"
            "1) 总体结论\n"
            "2) 主要模式\n"
            "3) 风险分层\n"
            "4) 可执行的监控规则（至少5条）\n"
            "5) 后续调查建议\n"
            "\n输入数据:\n"
            + json.dumps(payload, ensure_ascii=False)
        )
        messages = [
            {"role": "system", "content": "你是专业的区块链审计与风控分析专家。"},
            {"role": "user", "content": prompt},
        ]

        try:
            log(
                f"[global] 尝试汇总: sample={curr_sample_size}, max_tokens={max_tokens}, timeout={request_timeout}s"
            )
            return deepseek_chat(
                api_key=api_key,
                model=model,
                messages=messages,
                temperature=0.2,
                timeout=request_timeout,
                max_tokens=max_tokens,
            )
        except RuntimeError as exc:
            last_error = exc
            log(f"[global] 本次汇总失败，降级后重试: {exc}")

    # Fallback: never fail whole run just because global LLM call timed out.
    risk_dist = dict(stats["risk_distribution"])
    top_symbols = ", ".join([f"{k}:{v}" for k, v in stats["top_symbols"][:5]])
    providers = ", ".join([f"{k}:{v}" for k, v in stats["provider_distribution"][:5]])
    fallback = [
        "全局汇总调用 DeepSeek 多次超时，已启用本地统计兜底。",
        f"失败原因: {last_error}",
        f"总记录数: {stats['total_records']}",
        f"风险分布: {risk_dist}",
        f"主要资产: {top_symbols}",
        f"主要提供方: {providers}",
        "建议: 提高 --request-timeout 到 60-90，或降低 --global-sample-size 到 10-20 再重跑全局汇总。",
    ]
    return "\n".join(fallback)


def write_report(
    output_path,
    db_path,
    table,
    model,
    rows,
    per_record,
    stats,
    global_summary,
    segment_summaries=None,
    checkpoint_path=None,
):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = []
    lines.append("Flashloan LLM Analysis Report")
    lines.append("=" * 80)
    lines.append(f"Generated At: {now}")
    lines.append(f"Database: {db_path}")
    lines.append(f"Table: {table}")
    lines.append(f"Model: {model}")
    lines.append(f"Record Count: {len(rows)}")
    if checkpoint_path:
        lines.append(f"Checkpoint: {checkpoint_path}")
    lines.append("")

    lines.append("[Local Statistics]")
    lines.append(f"Top Symbols: {stats['top_symbols']}")
    lines.append(f"Provider Distribution: {stats['provider_distribution']}")
    lines.append(f"Risk Distribution: {dict(stats['risk_distribution'])}")
    lines.append("")

    if segment_summaries:
        lines.append("[Segment Summaries]")
        for seg in segment_summaries:
            lines.append("-" * 80)
            lines.append(
                f"Segment {seg.get('segment_id')} ({seg.get('start_index')}-{seg.get('end_index')}, count={seg.get('count')})"
            )
            lines.append(f"Risk Distribution: {seg.get('risk_distribution')}")
            lines.append(f"Top Actions: {seg.get('top_actions')}")
            lines.append(f"Top Symbols: {seg.get('top_symbols')}")
            lines.append("Summary:")
            lines.append(str(seg.get("summary")))
        lines.append("")

    lines.append("[Global Judgment]")
    lines.append(global_summary)
    lines.append("")

    lines.append("[Per-Record Summaries]")
    for item in per_record:
        lines.append("-" * 80)
        lines.append(f"Index: {item.get('_row_index')}")
        lines.append(f"TxHash: {item.get('_tx_hash')}")
        lines.append(f"Summary: {item.get('one_line_summary')}")
        lines.append(f"Risk: {item.get('risk_level')}")
        lines.append(f"Reason: {item.get('reason')}")
        lines.append(f"Key Asset: {item.get('key_asset_symbol')}")
        lines.append(f"Action Type: {item.get('action_type')}")
        lines.append(f"Suspicious Signals: {item.get('suspicious_signals')}")
    lines.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(
        description="读取 SQLite 的 flashloan 记录，调用 DeepSeek 进行逐条提炼与汇总判断，并输出文本报告。"
    )
    parser.add_argument("db_file", help="SQLite 文件路径")
    parser.add_argument("--table", default="transfer_records", help="表名，默认 transfer_records")
    parser.add_argument("--output", default="flashloan_llm_summary.txt", help="输出文本文件")
    parser.add_argument("--model", default="deepseek-chat", help="DeepSeek 模型名")
    parser.add_argument("--api-key", default=os.environ.get("DEEPSEEK_API_KEY"), help="DeepSeek API Key")
    parser.add_argument("--limit", type=int, default=None, help="仅处理前 N 条记录")
    parser.add_argument("--sleep", type=float, default=0.1, help="每条记录调用间隔秒数")
    parser.add_argument("--workers", type=int, default=1, help="逐条分析并发线程数，默认 1")
    parser.add_argument("--request-timeout", type=int, default=45, help="每次 API 请求超时秒数")
    parser.add_argument("--global-sample-size", type=int, default=40, help="全局汇总输入的样本条数")
    parser.add_argument("--segment-size", type=int, default=500, help="分段汇总每组记录数")
    parser.add_argument("--checkpoint-file", default=None, help="断点续跑文件路径")
    parser.add_argument("--no-resume", action="store_true", help="不读取历史 checkpoint，强制全量重跑")
    args = parser.parse_args()

    if not args.api_key:
        raise ValueError("请提供 DeepSeek API Key（--api-key 或环境变量 DEEPSEEK_API_KEY）。")

    rows = fetch_rows(args.db_file, args.table, args.limit)
    if not rows:
        raise ValueError("未读取到记录，请检查数据库路径和表名。")

    checkpoint_path = args.checkpoint_file or f"{args.output}.checkpoint.jsonl"
    valid_tx_hashes = {r.get("tx_hash") for r in rows if r.get("tx_hash")}
    existing_results = []
    if not args.no_resume:
        existing_results = load_checkpoint(checkpoint_path, valid_tx_hashes=valid_tx_hashes)
        if existing_results:
            log(f"[resume] 从 checkpoint 载入 {len(existing_results)} 条已完成记录")
    else:
        log("[resume] 已禁用断点续跑，执行全量重跑")

    log(f"[start] 读取到 {len(rows)} 条记录，开始逐条提炼")
    per_record = summarize_each_record(
        rows,
        api_key=args.api_key,
        model=args.model,
        sleep_seconds=args.sleep,
        request_timeout=args.request_timeout,
        existing_results=existing_results,
        checkpoint_path=checkpoint_path,
        workers=max(1, args.workers),
    )
    stats = build_local_stats(rows, per_record)
    log("[segment] 开始分段汇总")
    segment_summaries = summarize_segments(
        per_record,
        api_key=args.api_key,
        model=args.model,
        segment_size=args.segment_size,
        request_timeout=args.request_timeout,
    )
    log("[global] 开始全局汇总判断")
    global_summary = summarize_global(
        stats,
        per_record,
        api_key=args.api_key,
        model=args.model,
        request_timeout=args.request_timeout,
        sample_size=args.global_sample_size,
        segment_summaries=segment_summaries,
    )
    write_report(
        args.output,
        args.db_file,
        args.table,
        args.model,
        rows,
        per_record,
        stats,
        global_summary,
        segment_summaries=segment_summaries,
        checkpoint_path=checkpoint_path,
    )

    print(f"分析完成，共处理 {len(rows)} 条记录。")
    print(f"报告已写入: {args.output}")


if __name__ == "__main__":
    main()