#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import socket
import time
import urllib.error
import urllib.request
from datetime import datetime
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
    max_tokens=2000,
):
    """调用DeepSeek API进行对话"""
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


def fetch_all_rows(db_path, table):
    """从SQLite读取所有数据"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        sql = f'SELECT * FROM "{table}"'
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def format_stats_for_llm(rows):
    """将数据格式化为LLM可读的格式 - 智能处理扁平化的统计数据"""
    if not rows:
        return "无数据"
    
    # 按section分组并收集有值的数据
    result = "闪电贷详细统计数据概览：\n\n"
    
    for row in rows:
        section = row.get("section", "数据")
        result += f"【{section}】\n"
        
        # 收集该行中所有有值的字段
        non_null_fields = {}
        for key, value in row.items():
            # 跳过metadata列和NULL值
            if key in ["section", "index"] or value is None:
                continue
            # 清理列名（移除section前缀中的下划线）
            clean_key = key.replace("_", " ").title() if "_" in key else key
            non_null_fields[key] = value
        
        if not non_null_fields:
            result += "  (无数据)\n"
        else:
            # 按字段排序并显示
            for key in sorted(non_null_fields.keys()):
                value = non_null_fields[key]
                # 格式化数值
                if isinstance(value, float):
                    formatted_val = f"{value:,.2f}" if value > 1000 else f"{value:.4f}"
                else:
                    formatted_val = str(value)
                result += f"  {key}: {formatted_val}\n"
        
        result += "\n"
    
    return result


def analyze_stats(
    db_path,
    table,
    api_key,
    model,
    request_timeout=45,
):
    """对统计数据进行LLM分析"""
    log(f"[load] 从 SQLite 加载数据: {db_path}")
    rows = fetch_all_rows(db_path, table)
    
    if not rows:
        raise ValueError(f"表 {table} 中无数据")
    
    log(f"[format] 格式化 {len(rows)} 条记录")
    formatted_data = format_stats_for_llm(rows)
    
    # 分析1: 总体摘要
    log("[llm-1/3] 生成总体摘要...")
    summary_prompt = f"""请分析以下flashloan统计数据，生成一份简洁的中文总结（控制在300字以内）：

{formatted_data}

请从以下几个角度分析：
1. 整体规模和趋势
2. 主要协议和代币
3. 风险因素或异常情况"""
    
    overall_summary = deepseek_chat(
        api_key=api_key,
        model=model,
        messages=[{"role": "user", "content": summary_prompt}],
        temperature=0.2,
        timeout=request_timeout,
        max_tokens=800,
    )
    
    # 分析2: 关键洞察
    log("[llm-2/3] 生成关键洞察...")
    insight_prompt = f"""基于以下flashloan统计数据，提出5-7个关键洞察或发现：

{formatted_data}

请用简洁的列表格式，每条洞察不超过50字。"""
    
    key_insights = deepseek_chat(
        api_key=api_key,
        model=model,
        messages=[{"role": "user", "content": insight_prompt}],
        temperature=0.2,
        timeout=request_timeout,
        max_tokens=800,
    )
    
    # 分析3: 建议
    log("[llm-3/3] 生成建议...")
    recommendation_prompt = f"""基于以下flashloan统计数据，提出针对区块链/DeFi安全或交易策略的建议（3-5条）：

{formatted_data}

请用简洁的列表格式。"""
    
    recommendations = deepseek_chat(
        api_key=api_key,
        model=model,
        messages=[{"role": "user", "content": recommendation_prompt}],
        temperature=0.2,
        timeout=request_timeout,
        max_tokens=600,
    )
    
    return {
        "overall_summary": overall_summary,
        "key_insights": key_insights,
        "recommendations": recommendations,
        "total_records": len(rows),
        "data_sample": formatted_data,  # 完整的数据样本
    }


def write_analysis_report(output_path, db_path, table, model, analysis_result):
    """写入分析报告"""
    report = f"""# Flashloan 统计数据 LLM 分析报告

**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**数据源**: {Path(db_path).name}
**数据表**: {table}
**使用模型**: {model}
**记录数**: {analysis_result['total_records']}

---

## 总体摘要

{analysis_result['overall_summary']}

---

## 关键洞察

{analysis_result['key_insights']}

---

## 建议

{analysis_result['recommendations']}

---

## 附录：数据样本

```
{analysis_result['data_sample']}
...
```
"""
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report)
    
    log(f"报告已写入: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="读取SQLite统计数据，使用DeepSeek LLM进行分析并生成报告"
    )
    parser.add_argument("db_file", help="SQLite 数据库文件路径")
    parser.add_argument("--table", default="stats", help="要分析的表名（默认: stats）")
    parser.add_argument("--model", default="deepseek-chat", help="DeepSeek 模型名")
    parser.add_argument("--output", default=None, help="输出报告文件路径")
    parser.add_argument("--request-timeout", type=int, default=45, help="API 请求超时秒")
    args = parser.parse_args()

    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise EnvironmentError("未检测到 DEEPSEEK_API_KEY，请先在环境变量中设置")

    db_path = Path(args.db_file).expanduser().resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"数据库文件不存在: {db_path}")

    output_path = Path(args.output).expanduser().resolve() if args.output else db_path.with_name(f"{db_path.stem}_llm_analysis.md")

    log(f"[start] 开始分析: {db_path.name}")
    analysis_result = analyze_stats(
        str(db_path),
        args.table,
        api_key=api_key,
        model=args.model,
        request_timeout=args.request_timeout,
    )

    write_analysis_report(str(output_path), str(db_path), args.table, args.model, analysis_result)
    log("完成。")
    log(f"报告: {output_path}")


if __name__ == "__main__":
    main()
