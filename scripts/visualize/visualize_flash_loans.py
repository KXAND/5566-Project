import argparse
import json
import os
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple

import matplotlib.pyplot as plt


DEFAULT_AAVE_JSON = r"output/flash_loans_20260408_230617.json"
DEFAULT_BALANCER_JSON = r"output/balancer_events_20260414_174657.json"
DEFAULT_STATS_JSON = r"output/flashloan_stats.json"
DEFAULT_DETAILED_JSON = r"output/flashloan_detailed_stats.json"
DEFAULT_OUT_DIR = r"output/charts"
DEFAULT_AAVE_TOTAL_TX_IN_RANGE = 66784821


plt.style.use("seaborn-v0_8-whitegrid")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate supported flash loan charts from project outputs.")
    parser.add_argument("--aave-json", default=DEFAULT_AAVE_JSON)
    parser.add_argument("--balancer-json", default=DEFAULT_BALANCER_JSON)
    parser.add_argument("--stats-json", default=DEFAULT_STATS_JSON)
    parser.add_argument("--detailed-json", default=DEFAULT_DETAILED_JSON)
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--aave-total-tx-in-range", type=int, default=DEFAULT_AAVE_TOTAL_TX_IN_RANGE)
    return parser.parse_args()


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _save_fig(path: str) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=160, bbox_inches="tight")
    plt.close()


def _format_value(value: float, style: str) -> str:
    if style == "int":
        return f"{int(round(value)):,}"
    return f"{value:,.2f}"


def _bar_with_labels(
    labels: List[str],
    values: List[float],
    title: str,
    ylabel: str,
    out_path: str,
    rotate: int = 25,
    label_style: str = "float",
    show_top_share_inside: int = 0,
) -> None:
    plt.figure(figsize=(11.5, 6))
    bars = plt.bar(labels, values, color="#3b82f6")
    plt.title(title, fontsize=14, weight="bold")
    plt.ylabel(ylabel)
    plt.xticks(rotation=rotate, ha="right")

    total = sum(values)
    for idx, (bar, value) in enumerate(zip(bars, values)):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            _format_value(value, label_style),
            ha="center",
            va="bottom",
            fontsize=8,
        )

        if show_top_share_inside > 0 and idx < show_top_share_inside and total > 0 and value > 0:
            share = value / total * 100.0
            plt.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() * 0.55,
                f"{share:.1f}%",
                ha="center",
                va="center",
                fontsize=9,
                color="white",
                fontweight="bold",
            )

    _save_fig(out_path)


def _pie_chart(labels: List[str], values: List[float], title: str, out_path: str) -> None:
    paired = [(label, float(value)) for label, value in zip(labels, values) if float(value) > 0]
    if not paired:
        return

    total = sum(value for _, value in paired)
    major = []
    other_value = 0.0
    for label, value in paired:
        share = value / total * 100.0 if total > 0 else 0.0
        if share < 3.0:
            other_value += value
        else:
            major.append((label, value))

    if other_value > 0:
        major.append(("Other", other_value))

    pie_labels = [label for label, _ in major]
    pie_values = [value for _, value in major]

    fig, ax = plt.subplots(figsize=(9.5, 6.5))
    wedges, _texts, _autotexts = ax.pie(
        pie_values,
        labels=None,
        autopct=lambda pct: f"{pct:.1f}%" if pct >= 4 else "",
        startangle=90,
        pctdistance=0.72,
    )
    ax.set_title(title, fontsize=14, weight="bold")
    ax.legend(
        wedges,
        pie_labels,
        title="Tokens",
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=False,
    )
    _save_fig(out_path)


def draw_protocol_transaction_count(detailed: Dict[str, Any], out_dir: str) -> bool:
    by_protocol = detailed.get("by_protocol") or {}
    if not by_protocol:
        return False
    labels = list(by_protocol.keys())
    values = [float(by_protocol[k].get("count", 0)) for k in labels]
    _bar_with_labels(
        labels,
        values,
        "Flash Loan Transaction Count by Protocol",
        "Transaction Count",
        os.path.join(out_dir, "protocol_transaction_count.png"),
        label_style="int",
        show_top_share_inside=4,
    )
    return True


def draw_protocol_share(detailed: Dict[str, Any], out_dir: str) -> bool:
    by_protocol = detailed.get("by_protocol") or {}
    if not by_protocol:
        return False
    labels = list(by_protocol.keys())
    values = [float(by_protocol[k].get("count", 0)) for k in labels]
    _pie_chart(labels, values, "Protocol Share of Collected Flash Loan Transactions", os.path.join(out_dir, "protocol_transaction_share.png"))
    return True


def draw_aave_flashloan_share(aave_data: Dict[str, Any], total_tx: int, out_dir: str) -> bool:
    record_count = int(aave_data.get("record_count") or 0)
    if record_count <= 0 or total_tx <= 0:
        return False
    other = max(total_tx - record_count, 0)
    plt.figure(figsize=(8.5, 5.8))
    labels = ["Aave Flash Loan Tx", "All Other Tx in Range"]
    values = [record_count, other]
    plt.bar(labels, values, color=["#2563eb", "#cbd5e1"])
    plt.title("Aave Flash Loan Share in Covered Block Range", fontsize=14, weight="bold")
    plt.ylabel("Transaction Count")
    share = record_count / total_tx * 100
    plt.text(0, record_count, f"{record_count:,}\n({share:.4f}%)", ha="center", va="bottom", fontsize=10)
    plt.text(1, other, f"{other:,}", ha="center", va="bottom", fontsize=10)
    _save_fig(os.path.join(out_dir, "aave_flashloan_share_in_range.png"))
    return True


def draw_protocol_by_block_range(stats_data: List[Dict[str, Any]], out_dir: str) -> bool:
    if not stats_data:
        return False
    ranges = [x.get("block_range", "unknown") for x in stats_data if x.get("total", 0) > 0]
    if not ranges:
        return False
    protocols = sorted({p for row in stats_data for p in (row.get("breakdown") or {}).keys()})
    x = list(range(len(ranges)))
    width = 0.25 if len(protocols) <= 3 else 0.18
    plt.figure(figsize=(12, 6))
    for idx, proto in enumerate(protocols):
        vals = []
        for row in stats_data:
            if row.get("total", 0) > 0:
                vals.append(float((row.get("breakdown") or {}).get(proto, 0)))
        offsets = [i + (idx - (len(protocols) - 1) / 2) * width for i in x]
        plt.bar(offsets, vals, width=width, label=proto)
    plt.xticks(x, ranges, rotation=15)
    plt.ylabel("Transaction Count")
    plt.title("Protocol Comparison by Block Range", fontsize=14, weight="bold")
    plt.legend()
    _save_fig(os.path.join(out_dir, "protocol_by_block_range.png"))
    return True


def draw_protocol_daily_trend(detailed: Dict[str, Any], out_dir: str) -> bool:
    time_dist = detailed.get("time_distribution") or {}
    if not time_dist:
        return False

    dates = sorted(time_dist.keys())
    protocols = sorted({p for counts in time_dist.values() for p in counts.keys()})

    plt.figure(figsize=(12, 6))
    x = list(range(len(dates)))
    for proto in protocols:
        vals = []
        for d in dates:
            counts = time_dist[d] or {}
            vals.append(float(counts[proto]) if proto in counts else None)
        plt.plot(x, vals, marker="o", linewidth=1.2, label=proto)

    step = max(1, len(dates) // 10)
    tick_idx = list(range(0, len(dates), step))
    if tick_idx[-1] != len(dates) - 1:
        tick_idx.append(len(dates) - 1)
    tick_labels = [dates[i] for i in tick_idx]

    plt.title("Daily Flash Loan Trend by Protocol", fontsize=14, weight="bold")
    plt.ylabel("Transaction Count")
    plt.xticks(tick_idx, tick_labels, rotation=30, ha="right")
    plt.legend()
    _save_fig(os.path.join(out_dir, "protocol_daily_trend.png"))
    return True
def _aave_token_rows(aave_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return aave_data.get("token_summary") or []


def draw_aave_top_tokens(aave_data: Dict[str, Any], out_dir: str, top_n: int) -> List[str]:
    generated = []
    rows = _aave_token_rows(aave_data)
    if not rows:
        return generated

    by_count = sorted(rows, key=lambda x: int(x.get("transaction_count") or 0), reverse=True)[:top_n]
    labels = [r.get("symbol") or "UNKNOWN" for r in by_count]
    values = [float(r.get("transaction_count") or 0) for r in by_count]
    _bar_with_labels(
        labels,
        values,
        "Aave Top Tokens by Borrow Count",
        "Borrow Count",
        os.path.join(out_dir, "aave_top_tokens_by_count.png"),
        label_style="int",
        show_top_share_inside=4,
    )
    generated.append("aave_top_tokens_by_count.png")
    _pie_chart(labels, values, "Aave Token Share by Borrow Count", os.path.join(out_dir, "aave_top_tokens_by_count_share.png"))
    generated.append("aave_top_tokens_by_count_share.png")

    by_amount = sorted(rows, key=lambda x: _to_decimal(x.get("total_amount")), reverse=True)[:top_n]
    labels = [r.get("symbol") or "UNKNOWN" for r in by_amount]
    values = [float(_to_decimal(r.get("total_amount"))) for r in by_amount]
    _bar_with_labels(
        labels,
        values,
        "Aave Top Tokens by Total Borrowed Amount",
        "Total Amount",
        os.path.join(out_dir, "aave_top_tokens_by_amount.png"),
        show_top_share_inside=4,
    )
    generated.append("aave_top_tokens_by_amount.png")
    _pie_chart(labels, values, "Aave Token Share by Borrowed Amount", os.path.join(out_dir, "aave_top_tokens_by_amount_share.png"))
    generated.append("aave_top_tokens_by_amount_share.png")

    by_fee = sorted(rows, key=lambda x: _to_decimal(x.get("total_fee")), reverse=True)[:top_n]
    labels = [r.get("symbol") or "UNKNOWN" for r in by_fee]
    values = [float(_to_decimal(r.get("total_fee"))) for r in by_fee]
    _bar_with_labels(labels, values, "Aave Top Tokens by Total Fee", "Total Fee", os.path.join(out_dir, "aave_top_tokens_by_fee.png"))
    generated.append("aave_top_tokens_by_fee.png")
    return generated


def draw_aave_daily_trend(aave_data: Dict[str, Any], out_dir: str) -> bool:
    records = aave_data.get("records") or []
    if not records:
        return False
    daily = defaultdict(int)
    for rec in records:
        ts = rec.get("block_timestamp")
        if ts:
            day = __import__("datetime").datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")
            daily[day] += 1
    dates = sorted(daily.keys())
    vals = [daily[d] for d in dates]
    plt.figure(figsize=(12, 6))
    plt.plot(dates, vals, marker="o", linewidth=1.4, color="#2563eb")
    plt.title("Aave Daily Flash Loan Transaction Trend", fontsize=14, weight="bold")
    plt.ylabel("Transaction Count")
    plt.xticks(rotation=45, ha="right")
    _save_fig(os.path.join(out_dir, "aave_daily_trend.png"))
    return True


def _borrower_counts_and_amounts(aave_data: Dict[str, Any]) -> Tuple[Dict[str, int], Dict[str, Decimal]]:
    counts = defaultdict(int)
    amounts = defaultdict(Decimal)
    for rec in aave_data.get("records") or []:
        flash_loan = rec.get("flash_loan") or {}
        borrower = (flash_loan.get("borrower") or "").lower()
        if not borrower:
            continue
        counts[borrower] += 1
        for asset in flash_loan.get("assets") or []:
            amounts[borrower] += _to_decimal(asset.get("amount"))
    return dict(counts), dict(amounts)


def draw_aave_borrow_frequency(aave_data: Dict[str, Any], out_dir: str) -> bool:
    counts, _ = _borrower_counts_and_amounts(aave_data)
    if not counts:
        return False
    bucket_counter = Counter()
    for c in counts.values():
        if c > 10:
            bucket_counter[">10"] += 1
        elif c >= 5:
            bucket_counter["5-10"] += 1
        elif c >= 2:
            bucket_counter["2-4"] += 1
        else:
            bucket_counter["1"] += 1
    ordered = ["1", "2-4", "5-10", ">10"]
    vals = [float(bucket_counter.get(k, 0)) for k in ordered]
    _bar_with_labels(
        ordered,
        vals,
        "Aave Borrower Frequency Distribution",
        "Number of Borrowers",
        os.path.join(out_dir, "aave_borrow_frequency_distribution.png"),
        rotate=0,
        label_style="int",
        show_top_share_inside=4,
    )
    return True


def draw_aave_top_borrowers(aave_data: Dict[str, Any], out_dir: str, top_n: int) -> bool:
    counts, _ = _borrower_counts_and_amounts(aave_data)
    if not counts:
        return False
    ranked = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    labels = [addr[:10] + "..." for addr, _ in ranked]
    values = [float(v) for _, v in ranked]
    _bar_with_labels(
        labels,
        values,
        "Aave Top Borrowers by Transaction Count",
        "Borrow Count",
        os.path.join(out_dir, "aave_top_borrowers.png"),
        rotate=30,
        label_style="int",
        show_top_share_inside=4,
    )
    return True


def draw_aave_borrower_scale(aave_data: Dict[str, Any], out_dir: str, top_n: int) -> bool:
    _, amounts = _borrower_counts_and_amounts(aave_data)
    if not amounts:
        return False
    ranked = sorted(amounts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    labels = [addr[:10] + "..." for addr, _ in ranked]
    values = [float(v) for _, v in ranked]
    _bar_with_labels(labels, values, "Aave Top Borrowers by Total Borrowed Amount", "Total Borrowed Amount", os.path.join(out_dir, "aave_top_borrowers_by_amount.png"), rotate=30, show_top_share_inside=4)
    return True


def main() -> None:
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    generated: List[str] = []
    unsupported: List[Dict[str, str]] = []

    aave_data = _load_json(args.aave_json) if os.path.exists(args.aave_json) else {}
    stats_data = _load_json(args.stats_json) if os.path.exists(args.stats_json) else []
    detailed_data = _load_json(args.detailed_json) if os.path.exists(args.detailed_json) else {}

    if detailed_data:
        if draw_protocol_transaction_count(detailed_data, args.out_dir):
            generated.append("protocol_transaction_count.png")
        if draw_protocol_share(detailed_data, args.out_dir):
            generated.append("protocol_transaction_share.png")
        if draw_protocol_daily_trend(detailed_data, args.out_dir):
            generated.append("protocol_daily_trend.png")
    else:
        unsupported.append({"item": "protocol comparison charts", "reason": "missing output/flashloan_detailed_stats.json"})

    if isinstance(stats_data, list) and stats_data:
        if draw_protocol_by_block_range(stats_data, args.out_dir):
            generated.append("protocol_by_block_range.png")
    else:
        unsupported.append({"item": "protocol by block range chart", "reason": "missing output/flashloan_stats.json"})

    if aave_data:
        if draw_aave_flashloan_share(aave_data, args.aave_total_tx_in_range, args.out_dir):
            generated.append("aave_flashloan_share_in_range.png")
        generated.extend(draw_aave_top_tokens(aave_data, args.out_dir, args.top_n))
        if draw_aave_daily_trend(aave_data, args.out_dir):
            generated.append("aave_daily_trend.png")
        if draw_aave_borrow_frequency(aave_data, args.out_dir):
            generated.append("aave_borrow_frequency_distribution.png")
        if draw_aave_top_borrowers(aave_data, args.out_dir, args.top_n):
            generated.append("aave_top_borrowers.png")
        if draw_aave_borrower_scale(aave_data, args.out_dir, args.top_n):
            generated.append("aave_top_borrowers_by_amount.png")
    else:
        unsupported.append({"item": "Aave token and borrower charts", "reason": "missing Aave JSON"})

    unsupported.extend([
        {"item": "USD-normalized total borrowed amount chart", "reason": "historical token prices are not available in the current dataset"},
        {"item": "Per-protocol borrower count chart for Balancer/Uniswap", "reason": "Balancer and Uniswap records do not contain borrower addresses in current outputs"},
        {"item": "Per-protocol borrower size chart for Balancer/Uniswap", "reason": "Balancer and Uniswap records do not contain borrower amounts in current outputs"},
        {"item": "Application scenario chart (Arbitrage / Liquidation / Collateral Swap / Wash Trading)", "reason": "current outputs do not contain reliable trace-level labels for application classification"},
    ])

    with open(os.path.join(args.out_dir, "chart_manifest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated": generated}, f, indent=2, ensure_ascii=False)
    with open(os.path.join(args.out_dir, "unsupported_items.json"), "w", encoding="utf-8") as f:
        json.dump(unsupported, f, indent=2, ensure_ascii=False)

    print(json.dumps({"generated": generated, "unsupported": unsupported}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()





