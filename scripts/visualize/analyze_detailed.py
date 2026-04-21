import json
import os
import sys
from collections import defaultdict
from datetime import datetime

# 设置UTF-8输出
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# 数据目录
DATA_DIR = r"C:\Users\Voyage\Desktop\5566\爬数据\数据和草稿脚本"
OUTPUT_DIR = r"C:\Users\Voyage\Desktop\5566\爬数据放文件"

def load_all_data():
    """加载所有闪电贷数据"""
    all_records = []

    # 1. Uniswap数据
    uniswap_file = os.path.join(DATA_DIR, "uniswap_events_20260414_020655.json")
    if os.path.exists(uniswap_file):
        with open(uniswap_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            records = data.get('records', [])
            for r in records:
                r['protocol'] = 'uniswap_v2'
            all_records.extend(records)
            print(f"Uniswap V2: {len(records)} 条")

    # 2. Balancer数据
    balancer_file = os.path.join(DATA_DIR, "balancer_events_20260414_174657.json")
    if os.path.exists(balancer_file):
        with open(balancer_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            records = data.get('records', [])
            for r in records:
                r['protocol'] = 'balancer_v2'
            all_records.extend(records)
            print(f"Balancer V2: {len(records)} 条")

    # 3. Aave数据
    aave_files = [
        'flash_loans_20260408_230617.json',
        'flash_loans_20260404_135340.json'
    ]
    for af in aave_files:
        aave_file = os.path.join(DATA_DIR, af)
        if os.path.exists(aave_file):
            with open(aave_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                records = data.get('records', data.get('data', []))
                for r in records:
                    r['protocol'] = r.get('provider', 'aave_v3')
                all_records.extend(records)
                print(f"Aave ({af.split('_')[1]}): {len(records)} 条")

    return all_records

def detailed_analysis(records):
    """详细分析"""
    results = {
        "overview": {},
        "by_protocol": {},
        "by_block_range": {},
        "token_analysis": {},
        "top_addresses": {},
        "time_distribution": {}
    }

    # 1. 总体概览
    results["overview"] = {
        "total_records": len(records),
        "block_range": {
            "min": min(r.get('block_number', 0) for r in records if r.get('block_number')),
            "max": max(r.get('block_number', 0) for r in records if r.get('block_number'))
        },
        "time_range": {
            "earliest": datetime.fromtimestamp(min(r.get('block_timestamp', 0) for r in records if r.get('block_timestamp'))).strftime("%Y-%m-%d %H:%M"),
            "latest": datetime.fromtimestamp(max(r.get('block_timestamp', 0) for r in records if r.get('block_timestamp'))).strftime("%Y-%m-%d %H:%M")
        }
    }

    # 2. 按协议统计
    protocol_stats = defaultdict(lambda: {"count": 0, "success": 0, "failed": 0, "total_value": 0, "total_fee": 0})
    for r in records:
        protocol = r.get('protocol', 'unknown')
        protocol_stats[protocol]["count"] += 1
        status = r.get('status', '')
        if status == 'success':
            protocol_stats[protocol]["success"] += 1
        else:
            protocol_stats[protocol]["failed"] += 1

        # Aave数据有金额信息
        if 'flash_loan' in r:
            fl = r['flash_loan']
            for asset in fl.get('assets', []):
                try:
                    protocol_stats[protocol]["total_value"] += float(asset.get('amount', 0))
                    protocol_stats[protocol]["total_fee"] += float(asset.get('fee', 0))
                except:
                    pass

    results["by_protocol"] = dict(protocol_stats)

    # 3. 按区块范围统计 (每5万区块)
    block_ranges = [
        (24600000, 24699999),
        (24700000, 24799999),
        (24800000, 24899999),
    ]
    block_stats = {}
    for start, end in block_ranges:
        range_records = [r for r in records if start <= r.get('block_number', 0) <= end]
        if not range_records:
            continue

        counts = defaultdict(int)
        for r in range_records:
            counts[r.get('protocol', 'unknown')] += 1

        block_stats[f"{start}-{end}"] = {
            "total": len(range_records),
            "breakdown": dict(counts)
        }
    results["by_block_range"] = block_stats

    # 4. Token分析 (只针对Aave)
    token_stats = defaultdict(lambda: {"count": 0, "total_amount": 0, "total_fee": 0})
    for r in records:
        if 'flash_loan' in r:
            fl = r['flash_loan']
            for asset in fl.get('assets', []):
                symbol = asset.get('symbol', asset.get('token_address', 'unknown'))
                token_stats[symbol]["count"] += 1
                try:
                    token_stats[symbol]["total_amount"] += float(asset.get('amount', 0))
                    token_stats[symbol]["total_fee"] += float(asset.get('fee', 0))
                except:
                    pass

    results["token_analysis"] = dict(token_stats)

    # 5. Top地址分析
    borrower_counts = defaultdict(int)
    for r in records:
        if 'flash_loan' in r:
            borrower = r['flash_loan'].get('borrower', '')
            if borrower:
                borrower_counts[borrower.lower()] += 1

    results["top_addresses"] = {
        "top_borrowers": sorted(borrower_counts.items(), key=lambda x: -x[1])[:10]
    }

    # 6. 时间分布 (按天)
    daily_stats = defaultdict(lambda: defaultdict(int))
    for r in records:
        ts = r.get('block_timestamp', 0)
        if ts:
            day = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            daily_stats[day][r.get('protocol', 'unknown')] += 1

    results["time_distribution"] = {day: dict(counts) for day, counts in sorted(daily_stats.items())}

    return results

def print_report(results):
    """打印报告"""
    print("\n" + "=" * 70)
    print("闪电贷详细统计分析报告")
    print("=" * 70)

    # 概览
    print("\n【1. 总体概览】")
    print(f"  总记录数: {results['overview']['total_records']:,}")
    br = results['overview']['block_range']
    print(f"  区块范围: {br['min']:,} - {br['max']:,}")
    tr = results['overview']['time_range']
    print(f"  时间范围: {tr['earliest']} ~ {tr['latest']}")

    # 按协议
    print("\n【2. 各协议统计】")
    print(f"  {'协议':<15} {'数量':>8} {'成功':>8} {'失败':>8} {'总借款金额':>18} {'总费用':>15}")
    print("  " + "-" * 70)
    for protocol, stats in sorted(results['by_protocol'].items(), key=lambda x: -x[1]['count']):
        value_str = f"{stats['total_value']:,.2f}" if stats['total_value'] > 0 else "-"
        fee_str = f"{stats['total_fee']:,.2f}" if stats['total_fee'] > 0 else "-"
        print(f"  {protocol:<15} {stats['count']:>8,} {stats['success']:>8,} {stats['failed']:>8,} {value_str:>18} {fee_str:>15}")

    # 按区块范围
    print("\n【3. 按区块范围占比】")
    for range_str, data in results['by_block_range'].items():
        print(f"\n  区块 {range_str}:")
        total = data['total']
        for protocol, count in sorted(data['breakdown'].items(), key=lambda x: -x[1]):
            pct = count / total * 100
            bar = "█" * int(pct / 2)
            print(f"    {protocol:<15}: {count:>5,} ({pct:>5.1f}%) {bar}")

    # Token分析
    print("\n【4. Token分布 (Aave)】")
    print(f"  {'Token':<10} {'次数':>8} {'总借款金额':>20} {'总费用':>15}")
    print("  " + "-" * 60)
    for token, stats in sorted(results['token_analysis'].items(), key=lambda x: -x[1]['count'])[:15]:
        amount_str = f"{stats['total_amount']:,.2f}"
        fee_str = f"{stats['total_fee']:,.4f}"
        print(f"  {token:<10} {stats['count']:>8,} {amount_str:>20} {fee_str:>15}")

    # Top地址
    print("\n【5. Top 10 借款地址】")
    for i, (addr, count) in enumerate(results['top_addresses']['top_borrowers'], 1):
        print(f"  {i:>2}. {addr[:20]}... : {count:,} 次")

    # 时间分布
    print("\n【6. 按日分布】")
    for day, counts in list(results['time_distribution'].items())[:10]:
        total = sum(counts.values())
        print(f"  {day}: {total:,} 笔", end="")
        for proto, cnt in counts.items():
            print(f" | {proto}: {cnt}", end="")
        print()

    print("\n" + "=" * 70)

def main():
    print("=" * 70)
    print("闪电贷数据详细分析")
    print("=" * 70)

    # 加载数据
    records = load_all_data()
    print(f"\n总计: {len(records):,} 条记录")

    # 分析
    results = detailed_analysis(records)

    # 打印报告
    print_report(results)

    # 保存结果
    output_file = os.path.join(OUTPUT_DIR, "flashloan_detailed_stats.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)

    print(f"\n✅ 详细结果已保存到: {output_file}")

if __name__ == "__main__":
    main()
