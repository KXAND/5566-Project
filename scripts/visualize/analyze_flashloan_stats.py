import json
import os
import sys
from collections import defaultdict

# 设置UTF-8输出
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# 数据目录 - 使用Windows路径格式
DATA_DIR = r"C:\Users\Voyage\Desktop\5566\爬数据\数据和草稿脚本"
OUTPUT_DIR = r"C:\Users\Voyage\Desktop\5566\爬数据放文件"

def load_data():
    """加载所有闪电贷数据"""
    all_records = []

    # 1. Uniswap数据
    uniswap_file = os.path.join(DATA_DIR, "uniswap_events_20260414_020655.json")
    if os.path.exists(uniswap_file):
        with open(uniswap_file, 'r') as f:
            data = json.load(f)
            records = data.get('records', [])
            for r in records:
                r['protocol'] = 'uniswap_v2'
            all_records.extend(records)
            print(f"Uniswap: 加载 {len(records)} 条")

    # 2. Balancer数据
    balancer_file = os.path.join(DATA_DIR, "balancer_events_20260414_174657.json")
    if os.path.exists(balancer_file):
        with open(balancer_file, 'r') as f:
            data = json.load(f)
            records = data.get('records', [])
            for r in records:
                r['protocol'] = 'balancer_v2'
            all_records.extend(records)
            print(f"Balancer: 加载 {len(records)} 条")

    # 3. Aave数据 - 检查两个文件
    aave_files = [
        'flash_loans_20260404_135340.json',
        'flash_loans_20260408_230617.json'
    ]
    for af in aave_files:
        aave_file = os.path.join(DATA_DIR, af)
        if os.path.exists(aave_file):
            with open(aave_file, 'r') as f:
                data = json.load(f)
                # Aave数据可能在records或data字段
                records = data.get('records', data.get('data', []))
                for r in records:
                    provider = r.get('provider', 'aave_v3')
                    r['protocol'] = provider
                all_records.extend(records)
                print(f"Aave ({af}): 加载 {len(records)} 条")

    return all_records

def analyze_by_block_ranges(records, block_ranges):
    """按区块范围分析协议占比"""
    results = []

    for start, end in block_ranges:
        range_records = [r for r in records if start <= r.get('block_number', 0) <= end]

        if not range_records:
            results.append({
                'block_range': f"{start}-{end}",
                'total': 0,
                'breakdown': {}
            })
            continue

        # 统计各协议数量
        protocol_counts = defaultdict(int)
        for r in range_records:
            protocol_counts[r.get('protocol', 'unknown')] += 1

        total = len(range_records)
        breakdown = {}
        for protocol, count in protocol_counts.items():
            breakdown[protocol] = {
                'count': count,
                'percentage': round(count / total * 100, 2)
            }

        results.append({
            'block_range': f"{start}-{end}",
            'total': total,
            'breakdown': dict(protocol_counts)  # 原始计数
        })

    return results

def main():
    print("=" * 60)
    print("闪电贷协议占比分析")
    print("=" * 60)

    # 加载数据
    records = load_data()
    print(f"\n总计加载: {len(records)} 条记录")

    if not records:
        print("没有数据！")
        return

    # 分析区块范围
    blocks = [r.get('block_number') for r in records if r.get('block_number')]
    if blocks:
        print(f"区块范围: {min(blocks)} - {max(blocks)}")

    # 定义分析区块范围 (可调整)
    # 这里用10万区块为一个区间
    block_ranges = [
        (24700000, 24799999),
        (24800000, 24899999),
        (24900000, 24999999),
    ]

    results = analyze_by_block_ranges(records, block_ranges)

    # 打印结果
    print("\n" + "=" * 60)
    print("各区块范围协议占比分析")
    print("=" * 60)

    for r in results:
        print(f"\n📊 区块范围: {r['block_range']}")
        print(f"   总交易数: {r['total']}")
        if r['breakdown']:
            total = r['total']
            for protocol, count in sorted(r['breakdown'].items(), key=lambda x: -x[1]):
                pct = count / total * 100
                print(f"   - {protocol}: {count} ({pct:.1f}%)")

    # 保存结果
    output_file = os.path.join(OUTPUT_DIR, "flashloan_stats.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n✅ 结果已保存到: {output_file}")

    # 同时生成CSV方便查看
    csv_file = os.path.join(OUTPUT_DIR, "flashloan_stats.csv")
    with open(csv_file, 'w', encoding='utf-8') as f:
        f.write("block_range,protocol,count,percentage\n")
        for r in results:
            total = r['total']
            for protocol, count in r['breakdown'].items():
                pct = count / total * 100 if total > 0 else 0
                f.write(f"{r['block_range']},{protocol},{count},{pct:.2f}\n")

    print(f"✅ CSV已保存到: {csv_file}")

if __name__ == "__main__":
    main()
