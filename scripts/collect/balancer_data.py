import argparse
import json
import os
import sys
import time
import random
from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple

import requests
from eth_abi import decode
from web3 import Web3

# 设置高精度计算
getcontext().prec = 50


# 使用 Etherscan API V2
ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
DEFAULT_TARGET_TRANSACTIONS = 5000
DEFAULT_BLOCK_WINDOW = 2_000
DEFAULT_OFFSET = 200
PLACEHOLDER_API_KEY = "UGRFE4CGY1USM71MXI48G6WFVF9M4AC14X"

# 初始化 Web3
w3 = Web3()

def log_status(msg: str) -> None:
    sys.stderr.write(f"[*] {msg}\n")
    sys.stderr.flush()

def compute_topic0(event_signature: str) -> str:
    return w3.keccak(text=event_signature).hex()

@dataclass(frozen=True)
class EventConfig:
    provider: str
    provider_address: str
    event_name: str
    event_signature: str
    topic0: str = field(init=False)
    indexed_layout: Tuple[str, ...]
    data_layout: Tuple[str, ...]
    pattern: str
    pattern_version: str

    def __post_init__(self):
        object.__setattr__(self, 'topic0', compute_topic0(self.event_signature))

# Balancer V2 FlashLoan 合约
# 正确地址: 0xBA12222222228d8Ba445958a75a0704d566BF2C8
BALANCER_EVENTS = (
    EventConfig(
        provider="balancer_v2",
        provider_address="0xBA12222222228d8Ba445958a75a0704d566BF2C8",
        event_name="FlashLoan",
        event_signature="FlashLoan(address,address,uint256,uint256,uint256)",
        indexed_layout=("sender", "token"),
        data_layout=("uint256", "uint256", "uint256"),
        pattern="flashloan_event",
        pattern_version="v1",
    ),
)

def topic_to_address(topic_value: str) -> str:
    return Web3.to_checksum_address("0x" + topic_value[-40:])

class EtherscanClient:
    def __init__(self, api_key: str, base_url: str = ETHERSCAN_BASE_URL, timeout: int = 30) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "claw/1.0"})
        self._block_timestamp_cache: Dict[int, int] = {}

    def _request(self, params: Dict[str, object]) -> dict:
        # 添加 chainid 参数 (V2 API 需要)
        query = {"apikey": self.api_key, "chainid": 1, **params}
        retries = 5
        delay = 1.0

        for attempt in range(retries):
            try:
                response = self.session.get(self.base_url, params=query, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()

                result = payload.get("result", "")

                if isinstance(result, str) and "rate limit" in result.lower():
                    if attempt == retries - 1:
                        raise RuntimeError(result)
                    wait_time = delay + random.uniform(0.5, 1.5)
                    log_status(f"触发 API 速率限制。等待 {wait_time:.1f} 秒...")
                    time.sleep(wait_time)
                    delay *= 2
                    continue

                if payload.get("status") == "0" and result:
                    if attempt == retries - 1:
                        raise RuntimeError(result or "Etherscan request failed.")
                    wait_time = delay + random.uniform(0.3, 1.0)
                    log_status(f"API 请求异常 ({result})。等待 {wait_time:.1f} 秒...")
                    time.sleep(wait_time)
                    delay *= 2
                    continue

                # 请求成功，添加延迟避免触发限流
                time.sleep(random.uniform(0.3, 0.6))
                return payload
            except Exception as e:
                if attempt == retries - 1:
                    raise RuntimeError(f"Etherscan request failed: {e}")
                wait_time = delay + random.uniform(0.3, 1.0)
                log_status(f"请求异常 ({e})。等待 {wait_time:.1f} 秒...")
                time.sleep(wait_time)
                delay *= 2

        raise RuntimeError("Etherscan request failed after retries.")

    def get_latest_block(self) -> int:
        payload = self._request({"module": "proxy", "action": "eth_blockNumber"})
        return int(payload["result"], 16)

    def get_block_timestamp(self, block_number: int) -> int:
        if block_number in self._block_timestamp_cache:
            return self._block_timestamp_cache[block_number]

        payload = self._request(
            {
                "module": "proxy",
                "action": "eth_getBlockByNumber",
                "tag": hex(block_number),
                "boolean": "false",
            }
        )
        result = payload.get("result") or {}
        timestamp = int(result["timestamp"], 16)
        self._block_timestamp_cache[block_number] = timestamp
        return timestamp

    def get_logs(self, address: str, topic0: str, from_block: int, to_block: int, page: int, offset: int) -> List[dict]:
        # V2 API 需要 topic0[0]= 格式 (数组语法)
        payload = self._request(
            {
                "module": "logs",
                "action": "getLogs",
                "address": address,
                "topic0[0]": topic0,
                "fromBlock": from_block,
                "toBlock": to_block,
                "page": page,
                "offset": offset,
            }
        )
        if payload.get("status") == "0":
            result = payload.get("result")
            if isinstance(result, str) and result.lower() == "no records found":
                return []
            if result == []:
                return []
        return payload.get("result", [])

def decode_balancer_log(raw_log: dict, event: EventConfig, client: EtherscanClient) -> dict:
    topics = raw_log["topics"]
    data_str = raw_log.get("data", "")
    # 跳过空或无效数据
    if not data_str or len(data_str) < 66:
        return {}
    try:
        data_bytes = bytes.fromhex(data_str[2:])
        decoded_values = decode(list(event.data_layout), data_bytes)
    except Exception:
        return {}

    block_number = int(raw_log["blockNumber"], 16)
    log_index = int(raw_log["logIndex"], 16)
    tx_hash = raw_log["transactionHash"]
    block_timestamp = (
        int(raw_log["timeStamp"], 16)
        if "timeStamp" in raw_log
        else client.get_block_timestamp(block_number)
    )

    pool_address = Web3.to_checksum_address(raw_log.get("address", ""))

    sender = topic_to_address(topics[1]) if len(topics) > 1 else None
    token = topic_to_address(topics[2]) if len(topics) > 2 else None

    amount0_raw = int(decoded_values[0])
    amount1_raw = int(decoded_values[1])
    paid_raw = int(decoded_values[2])

    return {
        "tx_hash": tx_hash,
        "block_number": block_number,
        "block_timestamp": block_timestamp,
        "log_index": log_index,
        "event_type": "flashloan",
        "provider": event.provider,
        "pool_address": pool_address,
        "sender": sender,
        "token": token,
        "amount0_raw": str(amount0_raw),
        "amount1_raw": str(amount1_raw),
        "paid_raw": str(paid_raw),
        "event_name": event.event_name,
    }

def group_records_by_transaction(decoded_logs: List[dict], limit: int) -> List[dict]:
    grouped: Dict[str, dict] = {}
    for item in decoded_logs:
        key = item["tx_hash"]
        if key not in grouped:
            grouped[key] = {
                "tx_hash": item["tx_hash"],
                "block_number": item["block_number"],
                "block_timestamp": item["block_timestamp"],
                "chain": "ethereum",
                "status": "success",
                "event_type": item["event_type"],
                "provider": item["provider"],
                "pool_address": item.get("pool_address"),
                "labels": {
                    "category": "balancer_flashloan",
                    "subtype": item["event_type"],
                },
            }

    records = list(grouped.values())
    records.sort(key=lambda row: row["block_number"], reverse=True)
    return records[:limit]

def collect_recent_balancer_events(client: EtherscanClient, target_transactions: int, initial_window: int, offset: int) -> List[dict]:
    log_status("正在连接 Etherscan 获取最新区块高度...")
    latest_block = client.get_latest_block()
    log_status(f"最新区块高度为: {latest_block}。开始向后扫描 Balancer 事件...")

    current_to = latest_block
    window_size = initial_window
    decoded_logs: List[dict] = []

    while current_to >= 0:
        current_from = max(0, current_to - window_size + 1)
        log_status(f"正在扫描区块范围: {current_from} 到 {current_to} | 当前进度: 已收集 {len(group_records_by_transaction(decoded_logs, target_transactions))} / {target_transactions} 笔交易")

        batch_logs: List[dict] = []
        for event in BALANCER_EVENTS:
            page = 1
            while True:
                logs = client.get_logs(address=event.provider_address, topic0=event.topic0, from_block=current_from, to_block=current_to, page=page, offset=offset)
                if not logs:
                    break
                for raw_log in logs:
                    batch_logs.append(decode_balancer_log(raw_log, event, client))
                if len(logs) < offset:
                    break
                page += 1

        if batch_logs:
            decoded_logs.extend(batch_logs)
            # 过滤掉解码失败的空记录
            decoded_logs = [log for log in decoded_logs if log]
            grouped = group_records_by_transaction(decoded_logs, target_transactions)
            if len(grouped) >= target_transactions:
                log_status("已达到目标收集数量，准备输出结果！")
                return grouped
            if len(batch_logs) < 100:
                window_size = min(window_size * 2, 50_000)
            elif len(batch_logs) > 2000:
                window_size = max(window_size // 2, 250)
        else:
            window_size = min(window_size * 2, 50_000)

        if current_from == 0:
            break
        current_to = current_from - 1

    log_status("已扫描至创世区块。")
    return group_records_by_transaction(decoded_logs, target_transactions)

def build_output(records: List[dict]) -> dict:
    provider_summary: Dict[str, int] = {}
    event_summary: Dict[str, int] = {}

    for record in records:
        provider = record.get("provider", "unknown")
        event_type = record.get("event_type", "unknown")
        provider_summary[provider] = provider_summary.get(provider, 0) + 1
        event_summary[event_type] = event_summary.get(event_type, 0) + 1

    return {
        "query": {"chain": "ethereum", "source": "etherscan", "protocol": "balancer", "target_transaction_count": DEFAULT_TARGET_TRANSACTIONS},
        "record_count": len(records),
        "provider_summary": provider_summary,
        "event_summary": event_summary,
        "records": records,
    }

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Balancer events from Etherscan.")
    parser.add_argument("--api-key", default=os.environ.get("ETHERSCAN_API_KEY", PLACEHOLDER_API_KEY))
    parser.add_argument("--limit", type=int, default=DEFAULT_TARGET_TRANSACTIONS)
    parser.add_argument("--window", type=int, default=DEFAULT_BLOCK_WINDOW)
    parser.add_argument("--output", type=str, default=None)
    return parser.parse_args()

def main() -> None:
    args = parse_args()

    log_status("初始化 Etherscan 客户端...")
    client = EtherscanClient(api_key=args.api_key)

    records = collect_recent_balancer_events(
        client=client,
        target_transactions=args.limit,
        initial_window=args.window,
        offset=DEFAULT_OFFSET,
    )

    log_status("正在构建 JSON 输出...")
    payload = build_output(records)

    rendered = json.dumps(payload, indent=2, ensure_ascii=False)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    if args.output:
        output_path = args.output
    else:
        output_path = os.path.join(script_dir, f"balancer_events_{timestamp}.json")

    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(rendered)
        handle.write("\n")

    log_status(f"执行完毕！结果已保存至: {output_path}")

if __name__ == "__main__":
    main()