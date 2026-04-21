import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple

import requests
from eth_abi import decode
from web3 import Web3

# 设置高精度计算
getcontext().prec = 50

ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
CHAIN_ID = "1"
DEFAULT_TARGET_TRANSACTIONS = 5  # 改成了 5，方便你快速看到测试结果
DEFAULT_BLOCK_WINDOW = 2_000
DEFAULT_OFFSET = 1_000
# 你的 API Key
PLACEHOLDER_API_KEY = "677GN5M6I25HTNY56T4R6CETQGD65UNNXG"
# "W5CI6PRTA97WK84T3AMXRCE98E6SD864NI"

def log_status(msg: str) -> None:
    """将状态信息打印到终端，不影响最终的 JSON 输出"""
    # 使用 \r 可以让部分输出在同一行刷新，更像进度条
    sys.stderr.write(f"[*] {msg}\n")
    sys.stderr.flush()

@dataclass(frozen=True)
class EventConfig:
    provider: str
    provider_address: str
    event_name: str
    event_signature: str
    topic0: str
    indexed_layout: Tuple[str, ...]
    data_layout: Tuple[str, ...]
    pattern: str
    pattern_version: str

AAVE_EVENTS = (
    EventConfig(
        provider="aave_v2",
        provider_address="0x7d2768dE32b0b80b7a3454c06BdAc94A69DdC7A9",
        event_name="FlashLoan",
        event_signature="FlashLoan(address,address,address,uint256,uint256,uint16)",
        topic0="0x631042c832b07452973831137f2d73e395028b44b250dedc5abb0ee766e168ac",
        indexed_layout=("target", "initiator", "asset"),
        data_layout=("uint256", "uint256", "uint16"),
        pattern="flashloan_event",
        pattern_version="v1",
    ),
    EventConfig(
        provider="aave_v3",
        provider_address="0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
        event_name="FlashLoan",
        event_signature="FlashLoan(address,address,address,uint256,uint8,uint256,uint16)",
        topic0="0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0",
        indexed_layout=("target", "asset", "referralCode"),
        data_layout=("address", "uint256", "uint8", "uint256"),
        pattern="flashloan_event",
        pattern_version="v1",
    ),
)

ERC20_DECIMALS_CALL = "0x313ce567"
ERC20_SYMBOL_CALL = "0x95d89b41"

def normalize_amount(amount_raw: int, decimals: Optional[int]) -> str:
    if decimals is None:
        return str(amount_raw)
    scale = Decimal(10) ** decimals
    return format(Decimal(amount_raw) / scale, "f")

def topic_to_address(topic_value: str) -> str:
    return Web3.to_checksum_address("0x" + topic_value[-40:])

def topic_to_int(topic_value: str) -> int:
    return int(topic_value, 16)

class EtherscanClient:
    def __init__(self, api_key: str, base_url: str = ETHERSCAN_BASE_URL, timeout: int = 30) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "claw/1.0"})
        self._block_timestamp_cache: Dict[int, int] = {}
        self._token_metadata_cache: Dict[str, Dict[str, Optional[object]]] = {}

    def _request(self, params: Dict[str, object]) -> dict:
        query = {"chainid": CHAIN_ID, "apikey": self.api_key, **params}
        retries = 5
        delay = 0.5

        for attempt in range(retries):
            response = self.session.get(self.base_url, params=query, timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()

            message = str(payload.get("message", ""))
            result = payload.get("result")

            if isinstance(result, str) and "rate limit" in result.lower():
                if attempt == retries - 1:
                    raise RuntimeError(result)
                log_status(f"触发 API 速率限制 (Rate limit)。等待 {delay} 秒后重试...")
                time.sleep(delay)
                delay *= 2
                continue

            if payload.get("status") == "0" and message == "NOTOK":
                if attempt == retries - 1:
                    raise RuntimeError(result or "Etherscan request failed.")
                log_status(f"API 请求异常 ({result})。等待 {delay} 秒后重试...")
                time.sleep(delay)
                delay *= 2
                continue

            return payload
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
        payload = self._request(
            {
                "module": "logs",
                "action": "getLogs",
                "address": address,
                "topic0": topic0,
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

    def eth_call(self, to_address: str, data: str) -> Optional[str]:
        payload = self._request(
            {
                "module": "proxy",
                "action": "eth_call",
                "to": to_address,
                "data": data,
                "tag": "latest",
            }
        )
        return payload.get("result")

    def get_token_metadata(self, token_address: str) -> Dict[str, Optional[object]]:
        checksum = Web3.to_checksum_address(token_address)
        if checksum in self._token_metadata_cache:
            return self._token_metadata_cache[checksum]

        symbol: Optional[str] = None
        decimals: Optional[int] = None

        try:
            decimals_result = self.eth_call(checksum, ERC20_DECIMALS_CALL)
            if decimals_result and decimals_result != "0x":
                decimals = int(decimals_result, 16)
        except Exception:
            decimals = None

        try:
            symbol_result = self.eth_call(checksum, ERC20_SYMBOL_CALL)
            if symbol_result and symbol_result != "0x":
                symbol = self._decode_symbol(symbol_result)
        except Exception:
            symbol = None

        metadata = {"symbol": symbol, "decimals": decimals}
        self._token_metadata_cache[checksum] = metadata
        return metadata

    @staticmethod
    def _decode_symbol(raw_hex: str) -> Optional[str]:
        if raw_hex in ("0x", ""):
            return None
        raw_bytes = bytes.fromhex(raw_hex[2:])
        if len(raw_bytes) == 32:
            try:
                return raw_bytes.rstrip(b"\x00").decode("utf-8") or None
            except UnicodeDecodeError:
                return None
        try:
            decoded = decode(["string"], raw_bytes)[0]
            return decoded or None
        except Exception:
            return None

def decode_aave_log(raw_log: dict, event: EventConfig, client: EtherscanClient) -> dict:
    topics = raw_log["topics"]
    data_bytes = bytes.fromhex(raw_log["data"][2:])
    decoded_values = decode(list(event.data_layout), data_bytes)

    block_number = int(raw_log["blockNumber"], 16)
    log_index = int(raw_log["logIndex"], 16)
    tx_hash = raw_log["transactionHash"]
    block_timestamp = (
        int(raw_log["timeStamp"], 16)
        if "timeStamp" in raw_log
        else client.get_block_timestamp(block_number)
    )

    if event.provider == "aave_v2":
        borrower = topic_to_address(topics[1])
        initiator = topic_to_address(topics[2])
        asset = topic_to_address(topics[3])
        amount_raw = int(decoded_values[0])
        fee_raw = int(decoded_values[1])
        referral_code = int(decoded_values[2])
    else:
        borrower = topic_to_address(topics[1])
        asset = topic_to_address(topics[2])
        referral_code = topic_to_int(topics[3])
        initiator = Web3.to_checksum_address(decoded_values[0])
        amount_raw = int(decoded_values[1])
        fee_raw = int(decoded_values[3])

    token_meta = client.get_token_metadata(asset)
    decimals = token_meta.get("decimals")
    symbol = token_meta.get("symbol")

    return {
        "tx_hash": tx_hash,
        "block_number": block_number,
        "block_timestamp": block_timestamp,
        "log_index": log_index,
        "flash_loan": {
            "provider": event.provider,
            "provider_address": Web3.to_checksum_address(event.provider_address),
            "pattern": event.pattern,
            "pattern_version": event.pattern_version,
            "borrower": borrower,
            "initiator": initiator,
            "receiver_contract": borrower,
            "asset": {
                "token_address": asset,
                "symbol": symbol,
                "decimals": decimals,
                "amount_raw": str(amount_raw),
                "amount": normalize_amount(amount_raw, decimals),
                "fee_raw": str(fee_raw),
                "fee": normalize_amount(fee_raw, decimals),
            },
            "referral_code": referral_code,
        },
        "raw_refs": {
            "event_name": event.event_name,
            "event_signature": event.event_signature,
            "topic0": event.topic0,
        },
    }

def group_records_by_transaction(decoded_logs: List[dict], limit: int) -> List[dict]:
    grouped: Dict[Tuple[str, str, str, str], dict] = {}
    for item in decoded_logs:
        flash_loan = item["flash_loan"]
        key = (item["tx_hash"], flash_loan["provider"], flash_loan["provider_address"], flash_loan["borrower"])
        if key not in grouped:
            grouped[key] = {
                "tx_hash": item["tx_hash"],
                "block_number": item["block_number"],
                "block_timestamp": item["block_timestamp"],
                "chain": "ethereum",
                "status": "success",
                "flash_loan": {
                    "provider": flash_loan["provider"],
                    "provider_address": flash_loan["provider_address"],
                    "pattern": flash_loan["pattern"],
                    "pattern_version": flash_loan["pattern_version"],
                    "borrower": flash_loan["borrower"],
                    "initiator": flash_loan["initiator"],
                    "receiver_contract": flash_loan["receiver_contract"],
                    "assets": [],
                    "asset_count": 0,
                    "referral_code": flash_loan["referral_code"],
                },
                "execution": {"called_protocols": [], "log_count": None, "internal_call_count": None, "swap_count": None, "liquidation_count": None, "transfer_count": None, "profit_token_address": None, "profit_symbol": None, "profit_raw": None, "profit": None},
                "labels": {"category": "unknown", "subtype": None, "is_attack_related": False, "confidence": None, "notes": "Collected from Aave FlashLoan events on Etherscan."},
                "raw_refs": {"event_signatures": [item["raw_refs"]["event_signature"]], "trace_available": False},
                "_sort_log_index": item["log_index"],
            }
        grouped[key]["flash_loan"]["assets"].append(flash_loan["asset"])
        grouped[key]["flash_loan"]["asset_count"] = len(grouped[key]["flash_loan"]["assets"])

    records = list(grouped.values())
    records.sort(key=lambda row: (row["block_number"], row["_sort_log_index"]), reverse=True)
    trimmed = records[:limit]
    for row in trimmed:
        row.pop("_sort_log_index", None)
    return trimmed

def collect_recent_aave_flash_loans(client: EtherscanClient, target_transactions: int, initial_window: int, offset: int) -> List[dict]:
    log_status("正在连接 Etherscan 获取最新区块高度...")
    latest_block = client.get_latest_block()
    log_status(f"最新区块高度为: {latest_block}。开始向后扫描闪电贷事件...")
    
    current_to = latest_block
    window_size = initial_window
    decoded_logs: List[dict] = []

    while current_to >= 0:
        current_from = max(0, current_to - window_size + 1)
        log_status(f"正在扫描区块范围: {current_from} 到 {current_to} | 当前进度: 已收集 {len(group_records_by_transaction(decoded_logs, target_transactions))} / {target_transactions} 笔交易")
        
        batch_logs: List[dict] = []
        for event in AAVE_EVENTS:
            page = 1
            while True:
                logs = client.get_logs(address=event.provider_address, topic0=event.topic0, from_block=current_from, to_block=current_to, page=page, offset=offset)
                if not logs:
                    break
                for raw_log in logs:
                    batch_logs.append(decode_aave_log(raw_log, event, client))
                if len(logs) < offset:
                    break
                page += 1

        if batch_logs:
            decoded_logs.extend(batch_logs)
            grouped = group_records_by_transaction(decoded_logs, target_transactions)
            if len(grouped) >= target_transactions:
                log_status("已达到目标收集数量，准备输出结果！")
                return grouped
            if len(batch_logs) < 100:
                window_size = min(window_size * 2, 50_000)
            elif len(batch_logs) > 2_000:
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
    token_summary: Dict[str, Dict[str, object]] = {}

    for record in records:
        provider = record["flash_loan"]["provider"]
        provider_summary[provider] = provider_summary.get(provider, 0) + 1
        for asset in record["flash_loan"]["assets"]:
            key = f"{provider}:{asset['token_address'].lower()}"
            if key not in token_summary:
                token_summary[key] = {
                    "provider": provider,
                    "token_address": asset["token_address"],
                    "symbol": asset["symbol"],
                    "transaction_count": 0,
                    "total_amount_raw": 0,
                    "total_fee_raw": 0,
                    "decimals": asset["decimals"],
                }
            token_summary[key]["transaction_count"] += 1
            token_summary[key]["total_amount_raw"] += int(asset["amount_raw"])
            token_summary[key]["total_fee_raw"] += int(asset["fee_raw"])

    token_rows = []
    for item in token_summary.values():
        decimals = item["decimals"]
        token_rows.append({
            "provider": item["provider"],
            "token_address": item["token_address"],
            "symbol": item["symbol"],
            "transaction_count": item["transaction_count"],
            "total_amount_raw": str(item["total_amount_raw"]),
            "total_amount": normalize_amount(item["total_amount_raw"], decimals),
            "total_fee_raw": str(item["total_fee_raw"]),
            "total_fee": normalize_amount(item["total_fee_raw"], decimals),
            "decimals": decimals,
        })
    token_rows.sort(key=lambda row: Decimal(row["total_amount"]) if row["total_amount"] else Decimal(0), reverse=True)

    return {
        "query": {"chain": "ethereum", "source": "etherscan", "protocol": "aave", "target_transaction_count": DEFAULT_TARGET_TRANSACTIONS},
        "record_count": len(records),
        "provider_summary": provider_summary,
        "token_summary": token_rows,
        "records": records,
    }

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch the most recent Aave flash-loan transactions from Etherscan.")
    parser.add_argument("--api-key", default=os.environ.get("ETHERSCAN_API_KEY", PLACEHOLDER_API_KEY))
    parser.add_argument("--limit", type=int, default=DEFAULT_TARGET_TRANSACTIONS)
    parser.add_argument("--window", type=int, default=DEFAULT_BLOCK_WINDOW)
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--loop", type=int, default=0, help="循环运行次数，0 表示只运行一次")
    parser.add_argument("--interval", type=int, default=120, help="循环间隔时间（秒），默认 120 秒")
    return parser.parse_args()

def main() -> None:
    args = parse_args()

    loop_count = 0
    total_collected = 0

    while True:
        loop_count += 1
        is_last_run = args.loop == 0 or loop_count >= args.loop

        if args.loop > 0:
            log_status(f"=== 第 {loop_count}/{args.loop} 次运行 ===")
        else:
            log_status(f"=== 第 {loop_count} 次运行 (单次模式) ===")

        log_status("初始化 Etherscan 客户端...")
        client = EtherscanClient(api_key=args.api_key)

        records = collect_recent_aave_flash_loans(
            client=client,
            target_transactions=args.limit,
            initial_window=args.window,
            offset=DEFAULT_OFFSET,
        )

        log_status("正在构建 JSON 输出...")
        payload = build_output(records)
        payload["query"]["target_transaction_count"] = args.limit

        rendered = json.dumps(payload, indent=2, ensure_ascii=False)

        # 获取当前脚本所在目录
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # 生成带循环次数的文件名
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        if args.output:
            output_path = args.output
        elif args.loop > 0:
            output_path = os.path.join(script_dir, f"flash_loans_run{loop_count}_{timestamp}.json")
        else:
            output_path = os.path.join(script_dir, f"flash_loans_{timestamp}.json")

        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write(rendered)
            handle.write("\n")

        this_run_count = len(records)
        total_collected += this_run_count

        if args.loop > 0 and not is_last_run:
            log_status(f"✅ 第 {loop_count} 次完成，收集 {this_run_count} 笔交易。等待 {args.interval} 秒后继续...")
            time.sleep(args.interval)
        else:
            if args.loop > 0:
                log_status(f"🎉 全部完成！共运行 {loop_count} 次，收集 {total_collected} 笔交易")
            else:
                log_status(f"🎉 执行完毕！结果已保存至:\n  -> {output_path}")
            break

if __name__ == "__main__":
    main()