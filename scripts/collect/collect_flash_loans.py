import argparse
import csv
import json
import os
from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Dict, Iterable, List, Optional

from web3 import Web3


getcontext().prec = 50


AAVE_V2_POOL = "0x7d2768dE32b0b80b7a3454c06BdAc94A69DdC7A9"
AAVE_V3_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"


AAVE_V2_FLASHLOAN_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "internalType": "address", "name": "_target", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "_reserve", "type": "address"},
        {"indexed": False, "internalType": "uint256", "name": "_amount", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "_totalFee", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "_protocolFee", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "_timestamp", "type": "uint256"},
    ],
    "name": "FlashLoan",
    "type": "event",
}


AAVE_V3_FLASHLOAN_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "internalType": "address", "name": "target", "type": "address"},
        {"indexed": False, "internalType": "address", "name": "initiator", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "asset", "type": "address"},
        {"indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "interestRateMode", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "premium", "type": "uint256"},
        {"indexed": True, "internalType": "uint16", "name": "referralCode", "type": "uint16"},
    ],
    "name": "FlashLoan",
    "type": "event",
}


ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
]


@dataclass
class FlashLoanRecord:
    tx_hash: str
    block_number: int
    log_index: int
    provider: str
    pool_address: str
    borrower: str
    initiator: Optional[str]
    asset_address: str
    asset_symbol: str
    asset_decimals: int
    amount_raw: int
    amount_normalized: str
    fee_raw: int
    fee_normalized: str


class TokenMetadataCache:
    def __init__(self, w3: Web3) -> None:
        self.w3 = w3
        self.cache: Dict[str, Dict[str, object]] = {}

    def get(self, token_address: str) -> Dict[str, object]:
        checksum = self.w3.to_checksum_address(token_address)
        if checksum in self.cache:
            return self.cache[checksum]

        contract = self.w3.eth.contract(address=checksum, abi=ERC20_ABI)
        symbol = "UNKNOWN"
        decimals = 18

        try:
            symbol = contract.functions.symbol().call()
        except Exception:
            pass

        try:
            decimals = contract.functions.decimals().call()
        except Exception:
            pass

        self.cache[checksum] = {"symbol": symbol, "decimals": decimals}
        return self.cache[checksum]


def normalize_amount(amount_raw: int, decimals: int) -> str:
    scale = Decimal(10) ** decimals
    return format(Decimal(amount_raw) / scale, "f")


def iter_block_ranges(start_block: int, end_block: int, chunk_size: int) -> Iterable[range]:
    current = start_block
    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)
        yield range(current, chunk_end + 1)
        current = chunk_end + 1


def build_event_contract(w3: Web3, address: str, event_abi: dict):
    return w3.eth.contract(address=w3.to_checksum_address(address), abi=[event_abi])


def collect_aave_v2(w3: Web3, start_block: int, end_block: int, chunk_size: int) -> List[FlashLoanRecord]:
    contract = build_event_contract(w3, AAVE_V2_POOL, AAVE_V2_FLASHLOAN_EVENT_ABI)
    token_cache = TokenMetadataCache(w3)
    records: List[FlashLoanRecord] = []

    for block_range in iter_block_ranges(start_block, end_block, chunk_size):
        events = contract.events.FlashLoan().get_logs(
            from_block=block_range.start,
            to_block=block_range.stop - 1,
        )
        for event in events:
            args = event["args"]
            asset = args["_reserve"]
            metadata = token_cache.get(asset)
            amount_raw = int(args["_amount"])
            fee_raw = int(args["_totalFee"])
            decimals = int(metadata["decimals"])

            records.append(
                FlashLoanRecord(
                    tx_hash=event["transactionHash"].hex(),
                    block_number=event["blockNumber"],
                    log_index=event["logIndex"],
                    provider="aave_v2",
                    pool_address=AAVE_V2_POOL,
                    borrower=args["_target"],
                    initiator=None,
                    asset_address=asset,
                    asset_symbol=str(metadata["symbol"]),
                    asset_decimals=decimals,
                    amount_raw=amount_raw,
                    amount_normalized=normalize_amount(amount_raw, decimals),
                    fee_raw=fee_raw,
                    fee_normalized=normalize_amount(fee_raw, decimals),
                )
            )

    return records


def collect_aave_v3(w3: Web3, start_block: int, end_block: int, chunk_size: int) -> List[FlashLoanRecord]:
    contract = build_event_contract(w3, AAVE_V3_POOL, AAVE_V3_FLASHLOAN_EVENT_ABI)
    token_cache = TokenMetadataCache(w3)
    records: List[FlashLoanRecord] = []

    for block_range in iter_block_ranges(start_block, end_block, chunk_size):
        events = contract.events.FlashLoan().get_logs(
            from_block=block_range.start,
            to_block=block_range.stop - 1,
        )
        for event in events:
            args = event["args"]
            asset = args["asset"]
            metadata = token_cache.get(asset)
            amount_raw = int(args["amount"])
            fee_raw = int(args["premium"])
            decimals = int(metadata["decimals"])

            records.append(
                FlashLoanRecord(
                    tx_hash=event["transactionHash"].hex(),
                    block_number=event["blockNumber"],
                    log_index=event["logIndex"],
                    provider="aave_v3",
                    pool_address=AAVE_V3_POOL,
                    borrower=args["target"],
                    initiator=args["initiator"],
                    asset_address=asset,
                    asset_symbol=str(metadata["symbol"]),
                    asset_decimals=decimals,
                    amount_raw=amount_raw,
                    amount_normalized=normalize_amount(amount_raw, decimals),
                    fee_raw=fee_raw,
                    fee_normalized=normalize_amount(fee_raw, decimals),
                )
            )

    return records


def aggregate_by_token(records: List[FlashLoanRecord]) -> List[dict]:
    grouped: Dict[str, dict] = {}
    for record in records:
        key = f"{record.provider}:{record.asset_address.lower()}"
        if key not in grouped:
            grouped[key] = {
                "provider": record.provider,
                "asset_address": record.asset_address,
                "asset_symbol": record.asset_symbol,
                "flash_loan_count": 0,
                "amount_raw_total": 0,
                "fee_raw_total": 0,
                "asset_decimals": record.asset_decimals,
            }

        grouped[key]["flash_loan_count"] += 1
        grouped[key]["amount_raw_total"] += record.amount_raw
        grouped[key]["fee_raw_total"] += record.fee_raw

    result = []
    for item in grouped.values():
        decimals = int(item["asset_decimals"])
        result.append(
            {
                **item,
                "amount_normalized_total": normalize_amount(int(item["amount_raw_total"]), decimals),
                "fee_normalized_total": normalize_amount(int(item["fee_raw_total"]), decimals),
            }
        )

    result.sort(key=lambda x: (x["provider"], Decimal(x["amount_normalized_total"])), reverse=True)
    return result


def aggregate_by_provider(records: List[FlashLoanRecord]) -> List[dict]:
    grouped: Dict[str, dict] = {}
    for record in records:
        if record.provider not in grouped:
            grouped[record.provider] = {
                "provider": record.provider,
                "flash_loan_count": 0,
                "unique_borrowers": set(),
                "unique_assets": set(),
            }

        grouped[record.provider]["flash_loan_count"] += 1
        grouped[record.provider]["unique_borrowers"].add(record.borrower.lower())
        grouped[record.provider]["unique_assets"].add(record.asset_address.lower())

    result = []
    for item in grouped.values():
        result.append(
            {
                "provider": item["provider"],
                "flash_loan_count": item["flash_loan_count"],
                "unique_borrowers": len(item["unique_borrowers"]),
                "unique_assets": len(item["unique_assets"]),
            }
        )

    result.sort(key=lambda x: x["flash_loan_count"], reverse=True)
    return result


def write_csv(path: str, records: List[FlashLoanRecord]) -> None:
    fieldnames = [
        "tx_hash",
        "block_number",
        "log_index",
        "provider",
        "pool_address",
        "borrower",
        "initiator",
        "asset_address",
        "asset_symbol",
        "asset_decimals",
        "amount_raw",
        "amount_normalized",
        "fee_raw",
        "fee_normalized",
    ]

    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record.__dict__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Aave flash loan transactions from Ethereum.")
    parser.add_argument("--from-block", type=int, required=True, help="Start block number.")
    parser.add_argument("--to-block", type=int, required=True, help="End block number.")
    parser.add_argument(
        "--provider",
        choices=["aave", "aave_v2", "aave_v3"],
        default="aave",
        help="Which provider subset to collect.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=2000,
        help="Block chunk size used for eth_getLogs requests.",
    )
    parser.add_argument("--csv-out", type=str, default=None, help="Optional path to write raw event rows.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rpc_url = os.environ.get("ETH_RPC_URL")
    if not rpc_url:
        raise SystemExit("Missing ETH_RPC_URL environment variable.")

    if args.from_block > args.to_block:
        raise SystemExit("--from-block must be less than or equal to --to-block.")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise SystemExit("Failed to connect to Ethereum RPC.")

    records: List[FlashLoanRecord] = []

    if args.provider in ("aave", "aave_v2"):
        records.extend(collect_aave_v2(w3, args.from_block, args.to_block, args.chunk_size))

    if args.provider in ("aave", "aave_v3"):
        records.extend(collect_aave_v3(w3, args.from_block, args.to_block, args.chunk_size))

    records.sort(key=lambda x: (x.block_number, x.log_index))

    summary = {
        "query": {
            "from_block": args.from_block,
            "to_block": args.to_block,
            "provider": args.provider,
            "chunk_size": args.chunk_size,
        },
        "event_count": len(records),
        "provider_summary": aggregate_by_provider(records),
        "token_summary": aggregate_by_token(records),
    }

    if args.csv_out:
        write_csv(args.csv_out, records)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
