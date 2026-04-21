# 5566 Project

This repository contains our COMP 5566 project on flash loan measurement and analysis on Ethereum.

## Project Scope

The project focuses on three tasks:

1. Illustrate the interactions between flash loan providers and users.
2. Design patterns to identify flash loan transactions.
3. Collect flash loan transactions from Ethereum and analyze the transferred cryptocurrency.

## Current Coverage

The current repository includes recent samples for:

- Aave V3
- Balancer V2
- Uniswap V2

The strongest structured analysis is available for Aave, because the Aave dataset contains borrower, asset, amount, and fee fields. Balancer and Uniswap are currently more suitable for protocol-level counting and time-distribution analysis.

## Repository Structure

- `scripts/collect/`
  - Data collection scripts.
  - `collect_flash_loans.py`: collect Aave V2/V3 flash loan events from Ethereum RPC.
  - `flashdata.py`: collect recent Aave flash loan transactions from Etherscan.
  - `balancer_data.py`: collect recent Balancer V2 `FlashLoan` events from Etherscan.
  - `uniswap_data.py`: collect sampled Uniswap pool `Swap` and `Flash` events from Etherscan.

- `scripts/summarize/`
  - SQLite import and DeepSeek-based summarization.
  - `import_records_to_sqlite.py`: flatten JSON records and import them into SQLite.
  - `run_flashloan_pipeline.py`: run the JSON -> SQLite -> DeepSeek pipeline.
  - `summarize_flashloans_with_deepseek.py`: generate record-level and global summaries.

- `scripts/visualize/`
  - Statistics and chart generation.
  - `analyze_flashloan_stats.py`: aggregate protocol statistics by block range.
  - `analyze_detailed.py`: build detailed protocol, token, borrower, and time statistics.
  - `visualize_flash_loans.py`: generate charts from project outputs.

- `schema/`
  - `flash_loans.sql`: normalized SQLite schema for flash loan data.

- `output/`
  - Raw collected JSON files.
  - Aggregated statistics JSON files.
  - Generated charts for presentation.

## Data Files in `output/`

Main output files currently used in the project:

- `output/flash_loans_20260408_230617.json`
  - Aave flash loan dataset.
- `output/balancer_events_20260414_174657.json`
  - Balancer flash loan event dataset.
- `output/flashloan_stats.json`
  - Protocol-level statistics by block range.
- `output/flashloan_detailed_stats.json`
  - Detailed protocol, token, borrower, and time statistics.
- `output/charts/`
  - Generated charts used in the presentation.

## Typical Workflow

1. Collect raw data with scripts in `scripts/collect/`.
2. Import records into SQLite if structured querying is needed.
3. Use DeepSeek-based scripts in `scripts/summarize/` for assisted summarization.
4. Generate aggregated charts with scripts in `scripts/visualize/`.

## Example Commands

### Collect Aave flash loans from RPC

```powershell
python scripts/collect/collect_flash_loans.py --from-block 19000000 --to-block 19010000 --provider aave
```

### Import JSON records into SQLite

```powershell
python scripts/summarize/import_records_to_sqlite.py output/flash_loans_20260408_230617.json --table transfer_records
```

### Run the JSON -> SQLite -> DeepSeek pipeline

```powershell
python scripts/summarize/run_flashloan_pipeline.py --json output/flash_loans_20260408_230617.json
```

### Generate charts

```powershell
python scripts/visualize/visualize_flash_loans.py
```

## SQLite Schema

The project includes a normalized schema in `schema/flash_loans.sql`.

Main tables:

- `transactions`
- `flash_loans`
- `flash_loan_assets`
- `flash_loan_execution`
- `flash_loan_labels`
- `flash_loan_evidence`

It also includes the convenience view:

- `v_flash_loan_asset_rows`

## Notes

- Current protocol comparison is count-based and depends on sampled windows.
- Aave supports the strongest token and borrower analysis in the current repository.
- Application-level classification such as arbitrage, liquidation, collateral swap, and wash trading is not yet reliably supported by the current outputs.
