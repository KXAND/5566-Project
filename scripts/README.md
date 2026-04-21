# Scripts

This directory is organized into three groups.

## `collect/`

Scripts for collecting raw flash-loan-related data.

- `collect_flash_loans.py`
  - Collects Aave V2 and Aave V3 flash loan events directly from Ethereum RPC.
- `flashdata.py`
  - Collects recent Aave flash loan transactions from Etherscan and writes structured JSON.
- `balancer_data.py`
  - Collects recent Balancer V2 `FlashLoan` events from Etherscan.
- `uniswap_data.py`
  - Collects recent sampled Uniswap pool `Swap` and `Flash` events from Etherscan.

## `summarize/`

Scripts for SQLite import and DeepSeek-based summarization.

- `import_records_to_sqlite.py`
  - Flattens JSON records and imports them into SQLite.
- `run_flashloan_pipeline.py`
  - Runs the JSON -> SQLite -> DeepSeek analysis pipeline.
- `summarize_flashloans_with_deepseek.py`
  - Summarizes individual records, segments, and global findings with DeepSeek.

## `visualize/`

Scripts for statistics aggregation and chart generation.

- `analyze_flashloan_stats.py`
  - Builds protocol-level aggregated statistics by block range.
- `analyze_detailed.py`
  - Builds detailed protocol, token, borrower, and time statistics.
- `visualize_flash_loans.py`
  - Generates supported charts from project JSON outputs.
