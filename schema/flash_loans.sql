PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS transactions (
    tx_hash TEXT PRIMARY KEY,
    chain TEXT NOT NULL DEFAULT 'ethereum',
    block_number INTEGER NOT NULL,
    block_timestamp INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('success', 'failed', 'reverted', 'unknown')),
    transaction_from TEXT,
    transaction_to TEXT,
    gas_used INTEGER,
    effective_gas_price TEXT,
    log_count INTEGER,
    internal_call_count INTEGER,
    transfer_count INTEGER,
    inserted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS flash_loans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_hash TEXT NOT NULL,
    provider TEXT NOT NULL,
    provider_address TEXT NOT NULL,
    pattern TEXT NOT NULL,
    pattern_version TEXT NOT NULL DEFAULT 'v1',
    borrower TEXT NOT NULL,
    initiator TEXT,
    receiver_contract TEXT,
    asset_count INTEGER NOT NULL DEFAULT 0,
    trace_available INTEGER NOT NULL DEFAULT 0 CHECK (trace_available IN (0, 1)),
    UNIQUE (tx_hash, provider, borrower, provider_address, pattern),
    FOREIGN KEY (tx_hash) REFERENCES transactions(tx_hash) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS flash_loan_assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flash_loan_id INTEGER NOT NULL,
    asset_index INTEGER NOT NULL,
    token_address TEXT NOT NULL,
    symbol TEXT,
    decimals INTEGER,
    amount_raw TEXT NOT NULL,
    amount_normalized TEXT NOT NULL,
    fee_raw TEXT,
    fee_normalized TEXT,
    UNIQUE (flash_loan_id, asset_index),
    FOREIGN KEY (flash_loan_id) REFERENCES flash_loans(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS flash_loan_execution (
    flash_loan_id INTEGER PRIMARY KEY,
    called_protocols_json TEXT NOT NULL DEFAULT '[]',
    swap_count INTEGER NOT NULL DEFAULT 0,
    liquidation_count INTEGER NOT NULL DEFAULT 0,
    profit_token_address TEXT,
    profit_symbol TEXT,
    profit_raw TEXT,
    profit_normalized TEXT,
    FOREIGN KEY (flash_loan_id) REFERENCES flash_loans(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS flash_loan_labels (
    flash_loan_id INTEGER PRIMARY KEY,
    category TEXT NOT NULL DEFAULT 'unknown' CHECK (
        category IN (
            'arbitrage',
            'liquidation',
            'collateral_swap',
            'debt_refinance',
            'wash_trading',
            'governance_manipulation',
            'exploit',
            'unknown'
        )
    ),
    subtype TEXT,
    is_attack_related INTEGER NOT NULL DEFAULT 0 CHECK (is_attack_related IN (0, 1)),
    confidence REAL,
    notes TEXT,
    FOREIGN KEY (flash_loan_id) REFERENCES flash_loans(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS flash_loan_evidence (
    flash_loan_id INTEGER PRIMARY KEY,
    event_signatures_json TEXT NOT NULL DEFAULT '[]',
    detection_notes TEXT,
    FOREIGN KEY (flash_loan_id) REFERENCES flash_loans(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_transactions_block_number
    ON transactions(block_number);

CREATE INDEX IF NOT EXISTS idx_flash_loans_tx_hash
    ON flash_loans(tx_hash);

CREATE INDEX IF NOT EXISTS idx_flash_loans_provider
    ON flash_loans(provider);

CREATE INDEX IF NOT EXISTS idx_flash_loans_borrower
    ON flash_loans(borrower);

CREATE INDEX IF NOT EXISTS idx_flash_loan_assets_token_address
    ON flash_loan_assets(token_address);

CREATE INDEX IF NOT EXISTS idx_flash_loan_labels_category
    ON flash_loan_labels(category);

CREATE VIEW IF NOT EXISTS v_flash_loan_asset_rows AS
SELECT
    fl.id AS flash_loan_id,
    fl.tx_hash,
    tx.chain,
    tx.block_number,
    tx.block_timestamp,
    tx.status,
    fl.provider,
    fl.provider_address,
    fl.pattern,
    fl.pattern_version,
    fl.borrower,
    fl.initiator,
    fl.receiver_contract,
    asset.asset_index,
    asset.token_address,
    asset.symbol,
    asset.decimals,
    asset.amount_raw,
    asset.amount_normalized,
    asset.fee_raw,
    asset.fee_normalized,
    exec.called_protocols_json,
    exec.swap_count,
    exec.liquidation_count,
    exec.profit_token_address,
    exec.profit_symbol,
    exec.profit_raw,
    exec.profit_normalized,
    label.category,
    label.subtype,
    label.is_attack_related,
    label.confidence,
    label.notes AS label_notes
FROM flash_loans AS fl
JOIN transactions AS tx
    ON tx.tx_hash = fl.tx_hash
JOIN flash_loan_assets AS asset
    ON asset.flash_loan_id = fl.id
LEFT JOIN flash_loan_execution AS exec
    ON exec.flash_loan_id = fl.id
LEFT JOIN flash_loan_labels AS label
    ON label.flash_loan_id = fl.id;
