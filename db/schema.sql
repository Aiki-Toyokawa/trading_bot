PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_type TEXT NOT NULL CHECK (flow_type IN ('ENTRY', 'EXIT')),
    symbol TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('BUY', 'SELL', 'HOLD')),
    decision_score REAL,
    indicator_snapshot_json TEXT NOT NULL DEFAULT '{}',
    claude_snapshot_json TEXT NOT NULL DEFAULT '{}',
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_signals_symbol_created_at
    ON signals(symbol, created_at DESC);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER,
    alpaca_order_id TEXT,
    client_order_id TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    order_type TEXT NOT NULL DEFAULT 'market',
    time_in_force TEXT NOT NULL DEFAULT 'day',
    qty REAL NOT NULL CHECK (qty > 0),
    requested_price REAL,
    status TEXT NOT NULL,
    raw_request_json TEXT NOT NULL DEFAULT '{}',
    raw_response_json TEXT NOT NULL DEFAULT '{}',
    submitted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (signal_id) REFERENCES signals(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_client_order_id
    ON orders(client_order_id)
    WHERE client_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_orders_symbol_submitted_at
    ON orders(symbol, submitted_at DESC);

CREATE TABLE IF NOT EXISTS order_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN ('BUY_FILLED', 'BUY_NOT_FILLED', 'SELL_FILLED')
    ),
    filled_qty REAL,
    avg_fill_price REAL,
    slippage_pct REAL,
    capital_change_pct REAL,
    realized_pnl_amount REAL,
    outcome_tier TEXT CHECK (
        outcome_tier IS NULL OR
        outcome_tier IN (
            'EXTRA_LOSE', 'BIG_LOSE', 'LOSE',
            'WIN', 'BIG_WIN', 'EXTRA_WIN'
        )
    ),
    note TEXT NOT NULL DEFAULT '',
    event_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_order_logs_order_id_event_at
    ON order_logs(order_id, event_at DESC);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    entry_order_id INTEGER NOT NULL UNIQUE,
    exit_order_id INTEGER UNIQUE,
    qty REAL NOT NULL CHECK (qty > 0),
    initial_qty REAL NOT NULL DEFAULT 0 CHECK (initial_qty >= 0),
    entry_price REAL NOT NULL CHECK (entry_price > 0),
    exit_price REAL,
    invested_amount REAL NOT NULL,
    take_profit_done INTEGER NOT NULL DEFAULT 0 CHECK (take_profit_done IN (0, 1)),
    trailing_high_price REAL,
    gross_pnl_amount REAL,
    gross_pnl_pct REAL,
    outcome_tier TEXT CHECK (
        outcome_tier IS NULL OR
        outcome_tier IN (
            'EXTRA_LOSE', 'BIG_LOSE', 'LOSE',
            'WIN', 'BIG_WIN', 'EXTRA_WIN'
        )
    ),
    entry_slippage_pct REAL,
    exit_slippage_pct REAL,
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'CLOSED', 'CANCELLED')),
    close_reason TEXT NOT NULL DEFAULT '',
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (entry_order_id) REFERENCES orders(id),
    FOREIGN KEY (exit_order_id) REFERENCES orders(id)
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_status
    ON trades(symbol, status);


CREATE TABLE IF NOT EXISTS position_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    qty REAL NOT NULL,
    avg_entry_price REAL,
    market_price REAL,
    market_value REAL,
    unrealized_pl REAL,
    source TEXT NOT NULL DEFAULT 'ALPACA',
    captured_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_position_snapshots_symbol_captured_at
    ON position_snapshots(symbol, captured_at DESC);

CREATE TABLE IF NOT EXISTS analysis_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_type TEXT NOT NULL,
    summary TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_analysis_reports_type_created_at
    ON analysis_reports(report_type, created_at DESC);

CREATE TABLE IF NOT EXISTS run_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    duration_sec REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'SKIPPED', 'ERROR')),
    execution_mode INTEGER NOT NULL DEFAULT 0,
    market_is_open INTEGER NOT NULL DEFAULT 0,
    allow_order_submission INTEGER NOT NULL DEFAULT 0,
    alpaca_enabled INTEGER NOT NULL DEFAULT 0,
    claude_enabled INTEGER NOT NULL DEFAULT 0,
    bought_count INTEGER NOT NULL DEFAULT 0,
    sold_count INTEGER NOT NULL DEFAULT 0,
    reconciled_count INTEGER NOT NULL DEFAULT 0,
    recovered_manual_close_count INTEGER NOT NULL DEFAULT 0,
    recovered_manual_partial_count INTEGER NOT NULL DEFAULT 0,
    filled_count INTEGER NOT NULL DEFAULT 0,
    analysis_executed INTEGER NOT NULL DEFAULT 0,
    note TEXT NOT NULL DEFAULT '',
    error_text TEXT NOT NULL DEFAULT '',
    result_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_run_history_finished_at
    ON run_history(finished_at DESC);
