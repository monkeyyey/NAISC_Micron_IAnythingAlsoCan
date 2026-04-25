"""
database/models.py — Create database tables and indexes.

Tables in output.db:
  parsed_logs    — successfully parsed and validated records
  parse_failures — records that failed at any pipeline stage
  trie_templates — every template the trie has learned (visible in SQLite Studio)
  field_mappings — vendor→canonical field name mappings (mirrors field_mappings.json)
  llm_failures   — lines the LLM could not map after 5 retries, for engineer review
"""

import sqlite3
from database.connection import get_connection
from config import DB_PATH


CREATE_PARSED_LOGS = """
CREATE TABLE IF NOT EXISTS parsed_logs (
    record_id          TEXT PRIMARY KEY,
    timestamp          TEXT,
    tool_id            TEXT,
    log_source         TEXT,
    log_type           TEXT,
    raw_line           TEXT,
    recipe_name        TEXT,
    process_step       TEXT,
    wafer_id           TEXT,
    lot_id             TEXT,
    chamber_id         TEXT,
    temperature        REAL,
    pressure           REAL,
    rf_power           REAL,
    flow_rate          REAL,
    voltage            REAL,
    current            REAL,
    alarm_code         TEXT,
    alarm_severity     TEXT,
    event_description  TEXT,
    status             TEXT,
    extra_fields       TEXT,          -- JSON object string
    mapping_confidence REAL,
    parse_flags        TEXT,          -- JSON array string
    anomaly_score      REAL,
    dedup_key          TEXT UNIQUE,   -- sha256 for deduplication
    ingested_at        TEXT
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_timestamp     ON parsed_logs(timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_tool_id       ON parsed_logs(tool_id);",
    "CREATE INDEX IF NOT EXISTS idx_alarm_code    ON parsed_logs(alarm_code);",
    "CREATE INDEX IF NOT EXISTS idx_anomaly_score ON parsed_logs(anomaly_score);",
    "CREATE INDEX IF NOT EXISTS idx_log_type      ON parsed_logs(log_type);",
    "CREATE INDEX IF NOT EXISTS idx_wafer_id      ON parsed_logs(wafer_id);",
]

CREATE_PARSE_FAILURES = """
CREATE TABLE IF NOT EXISTS parse_failures (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_line    TEXT,
    log_source  TEXT,
    error       TEXT,
    failed_at   TEXT
);
"""

CREATE_TRIE_TEMPLATES = """
CREATE TABLE IF NOT EXISTS trie_templates (
    signature    TEXT PRIMARY KEY,
    template     TEXT NOT NULL,
    position_map TEXT NOT NULL,
    source       TEXT DEFAULT 'unknown',
    hit_count    INTEGER DEFAULT 0,
    created_at   TEXT NOT NULL
);
"""

CREATE_FIELD_MAPPINGS = """
CREATE TABLE IF NOT EXISTS field_mappings (
    vendor_field    TEXT PRIMARY KEY,
    canonical_field TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
"""

CREATE_LLM_FAILURES = """
CREATE TABLE IF NOT EXISTS llm_failures (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    signature  TEXT,
    raw_line   TEXT,
    log_source TEXT,
    error      TEXT,
    attempts   INTEGER DEFAULT 5,
    failed_at  TEXT NOT NULL
);
"""

CREATE_EXTRA_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_llm_failures_source ON llm_failures(log_source);",
]


def init_db(db_path: str = DB_PATH) -> None:
    """Create tables and indexes if they don't exist. Safe to call multiple times."""
    conn = get_connection(db_path)
    conn.execute(CREATE_PARSED_LOGS)
    for idx_sql in CREATE_INDEXES:
        conn.execute(idx_sql)
    conn.execute(CREATE_PARSE_FAILURES)
    conn.execute(CREATE_TRIE_TEMPLATES)
    conn.execute(CREATE_FIELD_MAPPINGS)
    conn.execute(CREATE_LLM_FAILURES)
    for idx_sql in CREATE_EXTRA_INDEXES:
        conn.execute(idx_sql)
    conn.commit()
