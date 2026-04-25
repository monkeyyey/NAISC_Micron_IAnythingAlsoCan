"""
database/writer.py — Batch and single record insert logic.

Deduplication is enforced at the database level via the UNIQUE constraint on
dedup_key. Duplicate inserts are silently skipped (INSERT OR IGNORE).
JSON columns (extra_fields, parse_flags) are serialised to strings here.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from database.connection import get_connection, transaction
from config import DB_PATH, DB_BATCH_INSERT_SIZE

logger = logging.getLogger(__name__)

INSERT_PARSED_LOG = """
INSERT OR IGNORE INTO parsed_logs (
    record_id, timestamp, tool_id, log_source, log_type, raw_line,
    recipe_name, process_step, wafer_id, lot_id, chamber_id,
    temperature, pressure, rf_power, flow_rate, voltage, current,
    alarm_code, alarm_severity, event_description, status,
    extra_fields, mapping_confidence, parse_flags, anomaly_score,
    dedup_key, ingested_at
) VALUES (
    :record_id, :timestamp, :tool_id, :log_source, :log_type, :raw_line,
    :recipe_name, :process_step, :wafer_id, :lot_id, :chamber_id,
    :temperature, :pressure, :rf_power, :flow_rate, :voltage, :current,
    :alarm_code, :alarm_severity, :event_description, :status,
    :extra_fields, :mapping_confidence, :parse_flags, :anomaly_score,
    :dedup_key, :ingested_at
)
"""

INSERT_FAILURE = """
INSERT INTO parse_failures (raw_line, log_source, error, failed_at)
VALUES (:raw_line, :log_source, :error, :failed_at)
"""


def _serialise(record: dict) -> dict:
    """Prepare a record dict for SQLite insert — serialise JSON columns."""
    row = dict(record)

    # Serialise dict/list columns to JSON strings
    if isinstance(row.get("extra_fields"), dict):
        row["extra_fields"] = json.dumps(row["extra_fields"])
    elif row.get("extra_fields") is None:
        row["extra_fields"] = "{}"

    if isinstance(row.get("parse_flags"), list):
        row["parse_flags"] = json.dumps(row["parse_flags"])
    elif row.get("parse_flags") is None:
        row["parse_flags"] = "[]"

    # Set defaults for nullable fields
    nullable_fields = [
        "timestamp", "tool_id", "recipe_name", "process_step", "wafer_id", "lot_id", "chamber_id",
        "temperature", "pressure", "rf_power", "flow_rate", "voltage", "current",
        "alarm_code", "alarm_severity", "event_description", "status"
    ]
    for field in nullable_fields:
        row.setdefault(field, None)

    row.setdefault("ingested_at", datetime.now(timezone.utc).isoformat())
    row.setdefault("anomaly_score", None)

    return row


def bulk_insert(records: list[dict], db_path: str = DB_PATH) -> tuple[int, int]:
    """
    Insert records in batches of DB_BATCH_INSERT_SIZE.
    Duplicates are silently skipped via INSERT OR IGNORE on dedup_key.
    Returns (inserted_count, skipped_count).
    """
    if not records:
        return 0, 0

    inserted = 0
    skipped = 0

    for batch_start in range(0, len(records), DB_BATCH_INSERT_SIZE):
        batch = records[batch_start: batch_start + DB_BATCH_INSERT_SIZE]
        rows = [_serialise(r) for r in batch]

        with transaction(db_path) as conn:
            for row in rows:
                cursor = conn.execute(INSERT_PARSED_LOG, row)
                if cursor.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1

    logger.debug("bulk_insert: inserted=%d skipped=%d", inserted, skipped)
    return inserted, skipped


def update_anomaly_scores(updates: list[tuple[float, str]], db_path: str = DB_PATH) -> None:
    """
    Update anomaly_score for records after scoring.
    updates: list of (anomaly_score, record_id)
    """
    if not updates:
        return
    with transaction(db_path) as conn:
        conn.executemany(
            "UPDATE parsed_logs SET anomaly_score = ? WHERE record_id = ?",
            updates,
        )


def insert_failure(
    raw_line: str,
    log_source: str,
    error: str,
    db_path: str = DB_PATH,
) -> None:
    """Log a failed record to parse_failures table. Never raises."""
    try:
        with transaction(db_path) as conn:
            conn.execute(INSERT_FAILURE, {
                "raw_line":  raw_line,
                "log_source": log_source,
                "error":     error,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            })
    except Exception as exc:
        logger.error("Failed to write to parse_failures: %s", exc)


def insert_trie_template(
    signature: str,
    template: str,
    position_map: str,
    source: str = "unknown",
    db_path: str = DB_PATH,
) -> None:
    """Persist a trie template to the database. INSERT OR IGNORE (idempotent). Never raises."""
    try:
        with transaction(db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO trie_templates "
                "(signature, template, position_map, source, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (signature, template, position_map, source,
                 datetime.now(timezone.utc).isoformat()),
            )
    except Exception as exc:
        logger.error("Failed to write to trie_templates: %s", exc)


def increment_trie_hit(signature: str, db_path: str = DB_PATH) -> None:
    """Increment hit_count for a trie template. Never raises."""
    try:
        with transaction(db_path) as conn:
            conn.execute(
                "UPDATE trie_templates SET hit_count = hit_count + 1 WHERE signature = ?",
                (signature,),
            )
    except Exception as exc:
        logger.error("Failed to increment trie hit: %s", exc)


def upsert_field_mappings(mappings: dict[str, str], db_path: str = DB_PATH) -> None:
    """Bulk-upsert vendor→canonical field name mappings. Never raises."""
    if not mappings:
        return
    now = datetime.now(timezone.utc).isoformat()
    try:
        with transaction(db_path) as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO field_mappings "
                "(vendor_field, canonical_field, created_at) VALUES (?, ?, ?)",
                [(vendor, canonical, now) for vendor, canonical in mappings.items()],
            )
    except Exception as exc:
        logger.error("Failed to upsert field_mappings: %s", exc)


def insert_llm_failure(
    signature: str,
    raw_line: str,
    log_source: str,
    error: str,
    attempts: int = 5,
    db_path: str = DB_PATH,
) -> None:
    """Record a permanently-failed LLM mapping for engineer review. Never raises."""
    try:
        with transaction(db_path) as conn:
            conn.execute(
                "INSERT INTO llm_failures "
                "(signature, raw_line, log_source, error, attempts, failed_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (signature, raw_line, log_source, error, attempts,
                 datetime.now(timezone.utc).isoformat()),
            )
    except Exception as exc:
        logger.error("Failed to write to llm_failures: %s", exc)
