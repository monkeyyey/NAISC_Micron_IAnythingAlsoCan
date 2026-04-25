"""
normalisation/cleaner.py — Type validation, range checking, timestamp standardisation,
and deduplication key generation.

Runs after unit normalisation, before canonical schema validation.
"""

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from dateutil import parser as dateutil_parser

from config import CANONICAL_FIELD_RANGES, MEASUREMENT_FIELDS

logger = logging.getLogger(__name__)

# Supported explicit timestamp formats (tried before dateutil fallback)
_TS_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%m/%d/%Y %H:%M:%S",
    "%d-%b-%Y %H:%M:%S",   # 15-Apr-2024 12:03:20
    "%d/%m/%Y %H:%M:%S",
    "%b %d %H:%M:%S",       # Syslog BSD: Apr 15 12:03:20
]


def clean_record(record: dict) -> dict:
    """
    Run all cleaning passes on a record.
    Modifies record in-place and returns it.

    Passes (in order):
      1. Type validation for measurement fields
      2. Range checking (redundant with unit_normaliser but defensive)
      3. Timestamp standardisation
      4. Missing required field flags
      5. Deduplication key generation
    """
    if "parse_flags" not in record or record["parse_flags"] is None:
        record["parse_flags"] = []

    _validate_types(record)
    _standardise_timestamp(record)
    _check_missing_required(record)
    _generate_dedup_key(record)

    return record


# ---------------------------------------------------------------------------
# Type validation
# ---------------------------------------------------------------------------

def _validate_types(record: dict) -> None:
    for field in MEASUREMENT_FIELDS:
        value = record.get(field)
        if value is None:
            continue
        try:
            record[field] = float(value)
        except (TypeError, ValueError):
            record["parse_flags"].append(f"type_error:{field}")
            record[field] = None


# ---------------------------------------------------------------------------
# Timestamp standardisation
# ---------------------------------------------------------------------------

def _standardise_timestamp(record: dict) -> None:
    raw_ts = record.get("timestamp")
    if raw_ts is None:
        return

    # Already a proper ISO string
    if isinstance(raw_ts, str) and re.match(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", raw_ts
    ):
        return

    # Unix epoch integer (10 digits = seconds, 13 = milliseconds)
    if isinstance(raw_ts, (int, float)):
        epoch = float(raw_ts)
        if epoch > 1e12:
            epoch /= 1000.0  # milliseconds → seconds
        try:
            record["timestamp"] = datetime.fromtimestamp(
                epoch, tz=timezone.utc
            ).isoformat()
            return
        except (OSError, OverflowError, ValueError):
            pass

    if isinstance(raw_ts, str):
        # Try integer string
        stripped = raw_ts.strip()
        if stripped.isdigit() and len(stripped) >= 10:
            try:
                epoch = float(stripped)
                if epoch > 1e12:
                    epoch /= 1000.0
                record["timestamp"] = datetime.fromtimestamp(
                    epoch, tz=timezone.utc
                ).isoformat()
                return
            except (OSError, OverflowError, ValueError):
                pass

        # Try explicit formats
        for fmt in _TS_FORMATS:
            try:
                dt = datetime.strptime(stripped, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                record["timestamp"] = dt.isoformat()
                return
            except ValueError:
                pass

        # Fall back to dateutil (handles many formats)
        try:
            dt = dateutil_parser.parse(stripped)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            record["timestamp"] = dt.isoformat()
            return
        except (ValueError, OverflowError):
            pass

    record["parse_flags"].append("timestamp_parse_error")
    # Keep raw string for audit trail


# ---------------------------------------------------------------------------
# Required field checks
# ---------------------------------------------------------------------------

def _check_missing_required(record: dict) -> None:
    for field in ("raw_line",):
        if not record.get(field):
            record["parse_flags"].append(f"missing_required:{field}")


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _generate_dedup_key(record: dict) -> None:
    """
    Generate a sha256 dedup key from (tool_id, timestamp, raw_line).
    Even if some fields are None, the key is still generated to enable
    deduplication of identical-looking failures.
    """
    tool_id   = str(record.get("tool_id") or "")
    timestamp = str(record.get("timestamp") or "")
    raw_line  = str(record.get("raw_line") or "")
    composite = f"{tool_id}|{timestamp}|{raw_line}"
    record["dedup_key"] = hashlib.sha256(composite.encode("utf-8")).hexdigest()
