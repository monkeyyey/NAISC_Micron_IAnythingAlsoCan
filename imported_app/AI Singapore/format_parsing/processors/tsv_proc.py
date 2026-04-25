"""
tsv_proc.py — Processor for tab-separated value (TSV) log files.

TSV is like CSV but uses tab as delimiter and rarely quotes fields.
Common in system/application logs exported from databases or monitoring tools.

Example:
    timestamp\tlevel\thost\tservice\tlatency_ms\tmessage
    2024-01-15T10:23:01Z\tERROR\tdb-1\tpostgres\t2450\tQuery timeout after 2.45s
"""

from __future__ import annotations

from typing import Any, Iterator

from ..base_processor import BaseLogProcessor, LogRecord
from ..normalizer import (
    clean_text,
    parse_timestamp,
    to_iso8601,
    remove_units,
    try_numeric,
    normalize_whitespace,
)

_TS_COLS  = frozenset({"timestamp", "time", "datetime", "date", "ts", "log_time", "@timestamp"})
_LVL_COLS = frozenset({"level", "severity", "loglevel", "log_level", "lvl", "priority"})
_MSG_COLS = frozenset({"message", "msg", "text", "event", "description", "log", "detail"})
_LEVEL_VALUES = frozenset({
    "TRACE", "DEBUG", "INFO", "NOTICE", "WARN", "WARNING",
    "ERROR", "CRITICAL", "FATAL", "SEVERE", "ALERT",
})


class TSVProcessor(BaseLogProcessor):
    """Parse tab-separated log files."""

    FORMAT = "tsv"

    def __init__(self, has_header: bool | None = None):
        """
        Args:
            has_header: True = first row is a header. None = auto-detect.
        """
        self.has_header = has_header

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        if not lines:
            return

        header: list[str] = []
        expected_cols: int | None = None
        data_lines = lines

        # --- Header detection -----------------------------------------------
        first_parts = lines[0].split("\t")
        use_header = (
            self.has_header
            if self.has_header is not None
            else self._looks_like_header(first_parts)
        )
        if use_header:
            header = [p.strip().lower() for p in first_parts]
            expected_cols = len(header)
            data_lines = lines[1:]
        else:
            expected_cols = len(first_parts)
            header = [f"col_{i}" for i in range(expected_cols)]

        # --- Process rows ---------------------------------------------------
        for lineno, raw_line in enumerate(data_lines, start=2 if use_header else 1):
            corrupted, reason = self._is_corrupted(raw_line)
            if corrupted:
                yield self._make_corrupted(raw_line, lineno, source, reason)
                continue

            parts = raw_line.split("\t")

            # Validate column count
            if expected_cols is not None and len(parts) != expected_cols:
                yield self._make_corrupted(
                    raw_line, lineno, source,
                    f"column_mismatch: expected {expected_cols}, got {len(parts)}"
                )
                continue

            rec = self._build_record(parts, header, raw_line, lineno, source)
            yield self._normalize_record(rec)

    # -----------------------------------------------------------------------

    @staticmethod
    def _looks_like_header(parts: list[str]) -> bool:
        if not parts:
            return False
        alpha = sum(
            1 for p in parts
            if p.strip() and p.strip().replace("_", "").replace("-", "").isalpha()
        )
        numeric = sum(
            1 for p in parts
            if p.strip().replace(".", "").replace("-", "").isdigit()
        )
        return alpha / max(len(parts), 1) >= 0.5 and numeric == 0

    def _build_record(
        self,
        parts: list[str],
        header: list[str],
        raw_line: str,
        lineno: int,
        source: str,
    ) -> LogRecord:
        col_map: dict[str, str] = {
            h: clean_text(parts[i]) for i, h in enumerate(header)
        }

        # --- Timestamp -------------------------------------------------------
        ts_raw: str | None = None
        ts_iso: str | None = None
        for ts_key in _TS_COLS:
            if ts_key in col_map and col_map[ts_key]:
                dt = parse_timestamp(col_map[ts_key])
                if dt:
                    ts_raw = col_map[ts_key]
                    ts_iso = to_iso8601(dt)
                    break

        if ts_iso is None:
            for part in parts:
                dt = parse_timestamp(part.strip())
                if dt:
                    ts_raw = part.strip()
                    ts_iso = to_iso8601(dt)
                    break

        # --- Level -----------------------------------------------------------
        level: str | None = None
        for lvl_key in _LVL_COLS:
            if lvl_key in col_map:
                val = col_map[lvl_key].upper()
                if val in _LEVEL_VALUES:
                    level = "WARN" if val == "WARNING" else val
                    break

        if level is None:
            for part in parts:
                if part.strip().upper() in _LEVEL_VALUES:
                    level = part.strip().upper()
                    if level == "WARNING":
                        level = "WARN"
                    break

        # --- Message ---------------------------------------------------------
        message = ""
        for msg_key in _MSG_COLS:
            if msg_key in col_map and col_map[msg_key]:
                message = normalize_whitespace(col_map[msg_key])
                break

        if not message and parts:
            message = normalize_whitespace(clean_text(parts[-1]))

        # --- Fields ----------------------------------------------------------
        reserved = _TS_COLS | _LVL_COLS | _MSG_COLS
        fields: dict[str, Any] = {}
        for k, v in col_map.items():
            if k in reserved or not v:
                continue
            cleaned = remove_units(v)
            fields[k] = try_numeric(cleaned)

        return LogRecord(
            raw=raw_line,
            source=source,
            line_number=lineno,
            format=self.FORMAT,
            timestamp_raw=ts_raw,
            timestamp=ts_iso,
            message=message,
            level=level,
            fields=fields,
        )
