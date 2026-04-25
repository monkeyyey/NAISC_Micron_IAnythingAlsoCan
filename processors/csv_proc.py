"""
csv_proc.py — Processor for RFC-4180 compliant CSV log files.

Handles:
    - Header row auto-detection
    - Quoted fields with embedded commas / newlines
    - Mixed or missing values
    - Semantic column discovery (timestamp, level, message, …)
"""

from __future__ import annotations

import csv
import io
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

# Semantic column name groups
_TS_COLS  = frozenset({"timestamp", "time", "datetime", "date", "ts", "log_time", "@timestamp"})
_LVL_COLS = frozenset({"level", "severity", "loglevel", "log_level", "lvl", "priority"})
_MSG_COLS = frozenset({"message", "msg", "text", "event", "description", "log", "detail"})
_LEVEL_VALUES = frozenset({
    "TRACE", "DEBUG", "INFO", "NOTICE", "WARN", "WARNING",
    "ERROR", "CRITICAL", "FATAL", "SEVERE", "ALERT",
})


class CSVProcessor(BaseLogProcessor):
    """Parse CSV log files using Python's built-in csv module."""

    FORMAT = "csv"

    def __init__(
        self,
        has_header: bool | None = None,
        dialect: str | csv.Dialect = "excel",
    ):
        """
        Args:
            has_header: True = first row is a header. None = auto-detect.
            dialect:    csv.Dialect or dialect name (default 'excel').
        """
        self.has_header = has_header
        self.dialect = dialect

    # -----------------------------------------------------------------------
    # Override: CSV needs the full text, not pre-split lines
    # -----------------------------------------------------------------------

    def _run_pipeline(self, text: str, *, source: str) -> Iterator[LogRecord]:
        """Override to pass full text to csv.reader (handles multiline fields)."""
        yield from self._parse_csv(text, source=source)

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        """Not used directly for CSV — _run_pipeline delegates to _parse_csv."""
        raise NotImplementedError("Use _parse_csv instead")

    # -----------------------------------------------------------------------

    def _parse_csv(self, text: str, *, source: str) -> Iterator[LogRecord]:
        try:
            dialect = csv.get_dialect(self.dialect) if isinstance(self.dialect, str) else self.dialect
        except csv.Error:
            dialect = csv.excel  # type: ignore[assignment]

        buf = io.StringIO(text)
        reader = csv.reader(buf, dialect=dialect)  # type: ignore[call-overload]

        header: list[str] = []
        expected_cols: int | None = None
        lineno = 0

        for raw_row in reader:
            lineno += 1
            raw_line = ",".join(raw_row)  # reconstruct for raw storage

            # --- Determine header -------------------------------------------
            if lineno == 1:
                use_header = (
                    self.has_header
                    if self.has_header is not None
                    else self._looks_like_header(raw_row)
                )
                if use_header:
                    header = [c.strip().lower() for c in raw_row]
                    expected_cols = len(header)
                    continue
                else:
                    expected_cols = len(raw_row)
                    header = [f"col_{i}" for i in range(expected_cols)]

            # --- Validate row -----------------------------------------------
            if not raw_row or all(c.strip() == "" for c in raw_row):
                continue  # skip blank rows silently

            if expected_cols is not None and len(raw_row) != expected_cols:
                yield self._make_corrupted(
                    raw_line, lineno, source,
                    f"column_mismatch: expected {expected_cols}, got {len(raw_row)}"
                )
                continue

            # Basic corruption check on each cell
            if self._cells_corrupted(raw_row):
                yield self._make_corrupted(raw_line, lineno, source, "cell_encoding_error")
                continue

            rec = self._build_record(raw_row, header, raw_line, lineno, source)
            yield self._normalize_record(rec)

    # -----------------------------------------------------------------------

    @staticmethod
    def _looks_like_header(row: list[str]) -> bool:
        """Heuristic: a header row is mostly alphabetic, no pure-numeric cells."""
        if not row:
            return False
        alpha = sum(1 for c in row if c.strip() and c.strip().replace("_", "").replace("-", "").isalpha())
        numeric = sum(1 for c in row if c.strip().replace(".", "").replace("-", "").isdigit())
        return alpha / max(len(row), 1) >= 0.5 and numeric == 0

    @staticmethod
    def _cells_corrupted(row: list[str]) -> bool:
        """True if the majority of cells look like encoding garbage."""
        if not row:
            return True
        bad = sum(1 for c in row if c.count("\ufffd") / max(len(c), 1) > 0.3)
        return bad / len(row) > 0.5

    def _build_record(
        self,
        row: list[str],
        header: list[str],
        raw_line: str,
        lineno: int,
        source: str,
    ) -> LogRecord:
        col_map: dict[str, str] = {h: row[i].strip() for i, h in enumerate(header)}

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

        # Fallback: scan all cells for timestamp shape
        if ts_iso is None:
            for cell in row:
                dt = parse_timestamp(cell.strip())
                if dt:
                    ts_raw = cell.strip()
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

        # --- Message ---------------------------------------------------------
        message = ""
        for msg_key in _MSG_COLS:
            if msg_key in col_map and col_map[msg_key]:
                message = normalize_whitespace(clean_text(col_map[msg_key]))
                break

        if not message and row:
            message = normalize_whitespace(clean_text(row[-1]))

        # --- Remaining fields -----------------------------------------------
        reserved_keys = _TS_COLS | _LVL_COLS | _MSG_COLS
        fields: dict[str, Any] = {}
        for k, v in col_map.items():
            if k in reserved_keys or not v:
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
