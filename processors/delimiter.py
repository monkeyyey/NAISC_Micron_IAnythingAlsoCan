"""
delimiter.py — Processor for delimiter-separated logs that are NOT standard CSV/TSV.

Handles mixed or non-standard delimiters: pipe (|), semicolon (;), colon (:), 
or combinations thereof.

Example formats:
    2024-01-15 10:23:01|ERROR|database|Connection timeout after 30s
    INFO ; 2024-01-15T10:23:01Z ; cache ; Hit ratio: 94%
    ERROR:2024-01-15:app-server:Disk usage critical 98C
"""

from __future__ import annotations

import re
from typing import Iterator

from ..base_processor import BaseLogProcessor, LogRecord
from ..normalizer import (
    clean_text,
    parse_timestamp,
    to_iso8601,
    remove_units,
    try_numeric,
    normalize_whitespace,
)

# ---------------------------------------------------------------------------
# Delimiter auto-detection
# ---------------------------------------------------------------------------

_DELIMITER_CANDIDATES = ["|", ";", ":", "\t", ","]

# Regex to detect a delimiter character (outside of quoted strings)
_QUOTE_RE = re.compile(r'"[^"]*"|\'[^\']*\'')


def _strip_quotes(line: str) -> str:
    """Remove quoted strings so delimiters inside them don't confuse detection."""
    return _QUOTE_RE.sub("QQQ", line)


def detect_delimiter(lines: list[str]) -> str:
    """Return the most consistent delimiter found across sample lines."""
    candidate_counts: dict[str, list[int]] = {d: [] for d in _DELIMITER_CANDIDATES}

    for line in lines[:30]:
        stripped = _strip_quotes(line)
        for delim in _DELIMITER_CANDIDATES:
            candidate_counts[delim].append(stripped.count(delim))

    # Score: prefer delimiters that appear consistently (low variance) and frequently
    best_delim = "|"
    best_score = -1.0

    for delim, counts in candidate_counts.items():
        if not counts or max(counts) == 0:
            continue
        avg   = sum(counts) / len(counts)
        nonzero = [c for c in counts if c > 0]
        # Consistency: fraction of lines that have this delimiter
        consistency = len(nonzero) / len(counts)
        score = avg * consistency
        if score > best_score:
            best_score = score
            best_delim = delim

    return best_delim


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------

_LEVEL_VALUES = frozenset({
    "TRACE", "DEBUG", "INFO", "NOTICE", "WARN", "WARNING",
    "ERROR", "CRITICAL", "FATAL", "SEVERE", "ALERT",
})


class DelimiterProcessor(BaseLogProcessor):
    """Parse delimiter-separated (pipe/semicolon/custom) log files."""

    FORMAT = "delimiter"

    def __init__(self, delimiter: str | None = None, header: list[str] | None = None):
        """
        Args:
            delimiter: Force a specific delimiter. Auto-detected if None.
            header:    Column names. If None, uses positional field names.
        """
        self.delimiter = delimiter
        self.header = header

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        delim = self.delimiter or detect_delimiter(lines)
        header = self.header

        # Detect header row: if first line matches column-name pattern
        if header is None and lines:
            candidate = lines[0]
            if self._looks_like_header(candidate, delim):
                header = [c.strip().lower() for c in candidate.split(delim)]
                lines = lines[1:]

        for lineno, raw_line in enumerate(lines, start=1):
            corrupted, reason = self._is_corrupted(raw_line)
            if corrupted:
                yield self._make_corrupted(raw_line, lineno, source, reason)
                continue

            line = clean_text(raw_line)
            parts = [p.strip().strip('"').strip("'") for p in line.split(delim)]

            # Validate column count consistency
            if header and len(parts) != len(header):
                yield self._make_corrupted(
                    raw_line, lineno, source,
                    f"column_mismatch: expected {len(header)}, got {len(parts)}"
                )
                continue

            if len(parts) < 2:
                yield self._make_corrupted(raw_line, lineno, source, "too_few_columns")
                continue

            rec = self._build_record(parts, header, raw_line, lineno, source, delim)
            yield rec

    # -----------------------------------------------------------------------

    @staticmethod
    def _looks_like_header(line: str, delim: str) -> bool:
        """Heuristic: a header row has no timestamps and mostly alpha tokens."""
        parts = line.split(delim)
        alpha_count = sum(1 for p in parts if re.match(r'^[A-Za-z_]', p.strip()))
        return alpha_count / max(len(parts), 1) >= 0.6

    def _build_record(
        self,
        parts: list[str],
        header: list[str] | None,
        raw: str,
        lineno: int,
        source: str,
        delim: str,
    ) -> LogRecord:
        # Map parts → column names
        if header:
            col_map = dict(zip(header, parts))
        else:
            col_map = {f"col_{i}": v for i, v in enumerate(parts)}

        # --- Timestamp -------------------------------------------------------
        ts_raw: str | None = None
        ts_iso: str | None = None
        ts_hint_keys = {"timestamp", "time", "date", "datetime", "ts", "col_0"}
        for k in ts_hint_keys:
            if k in col_map:
                dt = parse_timestamp(col_map[k])
                if dt:
                    ts_raw = col_map[k]
                    ts_iso = to_iso8601(dt)
                    break

        # If no dedicated TS column, scan all parts
        if ts_iso is None:
            for part in parts:
                dt = parse_timestamp(part)
                if dt:
                    ts_raw = part
                    ts_iso = to_iso8601(dt)
                    break

        # --- Level -----------------------------------------------------------
        level: str | None = None
        level_hint_keys = {"level", "severity", "loglevel", "col_1"}
        for k in level_hint_keys:
            if k in col_map and col_map[k].upper() in _LEVEL_VALUES:
                level = col_map[k].upper()
                if level == "WARNING":
                    level = "WARN"
                break

        if level is None:
            for part in parts:
                if part.upper() in _LEVEL_VALUES:
                    level = part.upper()
                    if level == "WARNING":
                        level = "WARN"
                    break

        # --- Message ---------------------------------------------------------
        msg_hint_keys = {"message", "msg", "text", "event", "description"}
        message = ""
        for k in msg_hint_keys:
            if k in col_map:
                message = normalize_whitespace(col_map[k])
                break

        # Fallback: last column is usually the message
        if not message and parts:
            message = normalize_whitespace(parts[-1])

        # --- Fields ----------------------------------------------------------
        reserved_values = {ts_raw, level, message}
        fields = {}
        for k, v in col_map.items():
            if v in reserved_values:
                continue
            cleaned = remove_units(v)
            fields[k] = try_numeric(cleaned)

        return LogRecord(
            raw=raw,
            source=source,
            line_number=lineno,
            format=self.FORMAT,
            timestamp_raw=ts_raw,
            timestamp=ts_iso,
            message=message,
            level=level,
            fields=fields,
        )
