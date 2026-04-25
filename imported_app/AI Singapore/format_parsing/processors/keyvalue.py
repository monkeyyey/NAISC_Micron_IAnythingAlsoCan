"""
keyvalue.py — Processor for key=value / key:value style logs (logfmt-compatible).

Handles lines like:
    ts=2024-01-15T10:23:01Z level=error msg="disk full" host=db-1 usage=98%
    time="2024-01-15 10:23:01" severity:ERROR component:cache latency:45ms
    [2024-01-15 10:23:01] key1=val1 key2="spaced value" key3=42
"""

from __future__ import annotations

import re
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

# ---------------------------------------------------------------------------
# KV pair regex — supports both = and : separators, quoted or bare values
# ---------------------------------------------------------------------------
#
# Key:   word chars, dots, hyphens, slashes, @ (e.g., "log.level", "host@dc1")
# Value: double-quoted, single-quoted, or bare (non-whitespace)
#
_KV_RE = re.compile(
    r'(?P<key>[\w.\-/@]+)'
    r'\s*(?:=|:)\s*'
    r'(?:"(?P<dq>[^"\\]*(?:\\.[^"\\]*)*)"|'
    r"'(?P<sq>[^'\\]*(?:\\.[^'\\]*)*)'|"
    r'(?P<bare>[^\s,;]+))'
)

# Common timestamp-related key names
_TS_KEYS  = frozenset({"ts", "time", "timestamp", "datetime", "date", "@timestamp", "log_time"})
# Common message key names
_MSG_KEYS = frozenset({"msg", "message", "text", "log", "event", "description", "detail"})
# Common level key names
_LVL_KEYS = frozenset({"level", "severity", "loglevel", "log_level", "lvl", "priority"})


class KeyValueProcessor(BaseLogProcessor):
    """Parse logfmt / key=value style log lines."""

    FORMAT = "key_value"

    # Minimum number of KV pairs required for a line to be considered valid KV
    MIN_KV_PAIRS: int = 2

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        for lineno, raw_line in enumerate(lines, start=1):
            corrupted, reason = self._is_corrupted(raw_line)
            if corrupted:
                yield self._make_corrupted(raw_line, lineno, source, reason)
                continue

            line = clean_text(raw_line)
            pairs = self._extract_kv_pairs(line)

            if len(pairs) < self.MIN_KV_PAIRS:
                # Not enough structure — treat as corrupted / malformed KV
                yield self._make_corrupted(
                    raw_line, lineno, source,
                    f"insufficient_kv_pairs (found {len(pairs)}, need {self.MIN_KV_PAIRS})"
                )
                continue

            rec = self._build_record(pairs, raw_line, lineno, source)
            yield rec

    # -----------------------------------------------------------------------

    def _extract_kv_pairs(self, line: str) -> dict[str, str]:
        """Extract all key=value / key:value pairs from a line."""
        pairs: dict[str, str] = {}
        for m in _KV_RE.finditer(line):
            key   = m.group("key").lower().strip()
            value = m.group("dq") or m.group("sq") or m.group("bare") or ""
            pairs[key] = value
        return pairs

    def _build_record(
        self,
        pairs: dict[str, str],
        raw: str,
        lineno: int,
        source: str,
    ) -> LogRecord:
        # --- Timestamp -------------------------------------------------------
        ts_raw: str | None = None
        ts_iso: str | None = None
        for ts_key in _TS_KEYS:
            if ts_key in pairs:
                ts_raw = pairs[ts_key]
                dt = parse_timestamp(ts_raw)
                if dt:
                    ts_iso = to_iso8601(dt)
                break

        # --- Level -----------------------------------------------------------
        level: str | None = None
        for lvl_key in _LVL_KEYS:
            if lvl_key in pairs:
                raw_level = pairs[lvl_key].upper()
                level = "WARN" if raw_level in ("WARNING", "WARN") else raw_level
                break

        # --- Message ---------------------------------------------------------
        message = ""
        for msg_key in _MSG_KEYS:
            if msg_key in pairs:
                message = normalize_whitespace(pairs[msg_key])
                break

        # --- Build remaining fields ------------------------------------------
        reserved = _TS_KEYS | _MSG_KEYS | _LVL_KEYS
        fields: dict[str, Any] = {}
        for k, v in pairs.items():
            if k in reserved:
                continue
            # Remove units, then try numeric conversion
            cleaned_v = remove_units(v)
            fields[k] = try_numeric(cleaned_v)

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
