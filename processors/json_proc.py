"""
json_proc.py — Processor for JSON and JSON-Lines (NDJSON) log files.

Handles:
    - JSON-Lines / NDJSON (one JSON object per line)
    - JSON arrays containing log objects
    - Single JSON objects (treated as one record)
    - Nested structures (flattened with dot notation)
    - Mixed content (JSON lines interspersed with plain-text)
"""

from __future__ import annotations

import json
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

# Semantic key discovery — checked in order, first match wins
_TS_KEYS  = ["timestamp", "time", "datetime", "date", "ts", "@timestamp", "log_time",
             "created_at", "logged_at", "event_time", "occurredAt"]
_LVL_KEYS = ["level", "severity", "loglevel", "log_level", "lvl", "priority",
             "log.level", "@level"]
_MSG_KEYS = ["message", "msg", "text", "event", "description", "log", "detail",
             "error", "exception", "reason", "@message"]

_LEVEL_VALUES = frozenset({
    "TRACE", "DEBUG", "INFO", "NOTICE", "WARN", "WARNING",
    "ERROR", "CRITICAL", "FATAL", "SEVERE", "ALERT",
    "0", "1", "2", "3", "4", "5", "6", "7",  # numeric syslog levels
})

# Numeric syslog level → canonical name
_SYSLOG_LEVEL_MAP = {
    "0": "EMERGENCY", "1": "ALERT", "2": "CRITICAL",
    "3": "ERROR",     "4": "WARN",  "5": "NOTICE",
    "6": "INFO",      "7": "DEBUG",
}


class JSONProcessor(BaseLogProcessor):
    """Parse JSON and NDJSON log files."""

    FORMAT = "json"

    def __init__(
        self,
        flatten: bool = True,
        flatten_separator: str = ".",
        max_depth: int = 5,
    ):
        """
        Args:
            flatten:            Flatten nested objects into dot-notation keys.
            flatten_separator:  Separator for flattened keys (default '.').
            max_depth:          Maximum nesting depth to flatten.
        """
        self.flatten = flatten
        self.flatten_separator = flatten_separator
        self.max_depth = max_depth

    # -----------------------------------------------------------------------
    # Override: JSON needs full text for array / object detection
    # -----------------------------------------------------------------------

    def _run_pipeline(self, text: str, *, source: str) -> Iterator[LogRecord]:
        yield from self._parse_json(text, source=source)

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        raise NotImplementedError("Use _parse_json instead")

    # -----------------------------------------------------------------------

    def _parse_json(self, text: str, *, source: str) -> Iterator[LogRecord]:
        stripped = text.strip()

        # --- Try: whole document is a JSON array ---------------------------
        if stripped.startswith("["):
            try:
                array = json.loads(stripped)
                if isinstance(array, list):
                    for lineno, item in enumerate(array, start=1):
                        if isinstance(item, dict):
                            yield self._build_record(item, f"[{lineno}]", lineno, source)
                        else:
                            yield self._make_corrupted(
                                str(item), lineno, source, "array_item_not_object"
                            )
                    return
            except json.JSONDecodeError:
                pass  # Fall through to NDJSON parsing

        # --- Try: whole document is a single JSON object -------------------
        if stripped.startswith("{") and "\n{" not in stripped:
            try:
                obj = json.loads(stripped)
                if isinstance(obj, dict):
                    yield self._build_record(obj, stripped[:80], 1, source)
                    return
            except json.JSONDecodeError:
                pass

        # --- NDJSON / JSON-Lines: one object per line ----------------------
        lines = [ln for ln in text.splitlines() if ln.strip()]
        for lineno, raw_line in enumerate(lines, start=1):
            corrupted, reason = self._is_corrupted(raw_line)
            if corrupted:
                yield self._make_corrupted(raw_line, lineno, source, reason)
                continue

            obj = self._try_parse_json_line(raw_line)
            if obj is None:
                # Not valid JSON — try recovering as plain-text fallback
                yield self._make_corrupted(raw_line, lineno, source, "invalid_json")
                continue

            if not isinstance(obj, dict):
                yield self._make_corrupted(raw_line, lineno, source, "json_not_object")
                continue

            yield self._build_record(obj, raw_line, lineno, source)

    # -----------------------------------------------------------------------

    @staticmethod
    def _try_parse_json_line(line: str) -> Any:
        try:
            return json.loads(line.strip())
        except json.JSONDecodeError:
            # Try fixing trailing commas (common in some logging frameworks)
            fixed = re.sub(r",\s*([}\]])", r"\1", line.strip())
            try:
                return json.loads(fixed)
            except json.JSONDecodeError:
                return None

    def _build_record(
        self,
        obj: dict[str, Any],
        raw: str,
        lineno: int,
        source: str,
    ) -> LogRecord:
        # Flatten nested structure
        flat = self._flatten(obj) if self.flatten else obj

        # --- Timestamp -------------------------------------------------------
        ts_raw: str | None = None
        ts_iso: str | None = None
        for ts_key in _TS_KEYS:
            if ts_key in flat and flat[ts_key] is not None:
                ts_candidate = str(flat[ts_key])
                dt = parse_timestamp(ts_candidate)
                if dt:
                    ts_raw = ts_candidate
                    ts_iso = to_iso8601(dt)
                    break

        # --- Level -----------------------------------------------------------
        level: str | None = None
        for lvl_key in _LVL_KEYS:
            if lvl_key in flat:
                raw_level = str(flat[lvl_key]).upper().strip()
                if raw_level in _LEVEL_VALUES:
                    level = _SYSLOG_LEVEL_MAP.get(raw_level, raw_level)
                    if level == "WARNING":
                        level = "WARN"
                    break

        # --- Message ---------------------------------------------------------
        message = ""
        for msg_key in _MSG_KEYS:
            if msg_key in flat and flat[msg_key]:
                message = normalize_whitespace(clean_text(str(flat[msg_key])))
                break

        # --- Exception / stack-trace ----------------------------------------
        stack_trace: str | None = None
        for exc_key in ("stack_trace", "stacktrace", "stack", "exception", "traceback"):
            if exc_key in flat and flat[exc_key]:
                stack_trace = str(flat[exc_key])
                break

        # --- Remaining fields -----------------------------------------------
        reserved = set(_TS_KEYS) | set(_LVL_KEYS) | set(_MSG_KEYS)
        reserved.update({"stack_trace", "stacktrace", "stack", "exception", "traceback"})

        fields: dict[str, Any] = {}
        for k, v in flat.items():
            if k in reserved:
                continue
            if isinstance(v, str):
                cleaned = remove_units(v)
                fields[k] = try_numeric(cleaned)
            else:
                fields[k] = v  # keep bool, int, float, None as-is

        if stack_trace:
            fields["stack_trace"] = stack_trace

        rec = LogRecord(
            raw=raw if len(raw) < 512 else raw[:512] + "…",
            source=source,
            line_number=lineno,
            format=self.FORMAT,
            timestamp_raw=ts_raw,
            timestamp=ts_iso,
            message=message,
            level=level,
            fields=fields,
        )
        return self._normalize_record(rec)

    # -----------------------------------------------------------------------
    # Flattening
    # -----------------------------------------------------------------------

    def _flatten(
        self,
        obj: dict[str, Any],
        prefix: str = "",
        depth: int = 0,
    ) -> dict[str, Any]:
        """Recursively flatten nested dict using dot-notation keys."""
        if depth >= self.max_depth:
            return {prefix.rstrip(self.flatten_separator): obj} if prefix else {}

        result: dict[str, Any] = {}
        for k, v in obj.items():
            new_key = f"{prefix}{k}" if not prefix else f"{prefix}{self.flatten_separator}{k}"
            if isinstance(v, dict) and v:
                result.update(self._flatten(v, prefix=new_key, depth=depth + 1))
            elif isinstance(v, list):
                # Store lists as-is (could contain complex objects)
                result[new_key] = v
            else:
                result[new_key] = v
        return result
