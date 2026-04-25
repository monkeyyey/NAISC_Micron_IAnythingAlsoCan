"""
yaml_proc.py — Processor for common YAML log files.

This implementation intentionally targets phase-1 log ingestion needs without
depending on PyYAML. It handles:
    - document-separated mappings (---)
    - top-level list items of mappings
    - simple nested key/value blocks flattened by dotted keys
"""

from __future__ import annotations

import re
from typing import Any, Iterator

from ..base_processor import BaseLogProcessor, LogRecord, extract_level
from ..normalizer import clean_text, parse_timestamp, to_iso8601

_TS_KEYS = {"timestamp", "time", "datetime", "date", "ts", "log_time"}
_LVL_KEYS = {"level", "severity", "priority", "loglevel"}
_MSG_KEYS = {"message", "msg", "text", "event", "description", "detail"}
_KV_RE = re.compile(r"^(?P<key>[^:#][^:]*?):(?:\s+(?P<value>.*))?$")


class YAMLProcessor(BaseLogProcessor):
    """Parse simple YAML logs into canonical records."""

    FORMAT = "yaml"

    def _run_pipeline(self, text: str, *, source: str) -> Iterator[LogRecord]:
        blocks = self._split_documents(text)
        for lineno, block in enumerate(blocks, start=1):
            flat = self._parse_yaml_block(block)
            if not flat:
                yield self._make_corrupted(block[:200], lineno, source, "invalid_yaml")
                continue
            yield self._build_record(flat, block, lineno, source)

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        raise NotImplementedError("Use _run_pipeline for YAML input")

    def _split_documents(self, text: str) -> list[str]:
        lines = text.splitlines()
        blocks: list[list[str]] = []
        current: list[str] = []

        for raw_line in lines:
            stripped = raw_line.strip()
            if not stripped or stripped == "...":
                continue
            if stripped == "---":
                if current:
                    blocks.append(current)
                    current = []
                continue

            if stripped.startswith("- ") and current and not any(
                ln.lstrip().startswith("- ") for ln in current
            ):
                blocks.append(current)
                current = [raw_line]
                continue

            if stripped.startswith("- ") and current and current[0].lstrip().startswith("- "):
                blocks.append(current)
                current = [raw_line]
                continue

            current.append(raw_line)

        if current:
            blocks.append(current)

        return ["\n".join(block) for block in blocks if any(line.strip() for line in block)]

    def _parse_yaml_block(self, block: str) -> dict[str, Any]:
        flat: dict[str, Any] = {}
        path_stack: list[tuple[int, str]] = []

        for raw_line in block.splitlines():
            line = raw_line.rstrip()
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            indent = len(line) - len(line.lstrip(" "))
            if stripped.startswith("- "):
                stripped = stripped[2:].strip()
                line = (" " * indent) + stripped

            while path_stack and indent <= path_stack[-1][0]:
                path_stack.pop()

            match = _KV_RE.match(stripped)
            if not match:
                continue

            key = match.group("key").strip().strip('"').strip("'")
            value = (match.group("value") or "").strip()
            dotted_key = ".".join([segment for _, segment in path_stack] + [key])

            if value == "":
                path_stack.append((indent, key))
                continue

            flat[dotted_key] = self._coerce_scalar(value)

        return flat

    def _coerce_scalar(self, value: str) -> Any:
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            return value[1:-1]
        lower = value.lower()
        if lower == "null":
            return None
        if lower == "true":
            return True
        if lower == "false":
            return False
        return value

    def _build_record(self, flat: dict[str, Any], raw: str, lineno: int, source: str) -> LogRecord:
        ts_raw = None
        ts_iso = None
        for key in _TS_KEYS:
            if key in flat and flat[key] not in (None, ""):
                ts_candidate = str(flat[key])
                dt = parse_timestamp(ts_candidate)
                if dt:
                    ts_raw = ts_candidate
                    ts_iso = to_iso8601(dt)
                    break

        level = None
        for key in _LVL_KEYS:
            if key in flat and flat[key] not in (None, ""):
                level = str(flat[key]).upper()
                if level == "WARNING":
                    level = "WARN"
                break

        message = ""
        for key in _MSG_KEYS:
            if key in flat and flat[key] not in (None, ""):
                message = clean_text(str(flat[key]))
                break

        if not message:
            fallback = next(
                (str(v) for v in flat.values() if isinstance(v, str) and v.strip()),
                "",
            )
            message = clean_text(fallback) if fallback else "yaml_record"

        if level is None:
            level = extract_level(message)

        reserved = _TS_KEYS | _LVL_KEYS | _MSG_KEYS
        fields = {k: v for k, v in flat.items() if k not in reserved}

        return self._normalize_record(
            LogRecord(
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
        )
