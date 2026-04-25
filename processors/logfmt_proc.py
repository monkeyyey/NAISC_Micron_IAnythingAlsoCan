"""
logfmt_proc.py — Processor for logfmt logs.

Logfmt is a stricter key=value format than the generic key-value processor:
    ts=2024-01-15T10:23:01Z level=info msg="service started" host=app-01
"""

from __future__ import annotations

import re
from typing import Iterator

from ..base_processor import LogRecord
from .keyvalue import KeyValueProcessor

_STRICT_LOGFMT_RE = re.compile(
    r'(?P<key>[\w.\-/@]+)\s*=\s*'
    r'(?:"(?P<dq>[^"\\]*(?:\\.[^"\\]*)*)"|'
    r"'(?P<sq>[^'\\]*(?:\\.[^'\\]*)*)'|"
    r'(?P<bare>[^\s,;]+))'
)


class LogfmtProcessor(KeyValueProcessor):
    """Parse strict logfmt records using only ``=`` separators."""

    FORMAT = "logfmt"

    def _extract_kv_pairs(self, line: str) -> dict[str, str]:
        pairs: dict[str, str] = {}
        for match in _STRICT_LOGFMT_RE.finditer(line):
            key = match.group("key").lower().strip()
            value = match.group("dq") or match.group("sq") or match.group("bare") or ""
            pairs[key] = value
        return pairs

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        yield from super()._parse_lines(lines, source=source)
