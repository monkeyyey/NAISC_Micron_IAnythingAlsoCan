"""
plaintext.py — Processor for unstructured plain-text / syslog-style logs.

Handles formats like:
    2024-01-15 10:23:01 ERROR  Application crashed unexpectedly
    Jan 15 10:23:01 myhost sshd[1234]: Failed password for root
    [ERROR] 2024-01-15T10:23:01Z Something went wrong
"""

from __future__ import annotations

import re
from typing import Iterator

from ..base_processor import BaseLogProcessor, LogRecord, extract_level
from ..normalizer import (
    clean_text,
    extract_timestamp,
    to_iso8601,
    normalize_whitespace,
)

# ---------------------------------------------------------------------------
# Patterns for well-known plain-text log styles
# ---------------------------------------------------------------------------

# Generic: optional brackets/level + timestamp + message
_GENERIC_RE = re.compile(
    r"""
    ^
    (?:\[?                                          # optional opening bracket
        (?P<level>TRACE|DEBUG|INFO|NOTICE|WARN(?:ING)?|ERROR|CRITICAL|FATAL|SEVERE)
    \]?\s+)?                                        # optional log level
    (?P<rest>.+)$
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Syslog: "Jan 15 10:23:01 hostname process[pid]: message"
_SYSLOG_RE = re.compile(
    r"""
    ^(?P<month>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+
    (?P<day>\d{1,2})\s+
    (?P<time>\d{2}:\d{2}:\d{2})\s+
    (?P<host>\S+)\s+
    (?P<process>\S+?)(?:\[(?P<pid>\d+)\])?:\s+
    (?P<message>.+)$
    """,
    re.VERBOSE,
)

# Apache / Nginx access log
_APACHE_RE = re.compile(
    r"""
    ^(?P<client>\S+)\s+           # client IP
    \S+\s+                         # ident (usually -)
    (?P<user>\S+)\s+               # authuser
    \[(?P<ts>[^\]]+)\]\s+          # [timestamp]
    "(?P<request>[^"]+)"\s+        # "request line"
    (?P<status>\d{3})\s+           # status code
    (?P<bytes>\S+)                 # bytes sent
    """,
    re.VERBOSE,
)


class PlainTextProcessor(BaseLogProcessor):
    """Parse unstructured plain-text log files line by line."""

    FORMAT = "plain_text"

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        for lineno, raw_line in enumerate(lines, start=1):
            corrupted, reason = self._is_corrupted(raw_line)
            if corrupted:
                yield self._make_corrupted(raw_line, lineno, source, reason)
                continue

            line = clean_text(raw_line)

            # Try specialised parsers first, fall back to generic
            rec = (
                self._try_syslog(line, raw_line, lineno, source)
                or self._try_apache(line, raw_line, lineno, source)
                or self._try_generic(line, raw_line, lineno, source)
            )
            yield rec

    # -----------------------------------------------------------------------
    # Format-specific sub-parsers
    # -----------------------------------------------------------------------

    def _try_syslog(self, line: str, raw: str, lineno: int, source: str) -> LogRecord | None:
        m = _SYSLOG_RE.match(line)
        if not m:
            return None
        ts_str = f"{m.group('month')} {m.group('day')} {m.group('time')}"
        from ..normalizer import parse_timestamp, to_iso8601
        from datetime import datetime, timezone
        dt = parse_timestamp(ts_str)
        iso = to_iso8601(dt) if dt else None

        rec = LogRecord(
            raw=raw, source=source, line_number=lineno, format=self.FORMAT,
            timestamp_raw=ts_str,
            timestamp=iso,
            message=m.group("message"),
            fields={
                "hostname": m.group("host"),
                "process":  m.group("process"),
                "pid":      int(m.group("pid")) if m.group("pid") else None,
            },
        )
        rec.level = extract_level(m.group("message"))
        return rec

    def _try_apache(self, line: str, raw: str, lineno: int, source: str) -> LogRecord | None:
        m = _APACHE_RE.match(line)
        if not m:
            return None
        ts_str = m.group("ts")
        from ..normalizer import parse_timestamp, to_iso8601
        dt = parse_timestamp(ts_str)
        iso = to_iso8601(dt) if dt else None

        # Parse request: "GET /path HTTP/1.1"
        request_parts = m.group("request").split()
        method = request_parts[0] if len(request_parts) >= 1 else ""
        path   = request_parts[1] if len(request_parts) >= 2 else ""

        status_code = int(m.group("status"))
        bytes_sent  = m.group("bytes")

        return LogRecord(
            raw=raw, source=source, line_number=lineno, format=self.FORMAT,
            timestamp_raw=ts_str,
            timestamp=iso,
            message=m.group("request"),
            level="ERROR" if status_code >= 500 else ("WARN" if status_code >= 400 else "INFO"),
            fields={
                "client":      m.group("client"),
                "user":        m.group("user"),
                "method":      method,
                "path":        path,
                "status_code": status_code,
                "bytes_sent":  int(bytes_sent) if bytes_sent.isdigit() else bytes_sent,
            },
        )

    def _try_generic(self, line: str, raw: str, lineno: int, source: str) -> LogRecord:
        ts_raw, dt = extract_timestamp(line)
        iso = to_iso8601(dt) if dt else None

        # Remove the timestamp token from the message if found
        message = line
        if ts_raw:
            message = line.replace(ts_raw, "", 1)
        message = normalize_whitespace(message)

        level = extract_level(message)
        # Strip the level token from the message too
        if level:
            message = re.sub(
                rf"\[?\b{level}\b\]?",
                "",
                message,
                flags=re.IGNORECASE,
                count=1,
            )
            message = normalize_whitespace(message)

        return LogRecord(
            raw=raw, source=source, line_number=lineno, format=self.FORMAT,
            timestamp_raw=ts_raw,
            timestamp=iso,
            message=message,
            level=level,
        )
