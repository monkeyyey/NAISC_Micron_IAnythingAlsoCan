"""
syslog_proc.py — Processor for syslog logs.

Supports common RFC 3164 and RFC 5424 shapes:
    <34>Oct 11 22:14:15 host app[123]: alarm E334
    <165>1 2024-01-15T10:23:01Z host app 4521 ID47 - Message text
"""

from __future__ import annotations

import re
from typing import Iterator

from ..base_processor import BaseLogProcessor, LogRecord, extract_level
from ..normalizer import clean_text, normalize_whitespace, parse_timestamp, to_iso8601

_RFC3164_RE = re.compile(
    r"""
    ^
    (?:<(?P<pri>\d{1,3})>)?
    (?P<month>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+
    (?P<day>\d{1,2})\s+
    (?P<time>\d{2}:\d{2}:\d{2})\s+
    (?P<host>\S+)\s+
    (?P<app>[^\s:]+?)
    (?:\[(?P<pid>\d+)\])?
    :\s*
    (?P<message>.+)
    $
    """,
    re.VERBOSE,
)

_RFC5424_RE = re.compile(
    r"""
    ^
    <(?P<pri>\d{1,3})>
    (?P<version>\d+)\s+
    (?P<timestamp>\S+)\s+
    (?P<host>\S+)\s+
    (?P<app>\S+)\s+
    (?P<pid>\S+)\s+
    (?P<msgid>\S+)\s+
    (?P<structured_data>(?:-|\[[^\]]*\](?:\[[^\]]*\])*))\s*
    (?P<message>.*)
    $
    """,
    re.VERBOSE,
)

_SYSLOG_LEVEL_MAP = {
    0: "EMERGENCY",
    1: "ALERT",
    2: "CRITICAL",
    3: "ERROR",
    4: "WARN",
    5: "NOTICE",
    6: "INFO",
    7: "DEBUG",
}


class SyslogProcessor(BaseLogProcessor):
    """Parse syslog records into canonical ``LogRecord`` objects."""

    FORMAT = "syslog"

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        for lineno, raw_line in enumerate(lines, start=1):
            corrupted, reason = self._is_corrupted(raw_line)
            if corrupted:
                yield self._make_corrupted(raw_line, lineno, source, reason)
                continue

            line = clean_text(raw_line)
            rec = (
                self._try_rfc5424(line, raw_line, lineno, source)
                or self._try_rfc3164(line, raw_line, lineno, source)
            )
            if rec is None:
                yield self._make_corrupted(raw_line, lineno, source, "invalid_syslog")
                continue
            yield rec

    def _priority_level(self, pri_text: str | None) -> str | None:
        if not pri_text or not pri_text.isdigit():
            return None
        severity = int(pri_text) % 8
        return _SYSLOG_LEVEL_MAP.get(severity)

    def _try_rfc3164(self, line: str, raw: str, lineno: int, source: str) -> LogRecord | None:
        match = _RFC3164_RE.match(line)
        if not match:
            return None

        ts_raw = f"{match.group('month')} {match.group('day')} {match.group('time')}"
        dt = parse_timestamp(ts_raw)
        message = normalize_whitespace(match.group("message"))
        level = self._priority_level(match.group("pri")) or extract_level(message)

        return LogRecord(
            raw=raw,
            source=source,
            line_number=lineno,
            format=self.FORMAT,
            timestamp_raw=ts_raw,
            timestamp=to_iso8601(dt) if dt else None,
            message=message,
            level=level,
            fields={
                "priority": int(match.group("pri")) if match.group("pri") else None,
                "hostname": match.group("host"),
                "app_name": match.group("app"),
                "pid": int(match.group("pid")) if match.group("pid") else None,
            },
        )

    def _try_rfc5424(self, line: str, raw: str, lineno: int, source: str) -> LogRecord | None:
        match = _RFC5424_RE.match(line)
        if not match:
            return None

        ts_raw = match.group("timestamp")
        dt = parse_timestamp(ts_raw)
        message = normalize_whitespace(match.group("message"))
        level = self._priority_level(match.group("pri")) or extract_level(message)

        fields = {
            "priority": int(match.group("pri")),
            "version": int(match.group("version")),
            "hostname": match.group("host"),
            "app_name": None if match.group("app") == "-" else match.group("app"),
            "pid": None if match.group("pid") in {"-", ""} else match.group("pid"),
            "msgid": None if match.group("msgid") == "-" else match.group("msgid"),
        }
        structured_data = match.group("structured_data")
        if structured_data and structured_data != "-":
            fields["structured_data"] = structured_data

        return LogRecord(
            raw=raw,
            source=source,
            line_number=lineno,
            format=self.FORMAT,
            timestamp_raw=ts_raw,
            timestamp=to_iso8601(dt) if dt else None,
            message=message,
            level=level,
            fields=fields,
        )
