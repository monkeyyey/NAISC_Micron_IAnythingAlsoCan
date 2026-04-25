"""
parsing/syslog_parser.py — RFC 5424 and BSD syslog format parsers.

RFC 5424: <PRIORITY>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG
BSD:      <PRIORITY>MON DD HH:MM:SS HOSTNAME PROCESS[PID]: MESSAGE
"""

import logging
import re

logger = logging.getLogger(__name__)

# Facility names (RFC 3164)
_FACILITY_NAMES = [
    "kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
    "uucp", "cron", "security", "ftp", "ntp", "logaudit", "logalert",
    "clock", "local0", "local1", "local2", "local3", "local4", "local5",
    "local6", "local7",
]

# Severity names
_SEVERITY_NAMES = [
    "emergency", "alert", "critical", "error",
    "warning", "notice", "informational", "debug",
]

# RFC 5424 full format
# <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG
_RFC5424_RE = re.compile(
    r"^<(\d{1,3})>"                      # 1: priority
    r"(\d+)\s+"                          # 2: version
    r"(\S+)\s+"                          # 3: timestamp
    r"(\S+)\s+"                          # 4: hostname
    r"(\S+)\s+"                          # 5: app-name
    r"(\S+)\s+"                          # 6: procid
    r"(\S+)\s+"                          # 7: msgid
    r"(\[.*?\]|-)\s*"                    # 8: structured-data
    r"(.*)?$"                            # 9: message
)

# BSD syslog format
# <PRI>MON DD HH:MM:SS HOSTNAME PROCESS[PID]: MESSAGE
_BSD_RE = re.compile(
    r"^<(\d{1,3})>"                             # 1: priority
    r"([A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+"  # 2: timestamp
    r"(\S+)\s+"                                  # 3: hostname
    r"(\S+?)(?:\[(\d+)\])?:\s*"                 # 4: process, 5: pid
    r"(.*)?$"                                    # 6: message
)

# <PRI>ISO-TIMESTAMP HOSTNAME PROCESS[PID]: MESSAGE
# e.g. <34>2024-04-15T12:03:20Z ETCH01 tool-monitor[1234]: ALARM E334
_PRI_ISO_RE = re.compile(
    r"^<(\d{1,3})>"                                      # 1: priority
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)\s+"  # 2: ISO timestamp
    r"(\S+)\s+"                                          # 3: hostname
    r"(\S+?)(?:\[(\d+)\])?:\s*"                         # 4: process, 5: pid
    r"(.*)?$"                                            # 6: message
)

# Syslog without priority tag (common in some equipment)
_NOPRI_RE = re.compile(
    r"^([A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})"  # 1: timestamp
    r"\s+(\S+)"                                          # 2: hostname
    r"\s+(\S+?)(?:\[(\d+)\])?:\s*"                     # 3: process, 4: pid
    r"(.*)?$"                                            # 5: message
)


def _decode_priority(priority: int) -> tuple[int, int]:
    """Decode syslog priority into (facility_code, severity_code)."""
    facility = priority >> 3
    severity = priority & 0x07
    return facility, severity


def parse_syslog(lines: list[str]) -> list[dict]:
    """
    Parse RFC 5424 and BSD syslog lines.
    Returns list of dicts with extracted fields.
    Falls back to raw_text for lines that don't match.
    """
    records = []
    for line in lines:
        line = line.strip()
        if not line:
            continue

        record = _parse_syslog_line(line)
        records.append(record)

    return records


def _parse_syslog_line(line: str) -> dict:
    # Try RFC 5424
    m = _RFC5424_RE.match(line)
    if m:
        priority = int(m.group(1))
        facility, severity = _decode_priority(priority)
        return {
            "priority":   priority,
            "facility":   _FACILITY_NAMES[facility] if facility < len(_FACILITY_NAMES) else str(facility),
            "severity":   _SEVERITY_NAMES[severity] if severity < len(_SEVERITY_NAMES) else str(severity),
            "timestamp":  m.group(3) if m.group(3) != "-" else None,
            "hostname":   m.group(4) if m.group(4) != "-" else None,
            "app_name":   m.group(5) if m.group(5) != "-" else None,
            "process_id": m.group(6) if m.group(6) != "-" else None,
            "message_id": m.group(7) if m.group(7) != "-" else None,
            "message":    m.group(9) or "",
            "raw_text":   line,
        }

    # Try BSD syslog
    m = _BSD_RE.match(line)
    if m:
        priority = int(m.group(1))
        facility, severity = _decode_priority(priority)
        return {
            "priority":   priority,
            "facility":   _FACILITY_NAMES[facility] if facility < len(_FACILITY_NAMES) else str(facility),
            "severity":   _SEVERITY_NAMES[severity] if severity < len(_SEVERITY_NAMES) else str(severity),
            "timestamp":  m.group(2),
            "hostname":   m.group(3),
            "app_name":   m.group(4),
            "process_id": m.group(5),
            "message":    m.group(6) or "",
            "raw_text":   line,
        }

    # Try <PRI>ISO-TIMESTAMP HOSTNAME PROCESS[PID]: MESSAGE
    m = _PRI_ISO_RE.match(line)
    if m:
        priority = int(m.group(1))
        facility, severity = _decode_priority(priority)
        return {
            "priority":   priority,
            "facility":   _FACILITY_NAMES[facility] if facility < len(_FACILITY_NAMES) else str(facility),
            "severity":   _SEVERITY_NAMES[severity] if severity < len(_SEVERITY_NAMES) else str(severity),
            "timestamp":  m.group(2),
            "hostname":   m.group(3),
            "app_name":   m.group(4),
            "process_id": m.group(5),
            "message":    m.group(6) or "",
            "raw_text":   line,
        }

    # Try without priority
    m = _NOPRI_RE.match(line)
    if m:
        return {
            "timestamp":  m.group(1),
            "hostname":   m.group(2),
            "app_name":   m.group(3),
            "process_id": m.group(4),
            "message":    m.group(5) or "",
            "raw_text":   line,
        }

    # No pattern matched — return as plaintext
    return {"raw_text": line}
