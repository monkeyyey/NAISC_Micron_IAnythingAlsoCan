"""
parsing/format_router.py — Detect log file format and route to correct parser.

Detection priority (from the spec):
  1. File extension (fast path)
  2. Binary marker check (non-UTF-8 bytes → "binary")
  3. JSON detection
  4. XML detection
  5. YAML detection
  6. Syslog priority pattern
  7. Logfmt pattern
  8. Key-value pattern
  9. Delimiter pattern
  10. Default → "plaintext"
"""

import json
import logging
import os
import re

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = {
    "json", "xml", "csv", "tsv", "yaml", "syslog",
    "logfmt", "keyvalue", "delimiter", "plaintext", "binary",
}

# Extension → format (fast path — must be unambiguous)
_EXT_MAP = {
    ".json":  "json",
    ".xml":   "xml",
    ".csv":   "csv",
    ".tsv":   "tsv",
    ".yaml":  "yaml",
    ".yml":   "yaml",
    ".bin":   "binary",
}

# Patterns used in heuristic detection
_RE_XML_DECL  = re.compile(r"^\s*<\?xml", re.IGNORECASE)
_RE_XML_TAG   = re.compile(r"^\s*<[A-Za-z_][\w:.-]*[\s>]")
_RE_YAML_HDR  = re.compile(r"^---\s*$")
_RE_YAML_KEY  = re.compile(r"^\s*\w[\w\s]*:\s+\S")
_RE_SYSLOG    = re.compile(r"^<\d{1,3}>")
_RE_LOGFMT    = re.compile(r"(?:\w+=(?:\"[^\"]*\"|\S+)\s*){2,}")
_RE_KV_PAIR   = re.compile(r"\w+\s*[=:]\s*\S+")
_RE_DELIMITER = re.compile(r"^[^\n]+[|;][^\n]+$", re.MULTILINE)


def detect_format(file_path: str) -> str:
    """
    Detect the format of a log file.
    Returns one of the SUPPORTED_FORMATS strings.
    """
    # 1. Extension check (fast)
    _, ext = os.path.splitext(file_path.lower())
    if ext in _EXT_MAP:
        return _EXT_MAP[ext]

    # 2. Read first 1024 bytes and check for binary
    try:
        with open(file_path, "rb") as fh:
            raw = fh.read(1024)
    except OSError as exc:
        logger.warning("Cannot open file %s: %s", file_path, exc)
        return "binary"

    # Binary check: non-UTF-8 bytes or null bytes
    if b"\x00" in raw:
        return "binary"
    try:
        head = raw.decode("utf-8")
    except UnicodeDecodeError:
        return "binary"

    first_lines = head.splitlines()
    first_line = first_lines[0].strip() if first_lines else ""
    sample = "\n".join(first_lines[:20])

    return _detect_from_content(first_line, sample)


def detect_format_from_content(content: str) -> str:
    """
    Detect format from a string (used when file path is not available,
    e.g. streaming ingestion).
    """
    lines = content.splitlines()
    first_line = lines[0].strip() if lines else ""
    sample = "\n".join(lines[:20])
    return _detect_from_content(first_line, sample)


def _detect_from_content(first_line: str, sample: str) -> str:
    # 3. JSON
    if first_line.startswith("{") or first_line.startswith("["):
        try:
            json.loads(first_line)
            return "json"
        except json.JSONDecodeError:
            pass
        # Newline-delimited JSON: try a few lines
        for line in sample.splitlines():
            stripped = line.strip()
            if stripped.startswith("{"):
                try:
                    json.loads(stripped)
                    return "json"
                except json.JSONDecodeError:
                    pass

    # 4. XML
    if _RE_XML_DECL.match(first_line) or _RE_XML_TAG.match(first_line):
        return "xml"

    # 5. YAML
    if _RE_YAML_HDR.match(first_line):
        return "yaml"
    yaml_key_count = sum(1 for l in sample.splitlines() if _RE_YAML_KEY.match(l))
    if yaml_key_count >= 3:
        return "yaml"

    # 6. Syslog
    if _RE_SYSLOG.match(first_line):
        return "syslog"

    # 7. Logfmt — multiple key=value pairs separated by spaces, no other delimiters
    logfmt_lines = [l for l in sample.splitlines() if l.strip()]
    logfmt_count = sum(
        1 for l in logfmt_lines
        if _RE_LOGFMT.search(l) and "|" not in l and ";" not in l
    )
    if logfmt_count >= max(1, len(logfmt_lines) // 2):
        return "logfmt"

    # 8. Key-value (mixed = and : separators, more varied structure)
    kv_count = sum(
        1 for l in sample.splitlines()
        if len(_RE_KV_PAIR.findall(l)) >= 2
    )
    if kv_count >= max(1, len([l for l in sample.splitlines() if l.strip()]) // 2):
        return "keyvalue"

    # 9. Delimiter (|, ;) — consistent non-comma delimiter
    delim_lines = [l for l in sample.splitlines() if l.strip()]
    if delim_lines:
        pipe_count = sum(1 for l in delim_lines if "|" in l)
        semi_count = sum(1 for l in delim_lines if ";" in l)
        if pipe_count >= len(delim_lines) // 2 or semi_count >= len(delim_lines) // 2:
            return "delimiter"

    # 10. Default
    return "plaintext"


def route_to_parser(format_type: str, content: str) -> list[dict]:
    """
    Dispatch to the correct parser for format_type.
    Returns list of raw dicts with vendor field names intact.
    Binary files return an empty list (they should not reach this function).
    """
    # Lazy imports to avoid circular dependencies at module load time
    from parsing.structured_parser import (
        parse_json, parse_xml, parse_csv, parse_tsv, parse_yaml, parse_logfmt,
    )
    from parsing.unstructured_parser import (
        parse_keyvalue, parse_delimiter, parse_plaintext,
    )
    from parsing.syslog_parser import parse_syslog

    lines = content.splitlines()

    dispatch = {
        "json":      lambda: parse_json(content),
        "xml":       lambda: parse_xml(content),
        "csv":       lambda: parse_csv(content),
        "tsv":       lambda: parse_tsv(content),
        "yaml":      lambda: parse_yaml(content),
        "logfmt":    lambda: parse_logfmt(lines),
        "syslog":    lambda: parse_syslog(lines),
        "keyvalue":  lambda: parse_keyvalue(lines),
        "delimiter": lambda: parse_delimiter(lines),
        "plaintext": lambda: parse_plaintext(lines),
        "binary":    lambda: [],
    }

    parser_fn = dispatch.get(format_type, lambda: parse_plaintext(lines))
    try:
        return parser_fn()
    except Exception as exc:
        logger.error("Parser error for format %s: %s", format_type, exc)
        return []
