"""
parsing/unstructured_parser.py — Parsers for semi-structured and plain text logs.

Key-value:   key=value or key:value pairs (mixed content)
Delimiter:   consistent non-comma separator (| or ;)
Plaintext:   raw text lines — returned as {"raw_text": line} for trie matching
"""

import logging
import re

logger = logging.getLogger(__name__)

# Match key=value or key:value, where value is non-whitespace/non-separator
# Keys must start with a letter or underscore
_KV_RE = re.compile(
    r"([A-Za-z_][\w.]*)"   # key
    r"\s*[=:]\s*"           # separator
    r"([^\s,;|\"']+|\"[^\"]*\"|'[^']*')"  # value (bare or quoted)
)

# URL and timestamp patterns to protect from key: splitting
_URL_RE       = re.compile(r"https?://\S+")
_TIMESTAMP_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
    r"|\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}:\d{2}"
    r"|\d{2}-[A-Za-z]{3}-\d{4}\s\d{2}:\d{2}:\d{2}"
)


def parse_keyvalue(lines: list[str]) -> list[dict]:
    """
    Parse lines that contain key=value or key:value pairs.
    Protects timestamps and URLs from erroneous splitting.
    Each non-empty line → one record dict.
    """
    records = []
    for line in lines:
        line = line.strip()
        if not line:
            continue

        record: dict = {}
        matches = _KV_RE.findall(line)

        for key, value in matches:
            # Strip surrounding quotes from values
            if (value.startswith('"') and value.endswith('"')) or \
               (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            record[key] = value if value else None

        if not record:
            record["raw_text"] = line
        else:
            record["_raw_kv_line"] = line  # Keep raw for trie matching

        records.append(record)

    return records


def parse_delimiter(lines: list[str]) -> list[dict]:
    """
    Parse lines with a consistent delimiter (| or ;).
    If the first line looks like a header (all non-numeric tokens), use it as keys.
    Otherwise generate positional names field_0, field_1, etc.
    """
    if not lines:
        return []

    # Detect the dominant delimiter
    delimiter = _detect_delimiter(lines)

    data_lines = [l for l in lines if l.strip()]
    if not data_lines:
        return []

    headers: list[str] | None = None
    start_idx = 0

    # Heuristic: if the first line has no numeric-only fields and
    # the second line has numeric values, treat first as header
    if len(data_lines) >= 2:
        first_fields = [f.strip() for f in data_lines[0].split(delimiter)]
        second_fields = [f.strip() for f in data_lines[1].split(delimiter)]
        first_numeric_count = sum(1 for f in first_fields if _is_numeric(f))
        if first_numeric_count == 0 and any(_is_numeric(f) for f in second_fields):
            headers = first_fields
            start_idx = 1

    records = []
    for line in data_lines[start_idx:]:
        if not line.strip():
            continue
        fields = [f.strip() for f in line.split(delimiter)]
        if headers:
            record = {
                headers[i] if i < len(headers) else f"field_{i}": v
                for i, v in enumerate(fields)
            }
        else:
            record = {f"field_{i}": v for i, v in enumerate(fields)}
        records.append(record)

    return records


def parse_plaintext(lines: list[str]) -> list[dict]:
    """
    Plain text lines are passed through as {"raw_text": line}.
    The trie and regex engine handle extraction downstream.
    """
    records = []
    for line in lines:
        line = line.strip()
        if line:
            records.append({"raw_text": line})
    return records


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _detect_delimiter(lines: list[str]) -> str:
    """Return the most common delimiter character among | and ;."""
    pipe_count = sum(l.count("|") for l in lines)
    semi_count = sum(l.count(";") for l in lines)
    return "|" if pipe_count >= semi_count else ";"


def _is_numeric(value: str) -> bool:
    """True if the value string looks like a pure number."""
    try:
        float(value)
        return True
    except ValueError:
        return False
