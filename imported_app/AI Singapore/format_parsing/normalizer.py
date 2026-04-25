"""
normalizer.py — Shared normalization utilities for all log processors.
Handles encoding, timestamps, whitespace, units, numerics, and control characters.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------

# Ordered by frequency of occurrence in real-world logs
_ENCODING_CANDIDATES = [
    "utf-8",
    "utf-8-sig",  # UTF-8 with BOM
    "latin-1",
    "windows-1252",
    "ascii",
    "utf-16",
]


def normalize_encoding(raw: bytes) -> str:
    """Decode raw bytes to a clean UTF-8 string.

    Tries a ranked list of encodings; falls back to UTF-8 with replacement.
    """
    for enc in _ENCODING_CANDIDATES:
        try:
            text = raw.decode(enc)
            # Re-encode to UTF-8 and decode to normalise the string
            return text.encode("utf-8").decode("utf-8")
        except (UnicodeDecodeError, LookupError):
            continue
    # Last resort: decode with replacement characters
    return raw.decode("utf-8", errors="replace")


def ensure_utf8(text: str) -> str:
    """Ensure a string is valid UTF-8 by round-tripping through bytes."""
    return text.encode("utf-8", errors="replace").decode("utf-8")


# ---------------------------------------------------------------------------
# Whitespace & control characters
# ---------------------------------------------------------------------------

# Matches ASCII / Unicode control characters except tab (\x09) and newline (\x0a/\x0d)
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
# Consecutive whitespace (but not newlines) collapsed to a single space
_MULTI_SPACE_RE = re.compile(r"[ \t]+")
# ANSI escape sequences (colour codes, cursor moves, etc.)
_ANSI_RE = re.compile(r"\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")


def strip_control_characters(text: str) -> str:
    """Remove ASCII control characters and ANSI escape codes."""
    text = _ANSI_RE.sub("", text)
    text = _CTRL_RE.sub("", text)
    return text


def normalize_whitespace(text: str) -> str:
    """Collapse runs of spaces/tabs; strip leading/trailing whitespace."""
    text = _MULTI_SPACE_RE.sub(" ", text)
    return text.strip()


def clean_text(text: str) -> str:
    """Apply encoding safety + control-char removal + whitespace normalisation."""
    text = ensure_utf8(text)
    text = strip_control_characters(text)
    text = normalize_whitespace(text)
    return text


# ---------------------------------------------------------------------------
# Timestamp normalisation
# ---------------------------------------------------------------------------

# Format strings tried in order when parsing timestamp strings
_TIMESTAMP_FORMATS = [
    # ISO variants
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    # Common log formats
    "%d/%b/%Y:%H:%M:%S %z",   # Apache CLF
    "%b %d %H:%M:%S",         # Syslog (no year)
    "%b %d %Y %H:%M:%S",      # Syslog with year
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%m-%d-%Y %H:%M:%S",
    # Date-only fallbacks
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
]

# Regex that broadly captures a timestamp-shaped token
_TS_PATTERN = re.compile(
    r"""
    (?:
        \d{4}[-/]\d{2}[-/]\d{2}       # YYYY-MM-DD or YYYY/MM/DD
        (?:[T\s]\d{2}:\d{2}:\d{2}     # optional time
            (?:\.\d+)?                 # optional fractional seconds
            (?:Z|[+-]\d{2}:?\d{2})?   # optional tz
        )?
    |
        \d{2}[-/]\d{2}[-/]\d{4}       # DD-MM-YYYY or MM/DD/YYYY
        (?:\s\d{2}:\d{2}:\d{2})?
    |
        (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}
        (?:\s+\d{4})?
        \s+\d{2}:\d{2}:\d{2}
        (?:\s+[+-]\d{4})?             # Apache CLF timezone
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)


def parse_timestamp(ts_str: str) -> datetime | None:
    """Try to parse *ts_str* into a timezone-aware UTC datetime. Returns None on failure."""
    ts_str = ts_str.strip()
    for fmt in _TIMESTAMP_FORMATS:
        try:
            dt = datetime.strptime(ts_str, fmt)
            # Make timezone-aware (assume UTC if naïve)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            # Patch missing year (syslog without year)
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now(timezone.utc).year)
            return dt
        except ValueError:
            continue
    return None


def extract_timestamp(text: str) -> tuple[str | None, datetime | None]:
    """Find and parse the first timestamp-shaped token inside *text*.

    Returns ``(raw_token, parsed_datetime)`` or ``(None, None)``.
    """
    match = _TS_PATTERN.search(text)
    if not match:
        return None, None
    raw = match.group(0)
    parsed = parse_timestamp(raw)
    return raw, parsed


def to_iso8601(dt: datetime) -> str:
    """Format a datetime as ISO-8601 UTC string."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ---------------------------------------------------------------------------
# Units removal
# ---------------------------------------------------------------------------

# Matches a numeric portion followed by a unit suffix
# Supports: 72C, 3.14ms, 512MB, 100%, 5s, 1.2GHz, -40.5°C
_UNIT_RE = re.compile(
    r"(-?\d+(?:\.\d+)?)"         # numeric part (group 1)
    r"\s*"
    r"(?:°)?"                     # optional degree symbol
    r"(?:"
    r"[KMGTPE]?[Bb](?:ytes?)?|" # B, KB, MB, GB, TB, PB, EB
    r"[KMGTPE]?[Bb]ps?|"        # bps, Mbps, Gbps
    r"[kmµunp]?(?:s|ms|us|ns)|" # time units
    r"[km]?Hz|[KMGT]?Hz|"       # frequency
    r"[kmMG]?W|"                 # power
    r"[mMkKuU]?[aAvV]|"         # current / voltage
    r"[°℃℉]?[CF]|"              # temperature
    r"rpm|dB|lx|Pa|psi|bar|"    # misc
    r"%"                          # percentage
    r")"
    r"\b",
    re.IGNORECASE,
)


def remove_units(value: str) -> str:
    """Strip measurement units from a value string, keeping the numeric part.

    Example: ``'72C'`` → ``'72'``, ``'3.14ms'`` → ``'3.14'``, ``'100%'`` → ``'100'``
    """
    return _UNIT_RE.sub(r"\1", value).strip()


# ---------------------------------------------------------------------------
# Numeric coercion
# ---------------------------------------------------------------------------

def try_numeric(value: str) -> int | float | str:
    """Try to convert *value* to int or float; return original string on failure."""
    stripped = value.strip()
    # Remove thousands separators before parsing
    cleaned = stripped.replace(",", "")
    try:
        i = int(cleaned)
        return i
    except ValueError:
        pass
    try:
        f = float(cleaned)
        return f
    except ValueError:
        pass
    return value


def coerce_value(value: str, *, remove_unit: bool = True) -> Any:
    """Full value pipeline: strip → remove units → numeric coercion."""
    v = value.strip()
    if remove_unit:
        v = remove_units(v)
    return try_numeric(v)


# ---------------------------------------------------------------------------
# Key-Value normalisation
# ---------------------------------------------------------------------------

# Matches key=value or key:value pairs (quoted or unquoted values)
_KV_RE = re.compile(
    r'([\w.\-/]+)'                          # key (group 1)
    r'\s*[:=]\s*'                           # separator
    r'(?:"([^"]*?)"|\'([^\']*?)\'|(\S+))',  # value: "dq", 'sq', or bare (groups 2-4)
)


def parse_kv_pairs(text: str) -> dict[str, str]:
    """Extract key-value pairs from a log line supporting both ``=`` and ``:`` separators."""
    result: dict[str, str] = {}
    for m in _KV_RE.finditer(text):
        key = m.group(1)
        # Pick whichever value group matched
        value = m.group(2) or m.group(3) or m.group(4) or ""
        result[key] = value
    return result


def normalize_kv_dict(raw_kv: dict[str, str], *, remove_unit: bool = True) -> dict[str, Any]:
    """Clean keys (lowercase, strip) and coerce values for a KV dictionary."""
    return {
        k.lower().strip(): coerce_value(v, remove_unit=remove_unit)
        for k, v in raw_kv.items()
    }


# ---------------------------------------------------------------------------
# Delimiter normalisation
# ---------------------------------------------------------------------------

# Characters treated as equivalent delimiters
_MULTI_DELIM_RE = re.compile(r"[|;,\t]+")


def normalize_delimiter(line: str, target: str = ",") -> str:
    """Replace any mix of ``|``, ``;``, ``,``, ``\t`` with *target*."""
    return _MULTI_DELIM_RE.sub(target, line)
