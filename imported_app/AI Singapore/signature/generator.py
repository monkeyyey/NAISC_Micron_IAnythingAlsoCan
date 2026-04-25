"""
signature/generator.py — Log signature generator.

A log signature is a stable, short hash that identifies a log *template*
(structure) independently of variable values.  Two lines with the same
structure but different sensor readings produce the same signature.

Variable token types replaced with placeholders:
  <NUM>   numeric values (integers and floats, including negative)
  <IP>    IPv4/IPv6 addresses
  <HEX>   hex literals (0x...)
  <TIME>  ISO timestamps and Unix epoch integers (10+ digit numbers)
  <UUID>  UUIDs
  <PATH>  file-system paths (/foo/bar, C:\foo\bar, ./relative)
"""

import hashlib
import re


# ---------------------------------------------------------------------------
# Compiled patterns — order matters (more specific patterns first)
# ---------------------------------------------------------------------------

_PAT_UUID = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)

_PAT_ISO_TIME = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?\b"
)

_PAT_UNIX_EPOCH = re.compile(r"\b\d{10,13}\b")   # 10-13 digit integers

_PAT_IP = re.compile(
    r"\b(?:\d{1,3}\.){3}\d{1,3}\b"               # IPv4
    r"|"
    r"\b(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}\b"  # IPv6 (simplified)
)

_PAT_HEX = re.compile(r"\b0[xX][0-9a-fA-F]+\b")

_PAT_PATH_UNIX  = re.compile(r"(?:^|(?<=\s))(?:/[\w.\-]+){2,}")
_PAT_PATH_WIN   = re.compile(r"[A-Za-z]:\\(?:[\w.\- ]+\\?)+")
_PAT_PATH_REL   = re.compile(r"(?:^|(?<=\s))\.{1,2}/[\w./\-]+")

_PAT_FLOAT = re.compile(r"-?\d+\.\d+(?:[eE][+-]?\d+)?")
_PAT_INT   = re.compile(r"-?\b\d+\b")

# Delimiter characters used for tokenisation (NOT removed from output)
_SPLIT_RE = re.compile(r"[\s,;|=:\"'()\[\]{}]+")


def _mask_variables(line: str) -> str:
    """Replace variable parts of a log line with canonical placeholders."""
    s = line

    s = _PAT_UUID.sub("<UUID>", s)
    s = _PAT_ISO_TIME.sub("<TIME>", s)
    s = _PAT_UNIX_EPOCH.sub("<TIME>", s)
    s = _PAT_IP.sub("<IP>", s)
    s = _PAT_HEX.sub("<HEX>", s)
    s = _PAT_PATH_WIN.sub("<PATH>", s)
    s = _PAT_PATH_UNIX.sub("<PATH>", s)
    s = _PAT_PATH_REL.sub("<PATH>", s)
    s = _PAT_FLOAT.sub("<NUM>", s)
    s = _PAT_INT.sub("<NUM>", s)

    return s


def generate_signature(line: str) -> str:
    """
    Generate a 16-character hex signature for a log line.

    The signature is stable across lines that share the same template
    but differ in numeric values, timestamps, IDs, and paths.

    Args:
        line: The raw log line (already stripped of leading/trailing whitespace).

    Returns:
        16-character lowercase hex string.
    """
    masked = _mask_variables(line.strip())
    # Collapse whitespace and split into tokens
    tokens = _SPLIT_RE.split(masked)
    # Remove empty tokens
    tokens = [t for t in tokens if t]
    stable = " ".join(tokens)
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()[:16]


def tokenise(line: str) -> list[str]:
    """
    Tokenise a log line by splitting on whitespace and common delimiters.
    Used by the trie and partitioner.
    """
    return [t for t in _SPLIT_RE.split(line.strip()) if t]


def generate_schema_fingerprint(parsed_row: dict) -> str:
    """
    Generate a 16-character hex fingerprint from the sorted field names of a
    parsed record dict.

    Two records with the same set of vendor field names (regardless of values
    or order) produce the same fingerprint.  This is the cache key used by
    the normalization_pipeline-style LLM mapping: one LLM call per unique
    vendor schema, not per record.

    Different from generate_signature() which fingerprints log line *content*
    (used for regex-pattern caching).  Use this when you have a structured
    dict and want to cache by schema shape.
    """
    canonical = "|".join(sorted(parsed_row.keys()))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
