"""
base_processor.py — Abstract base class for all log processors.

Defines the canonical LogRecord dataclass and the processing pipeline
that every format-specific processor inherits and extends.
"""

from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator

from .normalizer import (
    clean_text,
    coerce_value,
    extract_timestamp,
    to_iso8601,
    normalize_kv_dict,
)

# ---------------------------------------------------------------------------
# Canonical log record
# ---------------------------------------------------------------------------

@dataclass
class LogRecord:
    """Canonical representation of a single cleaned log entry."""

    raw: str                              # Original unmodified line / chunk
    source: str = ""                      # File path or stream identifier
    line_number: int = 0                  # 1-based line number in source
    format: str = "unknown"              # Detected format name
    timestamp_raw: str | None = None     # Timestamp as found in the log
    timestamp: str | None = None         # ISO-8601 UTC timestamp
    message: str = ""                    # Primary human-readable content
    level: str | None = None             # Log level (INFO, WARN, ERROR, …)
    fields: dict[str, Any] = field(default_factory=dict)  # Structured KV data
    corrupted: bool = False              # True = record failed validation
    corruption_reason: str = ""          # Why the record was flagged
    record_id: str = ""                  # Deterministic hash for dedup

    def __post_init__(self) -> None:
        if not self.record_id:
            self.record_id = hashlib.sha1(
                f"{self.source}:{self.line_number}:{self.raw}".encode()
            ).hexdigest()[:16]

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id":      self.record_id,
            "source":         self.source,
            "line_number":    self.line_number,
            "format":         self.format,
            "timestamp":      self.timestamp,
            "timestamp_raw":  self.timestamp_raw,
            "level":          self.level,
            "message":        self.message,
            "fields":         self.fields,
            "corrupted":      self.corrupted,
            "corruption_reason": self.corruption_reason,
        }


# ---------------------------------------------------------------------------
# Log level detector
# ---------------------------------------------------------------------------

_LEVEL_RE = re.compile(
    r"\b(TRACE|DEBUG|INFO|NOTICE|WARN(?:ING)?|ERROR|CRITICAL|FATAL|SEVERE|ALERT|EMERGENCY)\b",
    re.IGNORECASE,
)


def extract_level(text: str) -> str | None:
    m = _LEVEL_RE.search(text)
    if not m:
        return None
    raw = m.group(1).upper()
    return "WARN" if raw == "WARNING" else raw


# ---------------------------------------------------------------------------
# Base processor
# ---------------------------------------------------------------------------

class BaseLogProcessor(ABC):
    """
    Abstract base for format-specific log processors.

    Sub-classes must implement ``_parse_lines`` which converts raw text lines
    into ``LogRecord`` instances.  The base class owns the common pipeline:

        load → split lines → parse → validate → normalize → yield records
    """

    FORMAT: str = "unknown"

    # Minimum printable characters for a line to be considered non-empty
    MIN_LINE_LENGTH: int = 3

    def process_file(self, path: str, *, source_label: str = "") -> Iterator[LogRecord]:
        """Load a file from disk and process it."""
        from pathlib import Path
        raw_bytes = Path(path).read_bytes()
        source = source_label or path
        yield from self.process_bytes(raw_bytes, source=source)

    def process_text(self, text: str, *, source: str = "stream") -> Iterator[LogRecord]:
        """Process a pre-decoded string."""
        yield from self._run_pipeline(text, source=source)

    def process_bytes(self, data: bytes, *, source: str = "upload") -> Iterator[LogRecord]:
        """Normalise encoding, then process."""
        from .normalizer import normalize_encoding
        text = normalize_encoding(data)
        yield from self._run_pipeline(text, source=source)

    def process_stream(self, stream, *, source: str = "stream") -> Iterator[LogRecord]:
        """Read from a file-like object and process."""
        raw = stream.read()
        if isinstance(raw, str):
            yield from self._run_pipeline(raw, source=source)
        else:
            yield from self.process_bytes(raw, source=source)

    # -----------------------------------------------------------------------
    # Pipeline
    # -----------------------------------------------------------------------

    def _run_pipeline(self, text: str, *, source: str) -> Iterator[LogRecord]:
        lines = self._split_lines(text)
        for record in self._parse_lines(lines, source=source):
            record = self._normalize_record(record)
            yield record

    def _split_lines(self, text: str) -> list[str]:
        """Split text into individual log lines, removing blank lines."""
        return [ln for ln in text.splitlines() if ln.strip()]

    @abstractmethod
    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        """Format-specific: parse raw lines into LogRecord instances."""

    # -----------------------------------------------------------------------
    # Shared normalisation applied to every record
    # -----------------------------------------------------------------------

    def _normalize_record(self, rec: LogRecord) -> LogRecord:
        # 1. Timestamp
        if rec.timestamp is None:
            ts_raw, dt = extract_timestamp(rec.message or rec.raw)
            if ts_raw and dt:
                rec.timestamp_raw = ts_raw
                rec.timestamp = to_iso8601(dt)

        # 2. Log level
        if rec.level is None:
            rec.level = extract_level(rec.message or rec.raw)

        # 3. Clean message
        if rec.message:
            rec.message = clean_text(rec.message)

        # 4. Coerce field values
        rec.fields = {
            k: (coerce_value(str(v)) if isinstance(v, str) else v)
            for k, v in rec.fields.items()
        }

        return rec

    # -----------------------------------------------------------------------
    # Validation helpers
    # -----------------------------------------------------------------------

    def _is_corrupted(self, line: str) -> tuple[bool, str]:
        """Return (corrupted, reason) for a raw line."""
        if len(line.strip()) < self.MIN_LINE_LENGTH:
            return True, "line_too_short"
        # Check for excessive replacement characters (encoding failure)
        replacement_ratio = line.count("\ufffd") / max(len(line), 1)
        if replacement_ratio > 0.2:
            return True, "encoding_error"
        # Check for non-printable character saturation
        non_printable = sum(
            1 for c in line
            if unicodedata.category(c).startswith("C") and c not in "\t\n\r"
        )
        if non_printable / max(len(line), 1) > 0.3:
            return True, "excessive_control_chars"
        return False, ""

    def _make_corrupted(self, raw: str, line_no: int, source: str, reason: str) -> LogRecord:
        return LogRecord(
            raw=raw,
            source=source,
            line_number=line_no,
            format=self.FORMAT,
            corrupted=True,
            corruption_reason=reason,
        )


# ---------------------------------------------------------------------------
# Re-export unicodedata (used in _is_corrupted) so submodule works standalone
# ---------------------------------------------------------------------------
import unicodedata  # noqa: E402  (intentional late import for clarity)
