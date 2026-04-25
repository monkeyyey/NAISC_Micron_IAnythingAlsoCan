"""
detector.py — Automatic log format detection.

Inspects file headers, extensions, and content patterns to classify logs as:
    plain_text | key_value | delimiter | csv | tsv | json | xml | yaml | binary | syslog | logfmt
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
from pathlib import Path
from typing import Literal

LogFormat = Literal[
    "json",
    "xml",
    "yaml",
    "binary",
    "syslog",
    "logfmt",
    "csv",
    "tsv",
    "key_value",
    "delimiter",
    "plain_text",
]

# ---------------------------------------------------------------------------
# Scoring weights — each heuristic votes for a format; highest total wins
# ---------------------------------------------------------------------------

_WEIGHT_EXTENSION   = 40   # File extension is a strong signal
_WEIGHT_STRUCTURE   = 35   # Structural content test (parse attempt)
_WEIGHT_PATTERN     = 25   # Regex pattern match across sample lines

# Delimiter patterns
_KV_RE      = re.compile(r'[\w.\-/]+\s*[:=]\s*(?:"[^"]*"|\S+)')
_LOGFMT_RE  = re.compile(r'[\w.\-/@]+\s*=\s*(?:"[^"]*"|\'[^\']*\'|\S+)')
_PIPE_SEMI  = re.compile(r"[|;]")           # non-CSV/TSV delimiters
_CSV_LINE   = re.compile(r'[^,\n]*,[^,\n]*')
_TSV_LINE   = re.compile(r'[^\t\n]*\t[^\t\n]*')
_SYSLOG_RE  = re.compile(
    r'^(?:<\d{1,3}>)?(?:[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\S+|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})'
)
_XML_RE     = re.compile(r'^\s*<[^>]+>')
_YAML_RE    = re.compile(r'^\s*(?:---|-?\s*[\w.-]+\s*:\s*.+)$')


class LogFormatDetector:
    """Detect the format of a log source (file path, bytes, or text lines)."""

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def detect_from_path(self, path: str | Path) -> LogFormat:
        """Detect format using file extension + content sampling."""
        path = Path(path)
        sample_bytes = self._read_sample(path)
        extension_hint = path.suffix.lower().lstrip(".")
        return self._detect(sample_bytes, extension_hint=extension_hint)

    def detect_from_bytes(self, data: bytes, *, filename: str = "") -> LogFormat:
        """Detect format from raw bytes (e.g., uploaded file contents)."""
        ext = Path(filename).suffix.lower().lstrip(".") if filename else ""
        return self._detect(data, extension_hint=ext)

    def detect_from_lines(self, lines: list[str]) -> LogFormat:
        """Detect format from pre-split text lines."""
        sample = "\n".join(lines[:200]).encode("utf-8", errors="replace")
        return self._detect(sample)

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _read_sample(self, path: Path, max_bytes: int = 16_384) -> bytes:
        try:
            with open(path, "rb") as fh:
                return fh.read(max_bytes)
        except OSError:
            return b""

    def _detect(self, data: bytes, *, extension_hint: str = "") -> LogFormat:
        scores: dict[LogFormat, float] = {
            "json":        0,
            "xml":         0,
            "yaml":        0,
            "binary":      0,
            "syslog":      0,
            "logfmt":      0,
            "csv":         0,
            "tsv":         0,
            "key_value":   0,
            "delimiter":   0,
            "plain_text":  0,
        }

        # ---- Extension hint ------------------------------------------------
        ext_map: dict[str, LogFormat] = {
            "json": "json",
            "jsonl": "json",
            "ndjson": "json",
            "xml": "xml",
            "yaml": "yaml",
            "yml": "yaml",
            "bin": "binary",
            "dat": "binary",
            "syslog": "syslog",
            "logfmt": "logfmt",
            "csv": "csv",
            "tsv": "tsv",
            "txt": "plain_text",
            "log": "plain_text",
            "out": "plain_text",
        }
        if extension_hint in ext_map:
            scores[ext_map[extension_hint]] += _WEIGHT_EXTENSION

        # ---- Decode sample -------------------------------------------------
        try:
            text = data.decode("utf-8", errors="replace")
        except Exception:
            text = ""

        lines = [ln for ln in text.splitlines() if ln.strip()]
        sample_lines = lines[:50]

        # ---- Binary signal -------------------------------------------------
        scores["binary"] += self._score_binary(data, text)

        # ---- JSON structure test -------------------------------------------
        scores["json"] += self._score_json(text, sample_lines)

        # ---- XML / YAML structure tests -----------------------------------
        scores["xml"] += self._score_xml(text, sample_lines)
        scores["yaml"] += self._score_yaml(sample_lines)

        # ---- Syslog / Logfmt pattern tests --------------------------------
        scores["syslog"] += self._score_syslog(sample_lines)
        scores["logfmt"] += self._score_logfmt(sample_lines)

        # ---- CSV / TSV structure test --------------------------------------
        scores["csv"] += self._score_csv(text, sample_lines)
        scores["tsv"] += self._score_tsv(text, sample_lines)

        # ---- Key-Value pattern test ----------------------------------------
        scores["key_value"] += self._score_kv(sample_lines)

        # ---- Generic delimiter test ----------------------------------------
        scores["delimiter"] += self._score_delimiter(sample_lines)

        # Determine winner; plain_text is always a fallback
        scores["plain_text"] += 5  # small baseline so it's never zero
        best: LogFormat = max(scores, key=lambda f: scores[f])  # type: ignore[arg-type]
        return best

    # -----------------------------------------------------------------------
    # Per-format scorers
    # -----------------------------------------------------------------------

    def _score_json(self, text: str, lines: list[str]) -> float:
        stripped = text.strip()
        # Whole file is JSON array or object
        if (stripped.startswith("{") and stripped.endswith("}")) or \
           (stripped.startswith("[") and stripped.endswith("]")):
            try:
                json.loads(stripped)
                return _WEIGHT_STRUCTURE + _WEIGHT_PATTERN
            except json.JSONDecodeError:
                pass

        # NDJSON / JSON-Lines: majority of lines are valid JSON objects
        if not lines:
            return 0
        valid = sum(1 for ln in lines if self._is_json_object(ln))
        ratio = valid / len(lines)
        if ratio >= 0.8:
            return _WEIGHT_STRUCTURE + _WEIGHT_PATTERN * ratio
        if ratio >= 0.5:
            return _WEIGHT_PATTERN * ratio
        return 0

    def _score_xml(self, text: str, lines: list[str]) -> float:
        stripped = text.strip()
        if not stripped:
            return 0
        if stripped.startswith("<") and stripped.endswith(">"):
            try:
                import xml.etree.ElementTree as ET
                ET.fromstring(stripped if stripped.startswith("<") else f"<root>{stripped}</root>")
                return _WEIGHT_STRUCTURE + _WEIGHT_PATTERN
            except ET.ParseError:
                if lines and all(_XML_RE.match(line) for line in lines[: min(len(lines), 5)]):
                    return _WEIGHT_PATTERN * 0.7
        return 0

    def _score_yaml(self, lines: list[str]) -> float:
        if not lines:
            return 0
        yaml_hits = sum(1 for line in lines if _YAML_RE.match(line) and "=" not in line)
        ratio = yaml_hits / len(lines)
        if ratio >= 0.7:
            return _WEIGHT_STRUCTURE + _WEIGHT_PATTERN * ratio
        if ratio >= 0.3:
            return _WEIGHT_PATTERN * ratio
        return 0

    def _score_binary(self, data: bytes, text: str) -> float:
        if not data:
            return 0
        if b"\x00" in data:
            return _WEIGHT_STRUCTURE + 5
        if not text:
            return _WEIGHT_PATTERN
        non_printable = sum(
            1 for ch in text
            if ord(ch) < 32 and ch not in "\n\r\t" and ch != "\ufffd"
        )
        replacement = text.count("\ufffd")
        ratio = (non_printable + replacement) / max(len(text), 1)
        if ratio > 0.25:
            return _WEIGHT_STRUCTURE * ratio + _WEIGHT_PATTERN
        return 0

    def _score_syslog(self, lines: list[str]) -> float:
        if not lines:
            return 0
        hits = sum(1 for line in lines if _SYSLOG_RE.match(line))
        ratio = hits / len(lines)
        if ratio >= 0.7:
            return _WEIGHT_STRUCTURE + _WEIGHT_PATTERN * ratio
        if ratio >= 0.3:
            return _WEIGHT_PATTERN * ratio
        return 0

    def _score_logfmt(self, lines: list[str]) -> float:
        if not lines:
            return 0
        hits = sum(1 for line in lines if len(_LOGFMT_RE.findall(line)) >= 2)
        ratio = hits / len(lines)
        if ratio >= 0.7:
            return _WEIGHT_STRUCTURE + _WEIGHT_PATTERN * ratio
        if ratio >= 0.3:
            return _WEIGHT_PATTERN * ratio
        return 0

    @staticmethod
    def _is_json_object(line: str) -> bool:
        try:
            obj = json.loads(line.strip())
            return isinstance(obj, dict)
        except (json.JSONDecodeError, ValueError):
            return False

    def _score_csv(self, text: str, lines: list[str]) -> float:
        if not lines:
            return 0
        try:
            reader = csv.reader(io.StringIO(text))
            rows = [r for r, _ in zip(reader, range(20))]
            if len(rows) < 2:
                return 0
            col_counts = [len(r) for r in rows]
            # Penalise TSV-style (tabs dominate)
            tab_lines = sum(1 for ln in lines if "\t" in ln)
            if tab_lines / len(lines) > 0.6:
                return 0
            consistent = sum(1 for c in col_counts if c == col_counts[0])
            ratio = consistent / len(col_counts)
            base = _WEIGHT_STRUCTURE * ratio if col_counts[0] > 1 else 0
            # Pattern bonus
            pattern_hits = sum(1 for ln in lines if _CSV_LINE.search(ln))
            return base + _WEIGHT_PATTERN * (pattern_hits / len(lines))
        except Exception:
            return 0

    def _score_tsv(self, text: str, lines: list[str]) -> float:
        if not lines:
            return 0
        tab_lines = [ln for ln in lines if "\t" in ln]
        if not tab_lines:
            return 0
        ratio = len(tab_lines) / len(lines)
        col_counts = [ln.count("\t") + 1 for ln in tab_lines[:20]]
        consistency = sum(1 for c in col_counts if c == col_counts[0]) / len(col_counts)
        return (_WEIGHT_STRUCTURE * consistency + _WEIGHT_PATTERN) * ratio

    def _score_kv(self, lines: list[str]) -> float:
        if not lines:
            return 0
        hits = sum(
            1 for ln in lines
            if len(_KV_RE.findall(ln)) >= 2  # at least 2 KV pairs per line
        )
        ratio = hits / len(lines)
        if ratio >= 0.7:
            return _WEIGHT_STRUCTURE + _WEIGHT_PATTERN * ratio
        if ratio >= 0.3:
            return _WEIGHT_PATTERN * ratio
        return 0

    def _score_delimiter(self, lines: list[str]) -> float:
        if not lines:
            return 0
        hits = sum(1 for ln in lines if _PIPE_SEMI.search(ln))
        ratio = hits / len(lines)
        return _WEIGHT_PATTERN * ratio
