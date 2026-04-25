"""
ingestion.py — LogIngestionService: the unified entry point for the pipeline.

Responsibilities:
    1. Accept log data from files, bytes, text strings, or streams
    2. Auto-detect format (or accept explicit override)
    3. Route to the correct processor
    4. Push cleaned records into the StagingArea
    5. Return processing results / statistics

Usage:
    service = LogIngestionService()

    # From file (format auto-detected)
    result = service.ingest_file("app.log")

    # From uploaded bytes (e.g., HTTP multipart)
    result = service.ingest_bytes(file_bytes, filename="events.json")

    # Streaming line-by-line
    for record in service.stream_file("large.log"):
        print(record.to_dict())
"""

from __future__ import annotations

import io
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Literal, TextIO

from .base_processor import LogRecord
from .detector import LogFormatDetector, LogFormat
from .staging import StagingArea
from .processors import (
    PlainTextProcessor,
    KeyValueProcessor,
    LogfmtProcessor,
    DelimiterProcessor,
    CSVProcessor,
    TSVProcessor,
    JSONProcessor,
    XMLProcessor,
    YAMLProcessor,
    BinaryProcessor,
    SyslogProcessor,
)

# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class IngestionResult:
    """Summary returned after processing a log source."""
    source: str
    detected_format: str
    total_records: int = 0
    clean_records: int = 0
    corrupted_records: int = 0
    duration_seconds: float = 0.0
    staging: StagingArea | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return self.clean_records / self.total_records

    def __str__(self) -> str:
        return (
            f"IngestionResult("
            f"source='{self.source}', "
            f"format='{self.detected_format}', "
            f"total={self.total_records:,}, "
            f"clean={self.clean_records:,}, "
            f"corrupted={self.corrupted_records:,}, "
            f"success_rate={self.success_rate:.1%}, "
            f"duration={self.duration_seconds:.3f}s)"
        )


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class LogIngestionService:
    """
    Unified log ingestion service.

    Supports all configured log formats via auto-detection or explicit format override.
    """

    # Maps format names to processor factories
    _PROCESSOR_MAP = {
        "json":        JSONProcessor,
        "xml":         XMLProcessor,
        "yaml":        YAMLProcessor,
        "binary":      BinaryProcessor,
        "syslog":      SyslogProcessor,
        "logfmt":      LogfmtProcessor,
        "csv":         CSVProcessor,
        "tsv":         TSVProcessor,
        "key_value":   KeyValueProcessor,
        "delimiter":   DelimiterProcessor,
        "plain_text":  PlainTextProcessor,
    }

    def __init__(
        self,
        staging: StagingArea | None = None,
        processor_kwargs: dict[str, dict] | None = None,
    ):
        """
        Args:
            staging:          Shared StagingArea. A fresh one is created if None.
            processor_kwargs: Per-format constructor kwargs.
                              e.g. ``{"csv": {"has_header": True}}``
        """
        self.staging = staging if staging is not None else StagingArea()
        self.detector = LogFormatDetector()
        self._proc_kwargs = processor_kwargs or {}

    # -----------------------------------------------------------------------
    # Public API — ingestion methods
    # -----------------------------------------------------------------------

    def ingest_file(
        self,
        path: str | Path,
        *,
        format_override: LogFormat | None = None,
        source_label: str = "",
    ) -> IngestionResult:
        """Load and process a log file from disk."""
        path = Path(path)
        source = source_label or str(path)
        raw_bytes = path.read_bytes()
        fmt = format_override or self.detector.detect_from_path(path)
        return self._run(raw_bytes, fmt=fmt, source=source)

    def ingest_bytes(
        self,
        data: bytes,
        *,
        filename: str = "",
        format_override: LogFormat | None = None,
        source_label: str = "",
    ) -> IngestionResult:
        """Process raw bytes (e.g., from an HTTP file upload or S3 read)."""
        source = source_label or filename or "upload"
        fmt = format_override or self.detector.detect_from_bytes(data, filename=filename)
        return self._run(data, fmt=fmt, source=source)

    def ingest_text(
        self,
        text: str,
        *,
        format_override: LogFormat | None = None,
        source_label: str = "text_input",
    ) -> IngestionResult:
        """Process a string directly (e.g., from an API body or in-memory buffer)."""
        data = text.encode("utf-8")
        fmt = format_override or self.detector.detect_from_bytes(data)
        return self._run(data, fmt=fmt, source=source_label)

    def ingest_stream(
        self,
        stream: BinaryIO | TextIO,
        *,
        format_override: LogFormat | None = None,
        source_label: str = "stream",
        chunk_lines: int = 1000,
    ) -> IngestionResult:
        """
        Process a file-like stream, reading in line-based chunks.

        Args:
            chunk_lines: Lines to buffer per batch (for memory efficiency).
        """
        raw = stream.read()
        if isinstance(raw, str):
            data = raw.encode("utf-8")
        else:
            data = raw
        fmt = format_override or self.detector.detect_from_bytes(data, filename=source_label)
        return self._run(data, fmt=fmt, source=source_label)

    def stream_file(
        self,
        path: str | Path,
        *,
        format_override: LogFormat | None = None,
        include_corrupted: bool = False,
    ) -> Iterator[LogRecord]:
        """
        Lazy iterator over records from a file — memory-efficient for large logs.

        Yields individual LogRecord objects without staging.
        """
        path = Path(path)
        raw_bytes = path.read_bytes()
        fmt = format_override or self.detector.detect_from_path(path)
        processor = self._get_processor(fmt)
        for rec in processor.process_bytes(raw_bytes, source=str(path)):
            if include_corrupted or not rec.corrupted:
                yield rec

    def ingest_directory(
        self,
        directory: str | Path,
        *,
        pattern: str = "**/*",
        format_override: LogFormat | None = None,
        recursive: bool = True,
    ) -> dict[str, IngestionResult]:
        """
        Process all matching files in a directory.

        Returns a dict of ``{filepath: IngestionResult}``.
        """
        directory = Path(directory)
        glob_fn = directory.rglob if recursive else directory.glob
        results: dict[str, IngestionResult] = {}
        for file_path in glob_fn(pattern):
            if file_path.is_file():
                try:
                    results[str(file_path)] = self.ingest_file(
                        file_path, format_override=format_override
                    )
                except Exception as exc:
                    results[str(file_path)] = IngestionResult(
                        source=str(file_path),
                        detected_format="unknown",
                        errors=[str(exc)],
                    )
        return results

    # -----------------------------------------------------------------------
    # Internal
    # -----------------------------------------------------------------------

    def _run(self, data: bytes, *, fmt: str, source: str) -> IngestionResult:
        start = time.monotonic()
        result = IngestionResult(source=source, detected_format=fmt)

        try:
            processor = self._get_processor(fmt)
            records = list(processor.process_bytes(data, source=source))
            result.total_records = len(records)
            result.clean_records = sum(1 for r in records if not r.corrupted)
            result.corrupted_records = sum(1 for r in records if r.corrupted)
            self.staging.add_many(records)
            result.staging = self.staging
        except Exception as exc:
            result.errors.append(f"Pipeline error: {exc}")

        result.duration_seconds = time.monotonic() - start
        return result

    def _get_processor(self, fmt: str):
        """Instantiate the correct processor for *fmt* with any user-provided kwargs."""
        cls = self._PROCESSOR_MAP.get(fmt, PlainTextProcessor)
        kwargs = self._proc_kwargs.get(fmt, {})
        return cls(**kwargs)

    # -----------------------------------------------------------------------
    # Convenience properties
    # -----------------------------------------------------------------------

    @property
    def supported_formats(self) -> list[str]:
        return list(self._PROCESSOR_MAP.keys())

    def detect_format(self, path: str | Path) -> str:
        return self.detector.detect_from_path(path)

    def detect_format_bytes(self, data: bytes, filename: str = "") -> str:
        return self.detector.detect_from_bytes(data, filename=filename)
