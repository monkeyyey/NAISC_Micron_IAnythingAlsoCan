"""
binary_proc.py — Processor for binary / opaque machine logs.

Binary payloads are chunked into deterministic records so they can enter the
same staging pipeline as textual logs. Each record keeps both a hex dump and a
best-effort ASCII preview.
"""

from __future__ import annotations

from typing import Iterator

from ..base_processor import BaseLogProcessor, LogRecord, extract_level
from ..normalizer import clean_text, extract_timestamp, to_iso8601


class BinaryProcessor(BaseLogProcessor):
    """Chunk binary input into fixed-size records."""

    FORMAT = "binary"

    def __init__(self, chunk_size: int = 16):
        self.chunk_size = max(1, chunk_size)

    def process_bytes(self, data: bytes, *, source: str = "upload") -> Iterator[LogRecord]:
        if not data:
            return

        for index, chunk_start in enumerate(range(0, len(data), self.chunk_size), start=1):
            chunk = data[chunk_start:chunk_start + self.chunk_size]
            ascii_preview = "".join(chr(b) if 32 <= b <= 126 else "." for b in chunk)
            message = clean_text(ascii_preview)
            ts_raw, dt = extract_timestamp(message) if message else (None, None)

            yield LogRecord(
                raw=chunk.hex(),
                source=source,
                line_number=index,
                format=self.FORMAT,
                timestamp_raw=ts_raw,
                timestamp=to_iso8601(dt) if dt else None,
                message=message or f"binary_chunk_{index}",
                level=extract_level(message) if message else None,
                fields={
                    "offset": chunk_start,
                    "size_bytes": len(chunk),
                    "hex": chunk.hex(),
                    "ascii": ascii_preview,
                },
            )

    def process_text(self, text: str, *, source: str = "stream") -> Iterator[LogRecord]:
        yield from self.process_bytes(text.encode("latin-1", errors="replace"), source=source)

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        for lineno, line in enumerate(lines, start=1):
            ascii_preview = clean_text(line)
            yield LogRecord(
                raw=line.encode("latin-1", errors="replace").hex(),
                source=source,
                line_number=lineno,
                format=self.FORMAT,
                message=ascii_preview or f"binary_chunk_{lineno}",
                level=extract_level(ascii_preview) if ascii_preview else None,
                fields={
                    "offset": (lineno - 1) * self.chunk_size,
                    "size_bytes": len(line),
                    "hex": line.encode("latin-1", errors="replace").hex(),
                    "ascii": ascii_preview,
                },
            )
