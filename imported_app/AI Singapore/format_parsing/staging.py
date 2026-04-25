"""
staging.py — Staging area for cleaned log records.

Provides in-memory and disk-backed staging with:
    - Deduplication via record_id
    - Partitioning by format, level, or date
    - Export to JSON-Lines, CSV, or Python dicts
    - Summary statistics
"""

from __future__ import annotations

import csv
import io
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Literal

from .base_processor import LogRecord


class StagingArea:
    """
    Temporary storage for processed LogRecord objects before downstream use.

    Supports two backends:
        - ``"memory"``: All records kept in a list (default, fast).
        - ``"disk"``:   Records spooled to a temp NDJSON file (large datasets).
    """

    def __init__(
        self,
        backend: Literal["memory", "disk"] = "memory",
        deduplicate: bool = True,
        max_memory_records: int = 100_000,
    ):
        self.backend = backend
        self.deduplicate = deduplicate
        self.max_memory_records = max_memory_records

        self._records: list[LogRecord] = []
        self._seen_ids: set[str] = set()
        self._disk_path: Path | None = None
        self._disk_file = None

        # Statistics counters
        self._stats: dict[str, Any] = defaultdict(int)
        self._level_counts: dict[str, int] = defaultdict(int)
        self._format_counts: dict[str, int] = defaultdict(int)

        # Ensure staging_area folder exists
        self._staging_dir = Path(__file__).parent / "staging_area"
        self._staging_dir.mkdir(parents=True, exist_ok=True)

        if backend == "disk":
            self._init_disk()

    # -----------------------------------------------------------------------
    # Ingestion
    # -----------------------------------------------------------------------

    def add(self, record: LogRecord) -> bool:
        """Add a single LogRecord. Returns True if accepted, False if duplicate."""
        if self.deduplicate and record.record_id in self._seen_ids:
            self._stats["duplicates_skipped"] += 1
            return False

        if self.deduplicate:
            self._seen_ids.add(record.record_id)

        # Update statistics
        self._stats["total"] += 1
        if record.corrupted:
            self._stats["corrupted"] += 1
            self._stats[f"corruption:{record.corruption_reason}"] += 1
        else:
            self._stats["clean"] += 1
        if record.level:
            self._level_counts[record.level] += 1
        self._format_counts[record.format] += 1

        # Store
        if self.backend == "disk":
            self._write_disk(record)
        else:
            self._records.append(record)
            # Auto-spill to disk if over limit
            if len(self._records) > self.max_memory_records:
                self._spill_to_disk()

        return True

    def add_many(self, records: Iterable[LogRecord]) -> int:
        """Bulk add records. Returns count of accepted records."""
        return sum(1 for r in records if self.add(r))

    # -----------------------------------------------------------------------
    # Retrieval
    # -----------------------------------------------------------------------

    def all_records(self, *, include_corrupted: bool = False) -> Iterator[LogRecord]:
        """Iterate over all stored records."""
        if self.backend == "disk" or self._disk_path:
            yield from self._read_disk(include_corrupted=include_corrupted)
        else:
            for r in self._records:
                if include_corrupted or not r.corrupted:
                    yield r

    def clean_records(self) -> Iterator[LogRecord]:
        """Iterate over only valid (non-corrupted) records."""
        yield from self.all_records(include_corrupted=False)

    def corrupted_records(self) -> Iterator[LogRecord]:
        """Iterate over only corrupted / malformed records."""
        if self.backend == "disk" or self._disk_path:
            for rec in self._read_disk(include_corrupted=True):
                if rec.corrupted:
                    yield rec
        else:
            for r in self._records:
                if r.corrupted:
                    yield r

    def filter(self, predicate: Callable[[LogRecord], bool]) -> Iterator[LogRecord]:
        """Filter records using a custom predicate."""
        for r in self.all_records(include_corrupted=True):
            if predicate(r):
                yield r

    def filter_by_level(self, level: str) -> Iterator[LogRecord]:
        """Return clean records matching a specific log level."""
        level = level.upper()
        return self.filter(lambda r: not r.corrupted and r.level == level)

    def filter_by_format(self, fmt: str) -> Iterator[LogRecord]:
        """Return records from a specific log format."""
        return self.filter(lambda r: r.format == fmt)

    # -----------------------------------------------------------------------
    # Export
    # -----------------------------------------------------------------------

    def to_dicts(self, *, include_corrupted: bool = False) -> list[dict[str, Any]]:
        """Export all records as a list of dictionaries."""
        return [r.to_dict() for r in self.all_records(include_corrupted=include_corrupted)]

    def to_ndjson(self, path: str | None = None, *, include_corrupted: bool = False) -> str:
        """
        Serialise records to JSON-Lines format.

        If *path* is given, writes to disk and returns the path.
        Otherwise, returns the NDJSON string.
        """
        lines = [
            json.dumps(r.to_dict(), default=str)
            for r in self.all_records(include_corrupted=include_corrupted)
        ]
        content = "\n".join(lines)
        if path:
            p = Path(path)
            full_path = p if p.is_absolute() else Path(self._staging_dir) / p
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(content, encoding="utf-8")
            return str(full_path)
        return content

    def to_csv(self, path: str | None = None, *, include_corrupted: bool = False) -> str:
        """
        Serialise clean records to CSV.

        Returns path (if given) or the CSV string.
        """
        records = list(self.all_records(include_corrupted=include_corrupted))
        if not records:
            return ""

        fieldnames = [
            "record_id", "source", "line_number", "format",
            "timestamp", "level", "message", "corrupted", "corruption_reason",
        ]
        # Include all field keys seen across records
        all_field_keys: set[str] = set()
        for r in records:
            all_field_keys.update(r.fields.keys())
        fieldnames += [f"field.{k}" for k in sorted(all_field_keys)]

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            row = rec.to_dict()
            # Flatten fields into field.key columns
            for k, v in rec.fields.items():
                row[f"field.{k}"] = v
            del row["fields"]
            writer.writerow(row)

        content = buf.getvalue()
        if path:
            p = Path(path)
            full_path = p if p.is_absolute() else Path(self._staging_dir) / p
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(content, encoding="utf-8")
            return str(full_path)
        return content

    # -----------------------------------------------------------------------
    # Statistics
    # -----------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        """Return a summary of ingested records."""
        return {
            "total_records":     self._stats.get("total", 0),
            "clean_records":     self._stats.get("clean", 0),
            "corrupted_records": self._stats.get("corrupted", 0),
            "duplicates_skipped": self._stats.get("duplicates_skipped", 0),
            "level_distribution": dict(self._level_counts),
            "format_distribution": dict(self._format_counts),
            "corruption_reasons": {
                k.split("corruption:", 1)[1]: v
                for k, v in self._stats.items()
                if k.startswith("corruption:")
            },
            "backend": self.backend,
        }

    def print_stats(self) -> None:
        """Pretty-print staging area statistics."""
        s = self.stats()
        print("\n" + "=" * 60)
        print("  STAGING AREA STATISTICS")
        print("=" * 60)
        print(f"  Total records     : {s['total_records']:,}")
        print(f"  Clean records     : {s['clean_records']:,}")
        print(f"  Corrupted records : {s['corrupted_records']:,}")
        print(f"  Duplicates skipped: {s['duplicates_skipped']:,}")
        print(f"  Backend           : {s['backend']}")
        if s["level_distribution"]:
            print("\n  Log level distribution:")
            for lvl, cnt in sorted(s["level_distribution"].items()):
                print(f"    {lvl:<12} {cnt:>8,}")
        if s["format_distribution"]:
            print("\n  Format distribution:")
            for fmt, cnt in sorted(s["format_distribution"].items()):
                print(f"    {fmt:<20} {cnt:>8,}")
        if s["corruption_reasons"]:
            print("\n  Corruption reasons:")
            for reason, cnt in sorted(s["corruption_reasons"].items(), key=lambda x: -x[1]):
                print(f"    {reason:<40} {cnt:>6,}")
        print("=" * 60 + "\n")

    # -----------------------------------------------------------------------
    # Disk backend helpers
    # -----------------------------------------------------------------------

    def _init_disk(self) -> None:
        file_path = self._staging_dir / "log_staging_disk.ndjson"
        self._disk_path = file_path
        self._disk_file = open(file_path, "w", encoding="utf-8")

    def _write_disk(self, record: LogRecord) -> None:
        if self._disk_file:
            self._disk_file.write(json.dumps(record.to_dict(), default=str) + "\n")
            self._disk_file.flush()

    def _read_disk(self, *, include_corrupted: bool) -> Iterator[LogRecord]:
        if self._disk_file:
            self._disk_file.flush()
        path = self._disk_path
        if not path or not path.exists():
            return
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    corrupted = d.get("corrupted", False)
                    if not include_corrupted and corrupted:
                        continue
                    rec = LogRecord(
                        raw=d.get("raw", ""),
                        source=d.get("source", ""),
                        line_number=d.get("line_number", 0),
                        format=d.get("format", "unknown"),
                        timestamp_raw=d.get("timestamp_raw"),
                        timestamp=d.get("timestamp"),
                        message=d.get("message", ""),
                        level=d.get("level"),
                        fields=d.get("fields", {}),
                        corrupted=corrupted,
                        corruption_reason=d.get("corruption_reason", ""),
                        record_id=d.get("record_id", ""),
                    )
                    yield rec
                except (json.JSONDecodeError, TypeError):
                    continue

    def _spill_to_disk(self) -> None:
        """Move in-memory records to disk (triggered when memory limit is hit)."""
        if not self._disk_path:
            self._init_disk()
        for rec in self._records:
            self._write_disk(rec)
        self._records.clear()
        self.backend = "disk"

    def __del__(self) -> None:
        """Clean up temporary disk file."""
        if self._disk_file:
            try:
                self._disk_file.close()
            except Exception:
                pass
        if self._disk_path and self._disk_path.exists():
            try:
                self._disk_path.unlink()
            except Exception:
                pass

    def __len__(self) -> int:
        return self._stats.get("total", 0)

    def __repr__(self) -> str:
        s = self.stats()
        return (
            f"StagingArea(total={s['total_records']}, "
            f"clean={s['clean_records']}, "
            f"corrupted={s['corrupted_records']}, "
            f"backend='{self.backend}')"
        )