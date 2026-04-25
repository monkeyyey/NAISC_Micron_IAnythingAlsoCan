"""
demo.py — End-to-end demonstration of the Log Data Preparation Pipeline.

Loads sample log files from the local ``logs`` directory, runs them through the
pipeline, and prints the cleaned/structured output along with staging statistics.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Make sure the local package is importable when running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from log_pipeline import LogIngestionService
from log_pipeline.staging import StagingArea

# ============================================================================
# Helpers
# ============================================================================

SEPARATOR = "─" * 70
LOG_DIR = Path(__file__).parent / "logs"

SAMPLE_FILES = [
    ("Plain Text Logs", "plain_text", "plain_text.log", False),
    ("Key-Value Logs", "key_value", "key_value.log", False),
    ("Delimiter-Separated Logs", "delimiter", "delimiter.log", False),
    ("CSV Logs", "csv", "logs.csv", False),
    ("TSV Logs", "tsv", "logs.tsv", False),
    ("JSON Logs", "json", "logs.jsonl", False),
    ("XML Logs", "xml", "logs.xml", False),
    ("YAML Logs", "yaml", "logs.yaml", False),
    ("Binary Logs", "binary", "logs.bin", True),
    ("Syslog Format Logs", "syslog", "logs.syslog", False),
    ("Logfmt Logs", "logfmt", "logs.logfmt", False),
]


def load_sample(path: Path, is_binary: bool) -> bytes | str:
    return path.read_bytes() if is_binary else path.read_text(encoding="utf-8")

def section(title: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)

def print_record(rec, index: int) -> None:
    d = rec.to_dict()
    marker = "✗ CORRUPT" if rec.corrupted else "✓"
    print(f"  [{index:02d}] {marker}")
    if rec.corrupted:
        print(f"       reason  : {rec.corruption_reason}")
        print(f"       raw     : {rec.raw[:80]!r}")
        return
    if rec.timestamp:
        print(f"       time    : {rec.timestamp}")
    if rec.level:
        print(f"       level   : {rec.level}")
    if rec.message:
        print(f"       message : {rec.message[:80]}")
    if rec.fields:
        fields_str = ", ".join(f"{k}={v!r}" for k, v in list(rec.fields.items())[:5])
        print(f"       fields  : {fields_str}")

# ============================================================================
# Main demo
# ============================================================================

def run_demo() -> None:
    print("\n" + "═" * 70)
    print("  LOG DATA PREPARATION PIPELINE — FULL DEMO")
    print("═" * 70)

    # Shared staging area — collects ALL records from all formats
    staging = StagingArea(backend="memory", deduplicate=True)
    service = LogIngestionService(staging=staging)

    samples = [
        (title, fmt, LOG_DIR / filename, is_binary)
        for title, fmt, filename, is_binary in SAMPLE_FILES
    ]

    for title, fmt, sample_path, is_binary in samples:
        section(title)
        sample_data = load_sample(sample_path, is_binary)
        if is_binary:
            result = service.ingest_bytes(
                sample_data,  # type: ignore[arg-type]
                filename=sample_path.name,
                format_override=fmt,
                source_label=title,
            )
        else:
            result = service.ingest_text(
                sample_data,  # type: ignore[arg-type]
                format_override=fmt,
                source_label=title,
            )
        print(f"  Detected format : {result.detected_format}")
        print(f"  Total records   : {result.total_records}")
        print(f"  Clean           : {result.clean_records}")
        print(f"  Corrupted       : {result.corrupted_records}")
        print(f"  Duration        : {result.duration_seconds*1000:.1f} ms")
        print(f"  Sample file     : {sample_path.name}")
        print()

        # Print each record
        for i, rec in enumerate(
            staging.filter(lambda r, t=title: r.source == t), start=1
        ):
            print_record(rec, i)

    # -------------------------------------------------------------------
    # Global staging statistics
    # -------------------------------------------------------------------
    section("GLOBAL STAGING STATISTICS")
    staging.print_stats()

    # -------------------------------------------------------------------
    # Auto-detection demo
    # -------------------------------------------------------------------
    section("FORMAT AUTO-DETECTION DEMO")
    test_snippets = [
        ("JSON-Lines",   (LOG_DIR / "logs.jsonl").read_text(encoding="utf-8").strip(), "logs.jsonl"),
        ("CSV",          (LOG_DIR / "logs.csv").read_text(encoding="utf-8").strip(), "logs.csv"),
        ("Key-Value",    (LOG_DIR / "key_value.log").read_text(encoding="utf-8").strip(), "key_value.log"),
        ("Plain Text",   (LOG_DIR / "plain_text.log").read_text(encoding="utf-8").strip(), "plain_text.log"),
        ("XML",          (LOG_DIR / "logs.xml").read_text(encoding="utf-8").strip(), "logs.xml"),
        ("YAML",         (LOG_DIR / "logs.yaml").read_text(encoding="utf-8").strip(), "logs.yaml"),
        ("Syslog",       (LOG_DIR / "logs.syslog").read_text(encoding="utf-8").strip(), "logs.syslog"),
        ("Logfmt",       (LOG_DIR / "logs.logfmt").read_text(encoding="utf-8").strip(), "logs.logfmt"),
    ]
    for label, snippet, filename in test_snippets:
        detected = service.detect_format_bytes(snippet.encode(), filename=filename or "")
        print(f"  {label:<22} → detected as: {detected}")

    detected_binary = service.detect_format_bytes(
        (LOG_DIR / "logs.bin").read_bytes(),
        filename="logs.bin",
    )
    print(f"  {'Binary':<22} → detected as: {detected_binary}")

# -------------------------------------------------------------------
# Export demo
# -------------------------------------------------------------------
    section("EXPORT EXAMPLES")

    # Use staging_area inside the log_pipeline package
    staging_dir = Path(__file__).parent / "staging_area"
    staging_dir.mkdir(parents=True, exist_ok=True)

    ndjson_out = staging_dir / "cleaned_logs.ndjson"
    csv_out    = staging_dir / "cleaned_logs.csv"

    staging.to_ndjson(ndjson_out, include_corrupted=False)
    staging.to_csv(csv_out, include_corrupted=False)

    ndjson_lines = ndjson_out.read_text().strip().splitlines()
    csv_lines    = csv_out.read_text().strip().splitlines()
    print(f"  NDJSON export  : {ndjson_out} ({len(ndjson_lines):,} records)")
    print(f"  CSV export     : {csv_out} ({len(csv_lines)-1:,} records + header)")

    # Sample JSON record
    print("\n  Sample exported record (NDJSON):")
    if ndjson_lines:
        sample = json.loads(ndjson_lines[0])
        for k, v in sample.items():
            if v not in (None, "", {}, []):
                print(f"    {k:<20}: {str(v)[:60]}")

    print(f"\n{'═'*70}")
    print("  Pipeline complete.")
    print(f"{'═'*70}\n")


if __name__ == "__main__":
    run_demo()
