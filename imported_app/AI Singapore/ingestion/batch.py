"""
ingestion/batch.py — Batch file ingestion adapter.

Processing order per the spec:
  1. Load file, detect format, parse into raw record dicts
  2. Generate signatures, check registry for cached mappings
  3. Split into cache_hits and cache_misses
  4. Cache hits: apply regex mapping immediately
  5. Cache misses: batch → LLM → apply new mappings → store in registry
  6. Field name translation via hash table
  7. Unit normalisation → cleaning → schema validation
  8. Bulk insert to database
  9. Anomaly detection → update scores
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Optional

from anomaly.detector import AnomalyDetector
from caching.hash_table import FieldNameHashTable
from caching.registry import MappingRegistry
from caching.trie import LogTemplateTrie
from clustering.candidate_pool import CandidatePool
from config import (
    CANONICAL_SCHEMA, BATCH_LLM_MAX_SIZE,
    validate_record, move_extra_fields,
)
from database.models import init_db
from database.writer import bulk_insert, insert_failure, update_anomaly_scores
from llm.client import LLMClient
from normalisation.cleaner import clean_record
from normalisation.unit_normaliser import normalise_units
from parsing.regex_engine import apply_mapping
from signature.generator import generate_signature

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    total:      int = 0
    success:    int = 0
    failed:     int = 0
    cache_hits: int = 0
    llm_calls:  int = 0


class Pipeline:
    """
    Shared pipeline components. Instantiate once and pass to batch/stream adapters.
    """

    def __init__(self):
        init_db()
        self.registry  = MappingRegistry()
        self.hash_table = FieldNameHashTable()
        self.trie      = LogTemplateTrie()
        self.pool      = CandidatePool()
        self.llm       = LLMClient()
        self.anomaly   = AnomalyDetector()

        # Pre-populate trie from registry on startup
        loaded = self.trie.load_from_registry(self.registry)
        logger.info("Pipeline: trie pre-loaded with %d templates", loaded)


def process_batch(file_path: str, pipeline: Pipeline, source: str | None = None) -> BatchResult:
    """
    Load a file and run through the full pipeline.

    The format parsing layer handles format detection, parsing, encoding
    normalisation, and corruption detection.  The semantic pipeline
    (_run_pipeline) handles field mapping, unit conversion, LLM mapping,
    anomaly detection, and DB storage.

    Returns a BatchResult with counts.
    """
    from format_parsing.ingestion import LogIngestionService as _P1Service
    from format_parsing.staging import StagingArea as _P1Staging

    result = BatchResult()
    log_source = source or file_path

    # Format parsing layer — format detection, parsing, encoding normalisation, corruption detection
    p1_staging = _P1Staging(backend="memory", deduplicate=True)
    p1_service = _P1Service(staging=p1_staging)
    try:
        p1_service.ingest_file(file_path, source_label=log_source)
    except OSError as exc:
        insert_failure("", log_source, f"file_read_error:{exc}")
        result.failed += 1
        return result

    # Corrupted records → parse_failures (no LLM spend)
    for rec in p1_staging.all_records(include_corrupted=True):
        if rec.corrupted:
            insert_failure(
                raw_line=rec.message or "",
                log_source=log_source,
                error=rec.corruption_reason or "corrupted",
            )
            result.failed += 1

    # Build raw dicts from clean LogRecords for the semantic pipeline
    raw_records = []
    for rec in p1_staging.all_records(include_corrupted=False):
        raw: dict = dict(rec.fields or {})
        raw["raw_text"]   = rec.message or ""
        raw["log_source"] = log_source
        raw["_format"]    = rec.format
        if rec.timestamp:
            raw["timestamp"] = rec.timestamp
        if rec.level:
            raw["level"] = rec.level
        raw_records.append(raw)

    result.total = len(raw_records)
    logger.info(
        "Batch: %d clean, %d corrupted from %s",
        result.total, result.failed, file_path,
    )

    if not raw_records:
        return result

    return _run_pipeline(raw_records, "unknown", pipeline, result, log_source)


# ---------------------------------------------------------------------------
# Public: staging area connector
# ---------------------------------------------------------------------------

def process_from_phase1_staging(
    ndjson_path: str,
    pipeline: Pipeline,
    source: str | None = None,
) -> BatchResult:
    """
    Read the staging area cleaned_logs.ndjson and run through the semantic
    pipeline (field mapping, unit normalisation, anomaly detection, DB insert).

    The format parsing layer already handled format detection, parsing, encoding
    normalisation, deduplication, and corruption filtering — so we skip those
    steps here. Corrupted records are routed directly to parse_failures.
    """
    result = BatchResult()
    log_source = source or ndjson_path

    try:
        with open(ndjson_path, "r", encoding="utf-8") as fh:
            raw_lines = [ln.strip() for ln in fh if ln.strip()]
    except OSError as exc:
        insert_failure("", log_source, f"file_read_error:{exc}")
        result.failed += 1
        return result

    raw_records: list[dict] = []

    for ln in raw_lines:
        try:
            record = json.loads(ln)
        except json.JSONDecodeError:
            insert_failure(ln, log_source, "staging_ndjson_parse_error")
            result.failed += 1
            continue

        rec_source = source or record.get("source", log_source)

        if record.get("corrupted"):
            insert_failure(
                raw_line=record.get("message", ""),
                log_source=rec_source,
                error=record.get("corruption_reason") or "corrupted",
            )
            result.failed += 1
            continue

        # Build raw_record from LogRecord fields.
        # Spread `fields` dict so vendor names reach the hash_table lookup.
        raw: dict = dict(record.get("fields") or {})
        raw["raw_text"]   = record.get("message", "")
        raw["log_source"] = rec_source
        raw["_format"]    = record.get("format", "unknown")
        if record.get("timestamp"):
            raw["timestamp"] = record["timestamp"]
        if record.get("level"):
            raw["level"] = record["level"]
        raw_records.append(raw)

    result.total = len(raw_records)
    logger.info(
        "from-staging: %d clean records, %d corrupted skipped from %s",
        result.total, result.failed, ndjson_path,
    )

    if not raw_records:
        return result

    return _run_pipeline(raw_records, "unknown", pipeline, result, log_source)


# ---------------------------------------------------------------------------
# Private: shared pipeline body
# ---------------------------------------------------------------------------

def _run_pipeline(
    raw_records: list[dict],
    fmt: str,
    pipeline: Pipeline,
    result: BatchResult,
    log_source: str,
) -> BatchResult:
    """
    Steps 2–9 of the processing spec: signature → cache → LLM → normalise →
    DB insert → anomaly detection.  Operates on an already-parsed list of
    raw record dicts and mutates *result* in-place before returning it.
    """
    hit_records: list[dict] = []
    miss_lines:  list[str]  = []
    miss_sigs:   list[str]  = []
    miss_raw:    list[dict] = []

    for raw in raw_records:
        print(f"Processing raw record from {log_source}: {raw.get('raw_text', '')[:50]}...")
        raw_line = raw.get("raw_text") or raw.get("_raw_kv_line") or _dict_to_line(raw)
        raw["raw_line"] = raw_line
        raw.setdefault("log_source", log_source)
        raw.setdefault("record_id", str(uuid.uuid4()))

        sig = generate_signature(raw_line)
        raw["_signature"] = sig
        raw.setdefault("_format", fmt)

        patterns = pipeline.registry.lookup(sig)
        if patterns:
            pipeline.registry.increment_hit(sig)
            extracted, flags = apply_mapping(raw_line, patterns)
            print(f"Cache hit for signature {sig}: extracted fields {extracted} with flags {flags}")
            merged = _merge(raw, extracted, flags, pipeline.hash_table)
            merged["mapping_confidence"] = pipeline.registry.get_full_record(sig)["confidence"]
            print(f"Merged record for cache hit: {merged}")
            hit_records.append(merged)
            result.cache_hits += 1
        else:
            miss_lines.append(raw_line)
            miss_sigs.append(sig)
            miss_raw.append(raw)

    if miss_lines:
        llm_mappings = pipeline.llm.batch_generate(miss_lines, miss_sigs, pipeline.pool)
        result.llm_calls += (len(miss_lines) + BATCH_LLM_MAX_SIZE - 1) // BATCH_LLM_MAX_SIZE

        for raw, mapping in zip(miss_raw, llm_mappings):
            patterns   = mapping.get("fields", {})
            confidence = mapping.get("confidence", 0.0)
            llm_flags  = mapping.get("parse_flags", [])
            sig        = raw["_signature"]
            fmt_type   = raw["_format"]

            if patterns:
                pipeline.registry.store(sig, fmt_type, patterns, confidence)

            raw_line = raw["raw_line"]
            extracted, regex_flags = apply_mapping(raw_line, patterns) if patterns else ({}, [])
            merged = _merge(raw, extracted, regex_flags + llm_flags, pipeline.hash_table)
            merged["mapping_confidence"] = confidence
            hit_records.append(merged)

    valid_records: list[dict] = []

    for rec in hit_records:
        normalise_units(rec)
        clean_record(rec)
        move_extra_fields(rec)

        is_valid, errors = validate_record(rec)
        if not is_valid:
            rec["parse_flags"] = (rec.get("parse_flags") or []) + [f"schema_error:{e}" for e in errors]
        valid_records.append(rec)

    inserted, skipped = bulk_insert(valid_records)
    result.success += inserted
    result.failed  += len(raw_records) - inserted - skipped

    if not pipeline.anomaly.is_fitted and len(valid_records) >= 10:
        pipeline.anomaly.fit(valid_records)

    if pipeline.anomaly.is_fitted:
        scores = pipeline.anomaly.score_batch(valid_records)
        updates = [
            (score, rec["record_id"])
            for score, rec in zip(scores, valid_records)
        ]
        update_anomaly_scores(updates)

    logger.info(
        "Pipeline complete: total=%d success=%d failed=%d cache_hits=%d llm_calls=%d",
        result.total, result.success, result.failed, result.cache_hits, result.llm_calls,
    )
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dict_to_line(d: dict) -> str:
    """Serialise a raw record dict back to a single-line string for raw_line."""
    return " ".join(f"{k}={v}" for k, v in d.items() if not k.startswith("_"))


def _merge(
    raw: dict,
    extracted: dict,
    flags: list[str],
    hash_table: FieldNameHashTable,
) -> dict:
    """
    Merge a raw record dict with LLM-extracted fields.
    Translate vendor field names → canonical names via hash table.
    Unknown vendor fields go into extra_fields.
    """
    record: dict = {
        "record_id":   raw.get("record_id"),
        "log_source":  raw.get("log_source"),
        "raw_line":    raw.get("raw_line"),
        "log_type":    "unknown",
        "parse_flags": list(flags),
        "extra_fields": {},
    }

    # Translate fields from the raw record (structured parsers)
    for vendor_key, value in raw.items():
        if vendor_key.startswith("_"):
            continue
        if vendor_key in CANONICAL_SCHEMA:
            record[vendor_key] = value
        else:
            canonical = hash_table.lookup(vendor_key)
            if canonical:
                record[canonical] = value
            else:
                record["extra_fields"][vendor_key] = value

    # Overlay with LLM-extracted values (these take priority for canonical fields)
    for canonical_field, value in extracted.items():
        if value is not None:
            record[canonical_field] = value

    # Infer log_type from content
    record["log_type"] = _infer_log_type(record)

    return record


def _infer_log_type(record: dict) -> str:
    if record.get("alarm_code") or record.get("alarm_severity"):
        return "alarm"
    if any(record.get(f) is not None for f in ("temperature", "pressure", "rf_power", "flow_rate")):
        return "sensor"
    if record.get("recipe_name") or record.get("process_step"):
        return "process"
    if record.get("event_description") or record.get("status"):
        return "event"
    return "unknown"
