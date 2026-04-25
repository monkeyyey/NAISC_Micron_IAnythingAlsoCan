"""
ingestion/stream.py — Streaming ingestion adapter.

Buffers incoming log lines and flushes when:
  - STREAM_BUFFER_SIZE records accumulated, OR
  - STREAM_FLUSH_INTERVAL_SEC seconds have elapsed since last flush

Shares all pipeline components with the batch adapter.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Optional

from config import STREAM_BUFFER_SIZE, STREAM_FLUSH_INTERVAL_SEC, BATCH_LLM_MAX_SIZE
from parsing.format_router import detect_format_from_content, route_to_parser
from signature.generator import generate_signature
from parsing.regex_engine import apply_mapping
from normalisation.unit_normaliser import normalise_units
from normalisation.cleaner import clean_record
from config import validate_record, move_extra_fields, CANONICAL_SCHEMA
from database.writer import bulk_insert, insert_failure, update_anomaly_scores

logger = logging.getLogger(__name__)


class StreamProcessor:
    """
    Processes log lines as they arrive via an HTTP endpoint or similar source.
    Batches LLM calls for cache misses on each flush.
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.buffer: list[tuple[str, str]] = []   # (line, source)
        self.last_flush: float = time.time()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(self, line: str, source: str) -> dict:
        """
        Accept a single log line.
        Returns immediately with {"record_id": "...", "status": "queued"}.
        Flushes the buffer if thresholds are met.
        """
        record_id = str(uuid.uuid4())
        self.buffer.append((line, source))

        if self._should_flush():
            self.flush()

        return {"record_id": record_id, "status": "queued"}

    def flush(self) -> int:
        """
        Process all buffered lines through the full pipeline.
        Returns the count of records successfully inserted.
        """
        if not self.buffer:
            self.last_flush = time.time()
            return 0

        batch = list(self.buffer)
        self.buffer.clear()
        self.last_flush = time.time()

        logger.debug("StreamProcessor.flush: processing %d buffered lines", len(batch))
        return self._process_buffer(batch)

    def _should_flush(self) -> bool:
        return (
            len(self.buffer) >= STREAM_BUFFER_SIZE
            or (time.time() - self.last_flush) >= STREAM_FLUSH_INTERVAL_SEC
        )

    # ------------------------------------------------------------------
    # Internal processing
    # ------------------------------------------------------------------

    def _process_buffer(self, batch: list[tuple[str, str]]) -> int:
        """Run the full pipeline on a buffer of (line, source) tuples."""
        from ingestion.batch import _merge, _infer_log_type

        raw_records = []
        for line, source in batch:
            fmt = detect_format_from_content(line)
            parsed = route_to_parser(fmt, line)
            for p in parsed:
                p["raw_line"] = p.get("raw_text") or line
                p["log_source"] = source
                p["record_id"] = str(uuid.uuid4())
                p["_signature"] = generate_signature(p["raw_line"])
                p["_format"] = fmt
                raw_records.append(p)

        if not raw_records:
            return 0

        hit_records = []
        miss_lines, miss_sigs, miss_raw = [], [], []

        for raw in raw_records:
            sig = raw["_signature"]
            patterns = self.pipeline.registry.lookup(sig)
            if patterns:
                self.pipeline.registry.increment_hit(sig)
                extracted, flags = apply_mapping(raw["raw_line"], patterns)
                conf = self.pipeline.registry.get_full_record(sig)["confidence"]
                merged = _merge(raw, extracted, flags, self.pipeline.hash_table)
                merged["mapping_confidence"] = conf
                hit_records.append(merged)
            else:
                miss_lines.append(raw["raw_line"])
                miss_sigs.append(sig)
                miss_raw.append(raw)

        if miss_lines:
            llm_mappings = self.pipeline.llm.batch_generate(
                miss_lines, miss_sigs, self.pipeline.pool
            )
            for raw, mapping in zip(miss_raw, llm_mappings):
                patterns   = mapping.get("fields", {})
                confidence = mapping.get("confidence", 0.0)
                llm_flags  = mapping.get("parse_flags", [])
                if patterns:
                    self.pipeline.registry.store(
                        raw["_signature"], raw["_format"], patterns, confidence
                    )
                extracted, regex_flags = apply_mapping(raw["raw_line"], patterns) if patterns else ({}, [])
                merged = _merge(raw, extracted, regex_flags + llm_flags, self.pipeline.hash_table)
                merged["mapping_confidence"] = confidence
                hit_records.append(merged)

        valid_records = []
        for rec in hit_records:
            normalise_units(rec)
            clean_record(rec)
            move_extra_fields(rec)
            is_valid, errors = validate_record(rec)
            if not is_valid:
                rec["parse_flags"] = (rec.get("parse_flags") or []) + [
                    f"schema_error:{e}" for e in errors
                ]
            valid_records.append(rec)

        inserted, _ = bulk_insert(valid_records)

        if self.pipeline.anomaly.is_fitted:
            scores = self.pipeline.anomaly.score_batch(valid_records)
            updates = [(s, r["record_id"]) for s, r in zip(scores, valid_records)]
            update_anomaly_scores(updates)

        return inserted
