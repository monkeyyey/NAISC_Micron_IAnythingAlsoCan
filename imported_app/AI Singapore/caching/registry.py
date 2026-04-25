"""
caching/registry.py — SQLite-backed mapping registry.

Maps log signatures → LLM-generated regex patterns.
SQLite is the right tool here: the registry is a persistent key-value cache
with simple lookups by primary key.  No server required, survives restarts.

Schema:
    signature     TEXT PRIMARY KEY  — 16-char sha256 prefix
    format_type   TEXT              — detected format (json, plaintext, …)
    regex_patterns TEXT             — JSON: {canonical_field: regex_pattern}
    confidence    REAL              — LLM confidence score (0–1)
    hit_count     INTEGER           — how many times this mapping was used
    created_at    TEXT              — ISO 8601
    updated_at    TEXT              — ISO 8601
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone

from config import REGISTRY_DB_PATH

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS mapping_registry (
    signature      TEXT PRIMARY KEY,
    format_type    TEXT NOT NULL,
    regex_patterns TEXT NOT NULL,
    confidence     REAL NOT NULL,
    hit_count      INTEGER DEFAULT 0,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
"""

_CREATE_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_registry_format "
    "ON mapping_registry(format_type);"
)


class MappingRegistry:
    """Thread-safe (single-connection) SQLite mapping registry."""

    def __init__(self, db_path: str = REGISTRY_DB_PATH):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(_CREATE_TABLE)
        self._conn.execute(_CREATE_INDEX)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def lookup(self, signature: str) -> dict | None:
        """
        Return the stored regex pattern dict for this signature, or None.
        Regex patterns are returned as {canonical_field: pattern_string}.
        """
        row = self._conn.execute(
            "SELECT regex_patterns FROM mapping_registry WHERE signature = ?",
            (signature,),
        ).fetchone()

        if row is None:
            return None

        try:
            return json.loads(row[0])
        except json.JSONDecodeError as exc:
            logger.warning("Registry: corrupt patterns for %s — %s", signature, exc)
            return None

    def store(
        self,
        signature: str,
        format_type: str,
        patterns: dict,
        confidence: float,
    ) -> None:
        """
        Insert or update a mapping.  All regex patterns must have already been
        validated by the LLM response parser before calling this.
        """
        now = datetime.now(timezone.utc).isoformat()
        patterns_json = json.dumps(patterns)

        self._conn.execute(
            """
            INSERT INTO mapping_registry
                (signature, format_type, regex_patterns, confidence,
                 hit_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(signature) DO UPDATE SET
                format_type    = excluded.format_type,
                regex_patterns = excluded.regex_patterns,
                confidence     = excluded.confidence,
                updated_at     = excluded.updated_at
            """,
            (signature, format_type, patterns_json, confidence, now, now),
        )
        self._conn.commit()

    def increment_hit(self, signature: str) -> None:
        """Increment hit_count for monitoring cache effectiveness."""
        self._conn.execute(
            "UPDATE mapping_registry SET hit_count = hit_count + 1 WHERE signature = ?",
            (signature,),
        )
        self._conn.commit()

    def get_all_signatures(self) -> list[str]:
        """Return all stored signatures — used to pre-populate the trie on startup."""
        rows = self._conn.execute(
            "SELECT signature FROM mapping_registry"
        ).fetchall()
        return [r[0] for r in rows]

    def get_full_record(self, signature: str) -> dict | None:
        """Return the full registry row as a dict, or None."""
        row = self._conn.execute(
            "SELECT * FROM mapping_registry WHERE signature = ?",
            (signature,),
        ).fetchone()
        if row is None:
            return None
        keys = [
            "signature", "format_type", "regex_patterns",
            "confidence", "hit_count", "created_at", "updated_at",
        ]
        record = dict(zip(keys, row))
        try:
            record["regex_patterns"] = json.loads(record["regex_patterns"])
        except json.JSONDecodeError:
            record["regex_patterns"] = {}
        return record

    def stats(self) -> dict:
        """Return basic stats about the registry."""
        row = self._conn.execute(
            "SELECT COUNT(*), SUM(hit_count), AVG(confidence) FROM mapping_registry"
        ).fetchone()
        return {
            "total_signatures": row[0] or 0,
            "total_hits":       row[1] or 0,
            "avg_confidence":   round(row[2] or 0.0, 4),
        }

    def close(self) -> None:
        self._conn.close()
