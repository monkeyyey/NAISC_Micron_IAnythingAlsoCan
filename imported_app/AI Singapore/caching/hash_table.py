"""
caching/hash_table.py — Field name → canonical name mapping cache.

An in-memory dict backed by a JSON file.  Pre-populated with common
semiconductor vendor field name synonyms so the LLM is never called for
trivial mappings like temp → temperature.

Lookups are case-insensitive and strip leading/trailing whitespace.
"""

import json
import logging
import os

from config import CANONICAL_SCHEMA, FIELD_MAPPINGS_PATH

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pre-populated synonyms — semiconductor field name vocabulary
# ---------------------------------------------------------------------------

INITIAL_MAPPINGS: dict[str, str] = {
    # temperature
    "temp": "temperature", "temp_c": "temperature", "temperature_c": "temperature",
    "tmp": "temperature", "temp_celsius": "temperature", "t_celsius": "temperature",
    "celsius": "temperature", "deg_c": "temperature",

    # pressure
    "pres": "pressure", "pres_pa": "pressure", "pressure_pa": "pressure",
    "press": "pressure", "pres_torr": "pressure", "chamber_pressure": "pressure",
    "p": "pressure",

    # rf_power
    "rf": "rf_power", "rfpower": "rf_power", "rf_pwr": "rf_power",
    "power": "rf_power", "rf_watts": "rf_power",

    # flow_rate
    "flow": "flow_rate", "gas_flow": "flow_rate", "mfc_flow": "flow_rate",
    "flowrate": "flow_rate", "flow_sccm": "flow_rate",

    # voltage
    "volt": "voltage", "volts": "voltage", "v": "voltage",
    "bias_voltage": "voltage", "dc_bias": "voltage",

    # current
    "amp": "current", "amps": "current", "ampere": "current",
    "i": "current",

    # alarm_code
    "alarm": "alarm_code", "fault": "alarm_code", "error_code": "alarm_code",
    "err": "alarm_code", "fault_code": "alarm_code",

    # alarm_severity
    "severity": "alarm_severity", "level": "alarm_severity",
    "alarm_level": "alarm_severity",

    # timestamp
    "ts": "timestamp", "datetime": "timestamp", "time": "timestamp",
    "date_time": "timestamp", "log_time": "timestamp", "event_time": "timestamp",
    "created_at": "timestamp",

    # tool_id
    "tool": "tool_id", "toolid": "tool_id", "machine": "tool_id",
    "machine_id": "tool_id", "equipment": "tool_id", "equip_id": "tool_id",
    "tool_name": "tool_id",

    # wafer_id
    "wafer": "wafer_id", "wfr": "wafer_id", "wafer_num": "wafer_id",
    "wfr_id": "wafer_id",

    # lot_id
    "lot": "lot_id", "lot_number": "lot_id", "batch": "lot_id",
    "batch_id": "lot_id",

    # chamber_id
    "chamber": "chamber_id", "chm": "chamber_id", "chamber_num": "chamber_id",
    "ch": "chamber_id",

    # process_step
    "step": "process_step", "proc_step": "process_step", "step_name": "process_step",
    "process_step": "process_step",

    # recipe_name
    "recipe": "recipe_name", "rcp": "recipe_name", "process_recipe": "recipe_name",
    "rcp_name": "recipe_name",

    # log_type
    "type": "log_type", "log_type": "log_type", "category": "log_type",

    # status
    "state": "status", "tool_state": "status", "op_status": "status",

    # event_description
    "msg": "event_description", "message": "event_description",
    "description": "event_description", "desc": "event_description",
    "event": "event_description", "detail": "event_description",
}


class FieldNameHashTable:
    """
    In-memory hash table mapping vendor field names to canonical field names.
    Backed by a JSON file for persistence across process restarts.
    """

    def __init__(self, path: str = FIELD_MAPPINGS_PATH):
        self.path = path
        self.table: dict[str, str] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def lookup(self, vendor_field: str) -> str | None:
        """
        Return the canonical field name for vendor_field, or None.
        Lookup is case-insensitive.
        """
        return self.table.get(vendor_field.lower().strip())

    def store(self, vendor_field: str, canonical_name: str) -> None:
        """
        Add or update a vendor → canonical mapping.
        Raises ValueError if canonical_name is not in CANONICAL_SCHEMA.
        """
        if canonical_name not in CANONICAL_SCHEMA:
            raise ValueError(
                f"'{canonical_name}' is not in CANONICAL_SCHEMA. "
                f"Store vendor fields that don't match in extra_fields."
            )
        key = vendor_field.lower().strip()
        self.table[key] = canonical_name
        self._save()

    def bulk_store(self, mappings: dict[str, str]) -> None:
        """Add multiple mappings at once and save once."""
        for vendor, canonical in mappings.items():
            if canonical in CANONICAL_SCHEMA:
                self.table[vendor.lower().strip()] = canonical
            else:
                logger.debug("Skipping invalid canonical name: %s", canonical)
        self._save()

    def all_mappings(self) -> dict[str, str]:
        """Return a copy of the full mapping table."""
        return dict(self.table)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Load from JSON file, then overlay INITIAL_MAPPINGS for any missing keys."""
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as fh:
                    stored = json.load(fh)
                # Only keep entries whose canonical name is still valid
                self.table = {
                    k: v for k, v in stored.items()
                    if v in CANONICAL_SCHEMA
                }
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Could not load field mappings from %s: %s", self.path, exc)
                self.table = {}

        # Always ensure initial synonyms are present (don't overwrite learned ones)
        for vendor, canonical in INITIAL_MAPPINGS.items():
            key = vendor.lower().strip()
            if key not in self.table:
                self.table[key] = canonical

        self._save()
        self._sync_db()

    def _save(self) -> None:
        """Persist the current table to JSON and keep the SQLite mirror in sync."""
        try:
            with open(self.path, "w", encoding="utf-8") as fh:
                json.dump(self.table, fh, indent=2, sort_keys=True)
        except OSError as exc:
            logger.error("Could not save field mappings to %s: %s", self.path, exc)
        self._sync_db()

    def _sync_db(self) -> None:
        """Mirror the in-memory table to the field_mappings SQLite table."""
        try:
            from database.writer import upsert_field_mappings
            upsert_field_mappings(self.table)
        except Exception as exc:
            logger.warning("Could not sync field_mappings to DB: %s", exc)
