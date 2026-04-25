"""
config.py — Canonical schema, constants, and pipeline configuration.
Single source of truth for all field names, units, ranges, and pipeline settings.
"""

import os

# Load .env file from the project root if present (no error if file is missing)
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"), override=False)
except ImportError:
    pass  # python-dotenv not installed — env vars must be set manually

# ---------------------------------------------------------------------------
# Canonical Schema
# ---------------------------------------------------------------------------

CANONICAL_SCHEMA = {
    # Core — always present
    "record_id",           # UUID, generated on ingestion
    "timestamp",           # ISO 8601 datetime string
    "tool_id",             # Which machine generated this
    "log_source",          # Filename or stream identifier
    "log_type",            # "alarm" | "sensor" | "process" | "event" | "unknown"
    "raw_line",            # Original unparsed line for audit trail

    # Process context
    "recipe_name",         # Process recipe running at time of log
    "process_step",        # Current step in the recipe
    "wafer_id",            # Which wafer is being processed
    "lot_id",              # Production lot identifier
    "chamber_id",          # Which chamber within the tool

    # Measurements — all converted to canonical units
    "temperature",         # float, canonical unit: Celsius
    "pressure",            # float, canonical unit: Pascal (Pa)
    "rf_power",            # float, canonical unit: Watts
    "flow_rate",           # float, canonical unit: sccm
    "voltage",             # float, canonical unit: Volts
    "current",             # float, canonical unit: Amperes

    # Events and alarms
    "alarm_code",          # Vendor alarm identifier string
    "alarm_severity",      # "critical" | "warning" | "info"
    "event_description",   # Free text description field
    "status",              # Tool operational status string

    # Metadata
    "extra_fields",        # dict — any fields not in canonical schema
    "mapping_confidence",  # float 0-1, LLM confidence score
    "parse_flags",         # list of strings — warnings raised during parsing
    "anomaly_score",       # float, isolation forest score (-1 to 1)
    "dedup_key",           # sha256 hash for deduplication
}

LOG_TYPE_VALUES = {"alarm", "sensor", "process", "event", "unknown"}
ALARM_SEVERITY_VALUES = {"critical", "warning", "info"}

# Fields that must never be None in a valid record
REQUIRED_FIELDS = {"raw_line"}

# ---------------------------------------------------------------------------
# Unit Conversion Maps
# ---------------------------------------------------------------------------

CANONICAL_UNIT_MAP = {
    "temperature": {
        "C":    lambda x: x,
        "°C":   lambda x: x,
        "F":    lambda x: (x - 32) * 5 / 9,
        "°F":   lambda x: (x - 32) * 5 / 9,
        "K":    lambda x: x - 273.15,
    },
    "pressure": {
        "Pa":    lambda x: x,
        "kPa":   lambda x: x * 1_000,
        "MPa":   lambda x: x * 1_000_000,
        "mTorr": lambda x: x * 0.133322,
        "Torr":  lambda x: x * 133.322,
        "psi":   lambda x: x * 6_894.76,
        "bar":   lambda x: x * 100_000,
        "mbar":  lambda x: x * 100,
        "atm":   lambda x: x * 101_325,
    },
    "rf_power": {
        "W":  lambda x: x,
        "kW": lambda x: x * 1_000,
        "mW": lambda x: x / 1_000,
    },
    "flow_rate": {
        "sccm": lambda x: x,
        "slm":  lambda x: x * 1_000,
        "slpm": lambda x: x * 1_000,
        "ccm":  lambda x: x,
    },
    "voltage": {
        "V":  lambda x: x,
        "mV": lambda x: x / 1_000,
        "kV": lambda x: x * 1_000,
    },
    "current": {
        "A":  lambda x: x,
        "mA": lambda x: x / 1_000,
        "uA": lambda x: x / 1_000_000,
    },
}

# ---------------------------------------------------------------------------
# Valid Physical Ranges (used for range-checking and unit inference)
# ---------------------------------------------------------------------------

CANONICAL_FIELD_RANGES = {
    "temperature": (-200.0, 1500.0),    # Celsius
    "pressure":    (0.0,    1e8),       # Pascal
    "rf_power":    (0.0,    1e6),       # Watts
    "flow_rate":   (0.0,    1e5),       # sccm
    "voltage":     (-1e4,   1e4),       # Volts
    "current":     (-1e4,   1e4),       # Amperes
}

MEASUREMENT_FIELDS = set(CANONICAL_UNIT_MAP.keys())

# ---------------------------------------------------------------------------
# LLM Configuration
# ---------------------------------------------------------------------------

# Default OpenAI chat model. Override with LLM_MODEL in .env if you have access to a different model.
# Batching ensures cost is managed — never called per-line.
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-3.5-turbo")
LLM_MAX_TOKENS = 4096
LLM_TEMPERATURE = 0.0   # Deterministic for regex generation

# ---------------------------------------------------------------------------
# Cache / Registry Configuration
# ---------------------------------------------------------------------------

CACHE_SIMILARITY_THRESHOLD = 0.8
BATCH_LLM_MAX_SIZE = 20          # Max cache-miss lines per LLM call

# ---------------------------------------------------------------------------
# Streaming Configuration
# ---------------------------------------------------------------------------

STREAM_BUFFER_SIZE = 50           # Records to buffer before flush
STREAM_FLUSH_INTERVAL_SEC = 2.0   # Max seconds before flush regardless of buffer

# ---------------------------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------------------------

DB_BATCH_INSERT_SIZE = 500        # Records per database transaction
DB_PATH = os.environ.get("PIPELINE_DB_PATH", "output.db")
REGISTRY_DB_PATH = os.environ.get("PIPELINE_REGISTRY_PATH", "registry.db")
FIELD_MAPPINGS_PATH = os.environ.get("PIPELINE_FIELD_MAPPINGS", "field_mappings.json")
CANDIDATE_POOL_DIR = os.environ.get("PIPELINE_CANDIDATE_POOL", "candidate_pool")
ANOMALY_MODEL_PATH = os.environ.get("PIPELINE_ANOMALY_MODEL", "anomaly_model.joblib")

# ---------------------------------------------------------------------------
# Validator (Component 14 / Step 17)
# ---------------------------------------------------------------------------

def validate_record(record: dict) -> tuple[bool, list[str]]:
    """
    Validate a record against the canonical schema.
    Returns (is_valid, list_of_errors).
    Any vendor fields not in CANONICAL_SCHEMA must already be in extra_fields.
    """
    errors = []

    # Check all top-level keys are canonical
    extra_fields = record.get("extra_fields") or {}
    for key in record:
        if key not in CANONICAL_SCHEMA:
            errors.append(f"unknown_top_level_field:{key}")

    # Required fields
    for field in REQUIRED_FIELDS:
        if record.get(field) is None:
            errors.append(f"missing_required:{field}")

    # log_type enum check
    log_type = record.get("log_type")
    if log_type is not None and log_type not in LOG_TYPE_VALUES:
        errors.append(f"invalid_log_type:{log_type}")

    # alarm_severity enum check
    alarm_severity = record.get("alarm_severity")
    if alarm_severity is not None and alarm_severity not in ALARM_SEVERITY_VALUES:
        errors.append(f"invalid_alarm_severity:{alarm_severity}")

    # mapping_confidence range check
    conf = record.get("mapping_confidence")
    if conf is not None:
        try:
            conf_f = float(conf)
            if not (0.0 <= conf_f <= 1.0):
                errors.append(f"mapping_confidence_out_of_range:{conf_f}")
        except (TypeError, ValueError):
            errors.append(f"mapping_confidence_not_float:{conf}")

    # extra_fields must be a dict
    if "extra_fields" in record and record["extra_fields"] is not None:
        if not isinstance(record["extra_fields"], dict):
            errors.append("extra_fields_not_dict")

    # parse_flags must be a list
    if "parse_flags" in record and record["parse_flags"] is not None:
        if not isinstance(record["parse_flags"], list):
            errors.append("parse_flags_not_list")

    return (len(errors) == 0, errors)


def move_extra_fields(record: dict) -> dict:
    """
    Move any keys not in CANONICAL_SCHEMA into record['extra_fields'].
    Modifies record in-place and returns it.
    """
    if "extra_fields" not in record or record["extra_fields"] is None:
        record["extra_fields"] = {}

    to_move = [k for k in list(record.keys()) if k not in CANONICAL_SCHEMA]
    for key in to_move:
        record["extra_fields"][key] = record.pop(key)

    return record
