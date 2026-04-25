"""
normalisation/unit_normaliser.py — Convert measurement units to canonical standards.

Canonical units:
  temperature → Celsius
  pressure    → Pascal
  rf_power    → Watts
  flow_rate   → sccm
  voltage     → Volts
  current     → Amperes

Unit detection:
  1. If the value is a string like "85.2C" or "0.9Pa", extract numeric + unit.
  2. Look up the unit in CANONICAL_UNIT_MAP.
  3. If unit is unknown, store raw float and flag.
  4. If no unit found, try to infer from CANONICAL_FIELD_RANGES.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Callable

from config import CANONICAL_UNIT_MAP, CANONICAL_FIELD_RANGES, MEASUREMENT_FIELDS

logger = logging.getLogger(__name__)

# Pattern: optional sign, digits, optional decimal, optional exponent, optional unit
_VALUE_UNIT_RE = re.compile(
    r"^([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s*([A-Za-z°µ/]+)?$"
)


def normalise_units(record: dict) -> dict:
    """
    Detect and convert units for all measurement fields in the record.

    Modifies record in-place. Updates parse_flags list.
    Returns the modified record.
    """
    if "parse_flags" not in record or record["parse_flags"] is None:
        record["parse_flags"] = []

    for field in MEASUREMENT_FIELDS:
        raw_value = record.get(field)
        if raw_value is None:
            continue

        converted, flags = _normalise_field(field, raw_value)
        record[field] = converted
        record["parse_flags"].extend(flags)

    return record


def _normalise_field(field: str, raw_value) -> tuple[float | None, list[str]]:
    """
    Attempt to normalise a single measurement field value.

    Returns (converted_float_or_None, list_of_flags).
    """
    flags: list[str] = []
    unit_map = CANONICAL_UNIT_MAP.get(field, {})

    # If already a number, check range and return
    if isinstance(raw_value, (int, float)):
        val = float(raw_value)
        return _range_check(field, val, flags)

    # String processing
    s = str(raw_value).strip()
    if not s:
        return None, flags

    m = _VALUE_UNIT_RE.match(s)
    if not m:
        flags.append(f"unparseable_value:{field}:{s[:30]}")
        return None, flags

    num_str = m.group(1)
    unit_str = (m.group(2) or "").strip()

    try:
        num = float(num_str)
    except ValueError:
        flags.append(f"type_error:{field}")
        return None, flags

    if not unit_str:
        # No unit — infer from range
        return _range_check(field, num, flags)

    # Exact unit match
    if unit_str in unit_map:
        converted = unit_map[unit_str](num)
        return _range_check(field, converted, flags)

    # Case-insensitive match
    unit_lower = unit_str.lower()
    for known_unit, converter in unit_map.items():
        if known_unit.lower() == unit_lower:
            converted = converter(num)
            return _range_check(field, converted, flags)

    # Unit not recognised
    flags.append(f"unknown_unit:{field}:{unit_str}")
    return _range_check(field, num, flags)


def _range_check(field: str, value: float, flags: list[str]) -> tuple[float | None, list[str]]:
    """Verify value is within the expected physical range."""
    field_range = CANONICAL_FIELD_RANGES.get(field)
    if field_range is None:
        return value, flags

    lo, hi = field_range
    if not (lo <= value <= hi):
        flags.append(f"out_of_range:{field}:{value:.4g}")
        return None, flags

    return value, flags


# ---------------------------------------------------------------------------
# Named transform library
# ---------------------------------------------------------------------------
# Each function: raw vendor value in → canonical value out.
# The LLM can reference these by name when it returns a mapping.
# apply_transform(name, value) is the safe caller.

def _normalize_unit_id(x: Any) -> str:
    """Unit1 / Unit 01 / UNIT-1 / unit_1 → unit_01"""
    s = str(x).strip().lower()
    m = re.search(r"(\d+)", s)
    if not m:
        return s
    return f"unit_{int(m.group(1)):02d}"


def _make_enum_normalizer(allowed: list[str]) -> Callable[[Any], str]:
    """Return a transform that maps a raw value to one of the allowed strings."""
    allowed_lower = {a.lower(): a for a in allowed}

    def _transform(x: Any) -> str:
        s = str(x).strip().lower().replace(" ", "_").replace("-", "_")
        if s in allowed_lower:
            return allowed_lower[s]
        for key, canonical in allowed_lower.items():
            if key in s or s in key:
                return canonical
        raise ValueError(f"{x!r} not in allowed set {allowed}")

    return _transform


def _parse_flexible_datetime(x: Any) -> str:
    """Multiple date/time formats → ISO 8601 UTC string."""
    s = str(x).strip()
    _formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
        "%m/%d/%Y %H:%M:%S", "%m/%d/%Y",
        "%d-%m-%Y", "%d %B %Y", "%d %b %Y",
    ]
    for fmt in _formats:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    raise ValueError(f"Could not parse datetime: {s!r}")


TRANSFORMS: dict[str, Callable[[Any], Any]] = {
    # Generic
    "identity":               lambda x: x,
    "to_float":               lambda x: float(x),
    "to_int":                 lambda x: int(x),
    "to_string":              lambda x: str(x).strip(),

    # Datetime
    "parse_datetime":         _parse_flexible_datetime,
    "epoch_ms_to_utc":        lambda x: datetime.fromtimestamp(int(x) / 1000, timezone.utc).isoformat(),
    "epoch_s_to_utc":         lambda x: datetime.fromtimestamp(int(x), timezone.utc).isoformat(),

    # Temperature → Celsius
    "celsius_to_celsius":     lambda x: float(x),
    "fahrenheit_to_celsius":  lambda x: (float(x) - 32.0) * 5.0 / 9.0,
    "kelvin_to_celsius":      lambda x: float(x) - 273.15,

    # Pressure → Pascal
    "pa_to_pa":               lambda x: float(x),
    "kpa_to_pa":              lambda x: float(x) * 1_000.0,
    "torr_to_pa":             lambda x: float(x) * 133.322,
    "mbar_to_pa":             lambda x: float(x) * 100.0,
    "psi_to_pa":              lambda x: float(x) * 6_894.76,

    # Power → Watts
    "w_to_w":                 lambda x: float(x),
    "kw_to_w":                lambda x: float(x) * 1_000.0,
    "mw_to_w":                lambda x: float(x) / 1_000.0,

    # IDs and enums
    "normalize_unit_id":      _normalize_unit_id,
    "normalize_alarm_severity": _make_enum_normalizer(["critical", "warning", "info"]),
    "normalize_status":       _make_enum_normalizer(["active", "idle", "failure", "normal", "processing"]),
}


def apply_transform(name: str, value: Any) -> Any:
    """
    Apply a named transform from TRANSFORMS to a value.
    Raises KeyError for unknown names, lets transform exceptions propagate.
    """
    fn = TRANSFORMS[name]
    return fn(value)
