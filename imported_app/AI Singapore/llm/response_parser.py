"""
llm/response_parser.py — Parse and validate LLM JSON output.

Validates:
  - JSON parses correctly
  - Each field name is in CANONICAL_SCHEMA
  - Each regex compiles without error (exactly one capture group)
  - confidence is a float in [0, 1]

Invalid entries are not silently dropped — they are returned with
confidence=0 and a parse_flag added.
"""

import json
import logging
import re
from typing import Any

from config import CANONICAL_SCHEMA

logger = logging.getLogger(__name__)

# Strip markdown code fences if the model wraps output
_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def parse_llm_response(
    raw_response: str,
    expected_signatures: list[str],
) -> list[dict]:
    """
    Parse the raw LLM text response into a list of validated mapping dicts.

    Args:
        raw_response:        The model's text output.
        expected_signatures: The signatures for the lines that were sent.

    Returns:
        List of dicts, one per input line, each with keys:
          signature, fields, confidence, parse_flags
        Length always matches len(expected_signatures).
    """
    # Strip fences
    text = raw_response.strip()
    fence_match = _FENCE_RE.search(text)
    if fence_match:
        text = fence_match.group(1).strip()

    # Parse JSON
    parsed: list[Any] = []
    try:
        obj = json.loads(text)
        if isinstance(obj, list):
            parsed = obj
        elif isinstance(obj, dict):
            parsed = [obj]
        else:
            logger.warning("LLM response was neither list nor dict")
    except json.JSONDecodeError as exc:
        logger.error("LLM response JSON decode error: %s\n---\n%s\n---", exc, text[:500])

    # Ensure we have one entry per expected signature
    # Pad or truncate to match
    results = []
    for i, sig in enumerate(expected_signatures):
        raw_entry = parsed[i] if i < len(parsed) else {}
        validated = _validate_entry(raw_entry, sig)
        results.append(validated)

    return results


def _validate_entry(entry: dict, expected_sig: str) -> dict:
    """
    Validate a single mapping entry from the LLM.
    Returns a cleaned dict with keys: signature, fields, confidence, parse_flags.
    """
    flags: list[str] = []

    if not isinstance(entry, dict):
        return {
            "signature":  expected_sig,
            "fields":     {},
            "confidence": 0.0,
            "parse_flags": ["llm_response_not_dict"],
        }

    # Signature — use expected_sig as authoritative source
    signature = expected_sig

    # Confidence
    confidence = 0.0
    raw_conf = entry.get("confidence", 0.0)
    try:
        confidence = float(raw_conf)
        confidence = max(0.0, min(1.0, confidence))
    except (TypeError, ValueError):
        flags.append("invalid_confidence_value")

    # Fields
    raw_fields = entry.get("fields", {})
    if not isinstance(raw_fields, dict):
        flags.append("fields_not_dict")
        raw_fields = {}

    validated_fields: dict[str, str] = {}
    for field_name, pattern in raw_fields.items():
        # Check canonical schema membership
        if field_name not in CANONICAL_SCHEMA:
            logger.debug("LLM returned unknown field '%s' — skipping", field_name)
            flags.append(f"unknown_field:{field_name}")
            continue

        # Validate regex compiles and has exactly one capture group
        if not isinstance(pattern, str):
            flags.append(f"non_string_pattern:{field_name}")
            continue

        try:
            compiled = re.compile(pattern)
        except re.error as exc:
            logger.debug("Invalid regex for %s: %s — %s", field_name, pattern, exc)
            flags.append(f"invalid_regex:{field_name}")
            continue

        # Warn if capture group count != 1 but still store it
        groups = compiled.groups
        if groups != 1:
            flags.append(f"regex_group_count_{groups}:{field_name}")
            if groups == 0:
                # Unusable — skip
                continue

        validated_fields[field_name] = pattern

    # If all fields were invalid, set confidence to 0
    if not validated_fields and confidence > 0:
        confidence = 0.0
        flags.append("no_valid_fields")

    return {
        "signature":   signature,
        "fields":      validated_fields,
        "confidence":  confidence,
        "parse_flags": flags,
    }
