"""
parsing/regex_engine.py — Apply LLM-generated regex mappings to log lines.

Takes a dict of {canonical_field: regex_pattern} and a log line.
Returns extracted values and any per-field flags.
"""

import logging
import re

logger = logging.getLogger(__name__)


def apply_mapping(line: str, patterns: dict[str, str]) -> tuple[dict, list[str]]:
    """
    Apply regex patterns to a log line to extract field values.

    Args:
        line:     The raw log line.
        patterns: {canonical_field_name: regex_pattern_with_one_capture_group}

    Returns:
        (result_dict, flags)
        result_dict: {canonical_field_name: extracted_string_value | None}
        flags:       list of "pattern_miss:{field}" for every pattern that didn't match
    """
    result: dict[str, str | None] = {}
    flags: list[str] = []

    for field, pattern in patterns.items():
        if not pattern:
            result[field] = None
            flags.append(f"empty_pattern:{field}")
            continue

        try:
            match = re.search(pattern, line)
        except re.error as exc:
            logger.debug("Regex error for field %s pattern '%s': %s", field, pattern, exc)
            result[field] = None
            flags.append(f"regex_error:{field}")
            continue

        if match:
            # Use group(1) if it exists; fall back to group(0) if no capture group
            try:
                value = match.group(1)
            except IndexError:
                value = match.group(0)
            result[field] = value.strip() if value else None
        else:
            result[field] = None
            flags.append(f"pattern_miss:{field}")

    return result, flags


def apply_mapping_batch(
    lines: list[str],
    patterns: dict[str, str],
) -> list[tuple[dict, list[str]]]:
    """Apply the same pattern dict to multiple lines."""
    return [apply_mapping(line, patterns) for line in lines]
