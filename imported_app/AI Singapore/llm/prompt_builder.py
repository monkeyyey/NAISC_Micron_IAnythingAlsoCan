"""
llm/prompt_builder.py — Build ICL prompts for LLM mapping generation.

The prompt has three sections:
  1. System instruction — what the model must do and the output format
  2. ICL demonstrations — similar examples retrieved from the candidate pool
  3. Query — the batch of cache-miss lines to map
"""

from config import CANONICAL_SCHEMA


def build_system_prompt() -> str:
    """Return the static system instruction."""
    schema_list = "\n".join(f"  - {f}" for f in sorted(CANONICAL_SCHEMA))
    return f"""You are an expert log parser for semiconductor manufacturing equipment (e.g. CVD, ETCH, PVD tools).

Your task: analyse each log line provided and generate field extraction rules.

For EACH log line:
1. Identify all field names and their values.
2. Generate a Python regex pattern (with exactly one capture group) that extracts each field.
3. Map each vendor field name to the closest canonical schema field.
4. Return ONLY a JSON array — no explanation, no markdown fences.

Canonical schema fields:
{schema_list}

Return format (one object per log line, in a JSON array):
[
  {{
    "signature": "<log_signature_string>",
    "fields": {{
      "<canonical_field_name>": "<regex_with_one_capture_group>"
    }},
    "confidence": <float between 0 and 1>
  }},
  ...
]

Rules:
- Only include canonical fields you are confident about.
- Every regex must have exactly ONE capture group: (...).
- Confidence 1.0 = certain, 0.0 = guessing.
- If a line has no recognisable fields, return {{"signature": "...", "fields": {{}}, "confidence": 0.0}}.
- Return ONLY the JSON array. No other text."""


def build_user_prompt(
    log_lines: list[str],
    signatures: list[str],
    examples: list[dict],
) -> str:
    """
    Build the user-turn prompt.

    Args:
        log_lines:  batch of cache-miss log lines
        signatures: corresponding log signatures (same order as log_lines)
        examples:   list of dicts with keys 'line' and 'mapping' (from candidate pool)
    """
    parts: list[str] = []

    # Section 2: ICL demonstrations
    if examples:
        parts.append("=== EXAMPLES (use these to calibrate your output) ===\n")
        for ex in examples:
            line = ex.get("line", "")
            mapping = ex.get("mapping", {})
            fields_str = "\n".join(
                f'      "{k}": "{v}"'
                for k, v in mapping.get("fields", {}).items()
            )
            conf = mapping.get("confidence", 0.9)
            parts.append(
                f'Log line: "{line}"\n'
                f"Mapping:\n{{\n"
                f'  "fields": {{\n{fields_str}\n  }},\n'
                f'  "confidence": {conf}\n'
                f"}}\n"
            )
        parts.append("")

    # Section 3: Query
    parts.append("=== LOG LINES TO MAP ===\n")
    for i, (line, sig) in enumerate(zip(log_lines, signatures)):
        parts.append(f'{i + 1}. signature="{sig}" line="{line}"')

    parts.append(
        "\nReturn a JSON array with one object per log line, "
        "in the same order. Include the signature field in each object."
    )

    return "\n".join(parts)
