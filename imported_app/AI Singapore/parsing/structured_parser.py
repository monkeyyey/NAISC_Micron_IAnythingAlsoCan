"""
parsing/structured_parser.py — Parsers for fully structured log formats.

Each function returns list[dict] with the original vendor field names intact.
Nested structures are flattened using dot notation.
"""

import csv
import io
import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flatten(obj: Any, prefix: str = "", sep: str = ".") -> dict:
    """
    Recursively flatten a nested dict/list using dot notation.
    Lists use index notation: measurements[0].value
    """
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}{sep}{k}" if prefix else str(k)
            out.update(_flatten(v, key, sep))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            key = f"{prefix}[{i}]"
            out.update(_flatten(v, key, sep))
    else:
        out[prefix] = obj
    return out


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------

def parse_json(content: str) -> list[dict]:
    """
    Handle single JSON object, JSON array, or newline-delimited JSON (NDJSON).
    Nested structures are flattened with dot notation.
    """
    records = []
    content = content.strip()

    # Try as a single JSON value first
    if content.startswith("{") or content.startswith("["):
        try:
            obj = json.loads(content)
            if isinstance(obj, dict):
                records.append(_flatten(obj))
                return records
            if isinstance(obj, list):
                for item in obj:
                    if isinstance(item, dict):
                        records.append(_flatten(item))
                return records
        except json.JSONDecodeError:
            pass  # fall through to NDJSON parsing

    # Newline-delimited JSON
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                records.append(_flatten(obj))
            else:
                records.append({"raw_text": line})
        except json.JSONDecodeError as exc:
            logger.debug("JSON parse error on line: %s — %s", line[:80], exc)
            records.append({"raw_text": line, "_parse_error": str(exc)})

    return records


# ---------------------------------------------------------------------------
# XML
# ---------------------------------------------------------------------------

def parse_xml(content: str) -> list[dict]:
    """
    Parse XML. Each child element of the root becomes one record dict.
    Tag text content and attributes are extracted as field values.
    Nested tags are flattened.
    """
    try:
        from lxml import etree
    except ImportError:
        import xml.etree.ElementTree as etree  # type: ignore

    records = []
    try:
        root = etree.fromstring(content.encode("utf-8") if isinstance(content, str) else content)
    except Exception as exc:
        logger.error("XML parse error: %s", exc)
        return []

    def _extract_element(elem, prefix: str = "") -> dict:
        result = {}
        tag = elem.tag if isinstance(elem.tag, str) else str(elem.tag)
        # Strip namespace
        tag = re.sub(r"\{[^}]*\}", "", tag)
        key_base = f"{prefix}.{tag}" if prefix else tag

        # Attributes
        for attr_name, attr_val in elem.attrib.items():
            result[f"{key_base}_{attr_name}"] = attr_val

        # Text content
        text = (elem.text or "").strip()
        if text:
            result[key_base] = text

        # Children
        for child in elem:
            result.update(_extract_element(child, key_base))

        return result

    # If root has child elements that look like record containers, iterate them
    children = list(root)
    if children:
        for child in children:
            record = _extract_element(child)
            if record:
                records.append(record)
    else:
        record = _extract_element(root)
        if record:
            records.append(record)

    return records


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

def parse_csv(content: str) -> list[dict]:
    """Parse CSV with header row. Returns one dict per data row."""
    return _parse_delimited(content, delimiter=",")


def parse_tsv(content: str) -> list[dict]:
    """Parse TSV with header row. Returns one dict per data row."""
    return _parse_delimited(content, delimiter="\t")


def _parse_delimited(content: str, delimiter: str) -> list[dict]:
    records = []
    reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    for row in reader:
        # csv.DictReader returns OrderedDict; convert and strip None keys
        record = {
            k.strip(): (v.strip() if v else None)
            for k, v in row.items()
            if k is not None
        }
        records.append(record)
    return records


# ---------------------------------------------------------------------------
# YAML
# ---------------------------------------------------------------------------

def parse_yaml(content: str) -> list[dict]:
    """
    Parse YAML. Supports multi-document files (--- separator).
    Nested structures are flattened with dot notation.
    """
    try:
        import yaml
    except ImportError:
        logger.error("PyYAML not installed — cannot parse YAML")
        return []

    records = []
    try:
        docs = list(yaml.safe_load_all(content))
        for doc in docs:
            if doc is None:
                continue
            if isinstance(doc, dict):
                records.append(_flatten(doc))
            elif isinstance(doc, list):
                for item in doc:
                    if isinstance(item, dict):
                        records.append(_flatten(item))
    except yaml.YAMLError as exc:
        logger.error("YAML parse error: %s", exc)

    return records


# ---------------------------------------------------------------------------
# Logfmt
# ---------------------------------------------------------------------------

def parse_logfmt(lines: list[str]) -> list[dict]:
    """
    Parse logfmt lines.  Each line → one dict.
    Format: key=value key2="value with spaces" key3=bare_value
    """
    records = []
    # Pattern: key=value or key="quoted value"
    _kv_re = re.compile(r'(\w+)=("(?:[^"\\]|\\.)*"|\S*)')

    for line in lines:
        line = line.strip()
        if not line:
            continue
        record: dict[str, str | None] = {}
        pos = 0
        for match in _kv_re.finditer(line):
            key = match.group(1)
            val = match.group(2)
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1].replace('\\"', '"')
            record[key] = val or None
        if not record:
            record["raw_text"] = line
        records.append(record)

    return records
