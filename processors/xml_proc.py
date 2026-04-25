"""
xml_proc.py — Processor for XML log files.

Supports:
    - A root element containing repeated log/event/alarm children
    - A single log-like XML object
    - Repeated XML fragments wrapped into a synthetic root as fallback
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Iterator

from ..base_processor import BaseLogProcessor, LogRecord, extract_level
from ..normalizer import clean_text, parse_timestamp, to_iso8601

_TS_KEYS = {"timestamp", "time", "datetime", "date", "ts", "log_time"}
_LVL_KEYS = {"level", "severity", "priority", "loglevel"}
_MSG_KEYS = {"message", "msg", "text", "event", "description", "detail"}
_RECORD_TAGS = {"log", "entry", "event", "record", "alarm", "message"}


class XMLProcessor(BaseLogProcessor):
    """Parse XML logs into flattened records."""

    FORMAT = "xml"

    def _run_pipeline(self, text: str, *, source: str) -> Iterator[LogRecord]:
        cleaned = text.strip()
        if not cleaned:
            return

        root = self._parse_xml(cleaned)
        if root is None:
            yield self._make_corrupted(cleaned[:200], 1, source, "invalid_xml")
            return

        entries = self._extract_entries(root)
        for lineno, element in enumerate(entries, start=1):
            yield self._build_record(element, lineno, source)

    def _parse_lines(self, lines: list[str], *, source: str) -> Iterator[LogRecord]:
        raise NotImplementedError("Use _run_pipeline for XML input")

    def _parse_xml(self, text: str) -> ET.Element | None:
        try:
            return ET.fromstring(text)
        except ET.ParseError:
            try:
                return ET.fromstring(f"<root>{text}</root>")
            except ET.ParseError:
                return None

    def _extract_entries(self, root: ET.Element) -> list[ET.Element]:
        children = [child for child in list(root) if isinstance(child.tag, str)]
        if not children:
            return [root]

        child_tags = {self._strip_ns(child.tag).lower() for child in children}
        if len(children) > 1 and (child_tags & _RECORD_TAGS):
            return children
        if len(child_tags) == 1:
            return children
        return [root]

    def _build_record(self, element: ET.Element, lineno: int, source: str) -> LogRecord:
        raw = ET.tostring(element, encoding="unicode")
        flat = self._flatten_element(element)

        ts_raw = None
        ts_iso = None
        for key in _TS_KEYS:
            if key in flat and flat[key]:
                ts_candidate = str(flat[key])
                dt = parse_timestamp(ts_candidate)
                if dt:
                    ts_raw = ts_candidate
                    ts_iso = to_iso8601(dt)
                    break

        level = None
        for key in _LVL_KEYS:
            if key in flat and flat[key]:
                level = str(flat[key]).upper()
                if level == "WARNING":
                    level = "WARN"
                break

        message = ""
        for key in _MSG_KEYS:
            if key in flat and flat[key]:
                message = clean_text(str(flat[key]))
                break

        if not message:
            text_parts = [
                clean_text(text)
                for text in flat.values()
                if isinstance(text, str) and text.strip()
            ]
            message = text_parts[-1] if text_parts else self._strip_ns(element.tag)

        if level is None:
            level = extract_level(message)

        reserved = _TS_KEYS | _LVL_KEYS | _MSG_KEYS
        fields: dict[str, Any] = {k: v for k, v in flat.items() if k not in reserved}

        return self._normalize_record(
            LogRecord(
                raw=raw,
                source=source,
                line_number=lineno,
                format=self.FORMAT,
                timestamp_raw=ts_raw,
                timestamp=ts_iso,
                message=message,
                level=level,
                fields=fields,
            )
        )

    def _flatten_element(
        self,
        element: ET.Element,
        prefix: str = "",
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        tag = self._strip_ns(element.tag)
        current = prefix or tag

        for attr_key, attr_val in element.attrib.items():
            result[f"{current}.@{self._strip_ns(attr_key)}"] = attr_val
            result[f"@{self._strip_ns(attr_key)}"] = attr_val

        text = (element.text or "").strip()
        if text:
            result[current] = text
            if prefix:
                result[tag] = text

        children = [child for child in list(element) if isinstance(child.tag, str)]
        for child in children:
            child_tag = self._strip_ns(child.tag)
            child_prefix = f"{current}.{child_tag}"
            result.update(self._flatten_element(child, child_prefix))

        return result

    @staticmethod
    def _strip_ns(tag: str) -> str:
        return tag.split("}", 1)[-1]
