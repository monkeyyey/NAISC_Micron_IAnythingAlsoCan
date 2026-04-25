"""
tests/test_pipeline.py — Integration tests for the semantic pipeline.

These tests run without the LLM (cache-miss LLM calls are mocked) so they
can be executed in CI without an API key. They verify every component from
format detection through to database insert.

Run: python -m pytest tests/test_pipeline.py -v
"""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

# Ensure project root is on the path
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

LOG_DIR = os.path.join(_ROOT, "logs")


# ---------------------------------------------------------------------------
# Component unit tests
# ---------------------------------------------------------------------------

class TestSignatureGenerator(unittest.TestCase):
    def test_same_template_different_values(self):
        from signature.generator import generate_signature
        a = generate_signature("TOOL ETCH01 TEMP 85.2 STATUS PROCESSING")
        b = generate_signature("TOOL ETCH01 TEMP 90.0 STATUS PROCESSING")
        self.assertEqual(a, b, "Same template, different numbers → same signature")

    def test_different_templates(self):
        from signature.generator import generate_signature
        a = generate_signature("TOOL ETCH01 TEMP 85.2 STATUS PROCESSING")
        b = generate_signature("TOOL CVD02 ALARM E334 SEVERITY CRITICAL")
        self.assertNotEqual(a, b)

    def test_uuid_masked(self):
        from signature.generator import generate_signature
        a = generate_signature("record 550e8400-e29b-41d4-a716-446655440000 inserted")
        b = generate_signature("record 110e8400-e29b-41d4-a716-446655440000 inserted")
        self.assertEqual(a, b)

    def test_ip_masked(self):
        from signature.generator import generate_signature
        a = generate_signature("connected to 192.168.1.100")
        b = generate_signature("connected to 10.0.0.1")
        self.assertEqual(a, b)


class TestFormatDetection(unittest.TestCase):
    def test_csv(self):
        from parsing.format_router import detect_format
        path = os.path.join(LOG_DIR, "logs.csv")
        self.assertEqual(detect_format(path), "csv")

    def test_json(self):
        from parsing.format_router import detect_format
        path = os.path.join(LOG_DIR, "logs.jsonl")
        self.assertEqual(detect_format(path), "json")

    def test_xml(self):
        from parsing.format_router import detect_format
        path = os.path.join(LOG_DIR, "logs.xml")
        self.assertEqual(detect_format(path), "xml")

    def test_yaml(self):
        from parsing.format_router import detect_format
        path = os.path.join(LOG_DIR, "logs.yaml")
        self.assertEqual(detect_format(path), "yaml")

    def test_syslog(self):
        from parsing.format_router import detect_format
        path = os.path.join(LOG_DIR, "logs.syslog")
        self.assertEqual(detect_format(path), "syslog")

    def test_logfmt(self):
        from parsing.format_router import detect_format
        path = os.path.join(LOG_DIR, "logs.logfmt")
        self.assertIn(detect_format(path), ("logfmt", "keyvalue"))

    def test_plaintext(self):
        from parsing.format_router import detect_format
        path = os.path.join(LOG_DIR, "plain_text.log")
        self.assertIn(detect_format(path), ("plaintext", "keyvalue"))


class TestStructuredParsers(unittest.TestCase):
    def test_json_parser(self):
        from parsing.structured_parser import parse_json
        content = '{"ts": "2024-04-15T12:03:20", "tool": "ETCH01", "temp_c": 85.2}\n{"ts": "2024-04-15T12:05:00", "tool": "CVD02"}'
        records = parse_json(content)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["tool"], "ETCH01")
        self.assertEqual(records[0]["temp_c"], 85.2)

    def test_json_nested_flatten(self):
        from parsing.structured_parser import parse_json
        content = '{"sensor": {"id": "001", "value": 85.2}}'
        records = parse_json(content)
        self.assertIn("sensor.id", records[0])
        self.assertIn("sensor.value", records[0])

    def test_csv_parser(self):
        from parsing.structured_parser import parse_csv
        content = "timestamp,tool_id,temperature\n2024-04-15T12:03:20,ETCH01,85.2"
        records = parse_csv(content)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["tool_id"], "ETCH01")
        self.assertEqual(records[0]["temperature"], "85.2")

    def test_logfmt_parser(self):
        from parsing.structured_parser import parse_logfmt
        lines = ['ts=2024-04-15T12:03:20 tool=ETCH01 temp=85.2 alarm=E334']
        records = parse_logfmt(lines)


class TestLLMClientQuotaHandling(unittest.TestCase):
    @patch("llm.client.time.sleep")
    def test_quota_error_stops_retries_and_returns_failure(self, mock_sleep):
        from llm.client import LLMClient

        client = LLMClient()
        client._client = MagicMock()
        client._system_prompt = "system prompt"

        quota_exc = Exception("You exceeded your current quota")
        quota_exc.http_status = 429
        quota_exc.code = "insufficient_quota"
        client._client.chat.completions.create.side_effect = quota_exc

        mappings = client.generate_mapping(["line1"], ["sig1"], [])

        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0]["fields"], {})
        self.assertEqual(mappings[0]["confidence"], 0.0)
        self.assertTrue(mappings[0]["parse_flags"])
        self.assertIn("llm_permanent_failure", mappings[0]["parse_flags"][0])
        mock_sleep.assert_not_called()

    def test_yaml_parser(self):
        from parsing.structured_parser import parse_yaml
        content = "---\ntimestamp: '2024-04-15T12:03:20'\ntool_id: ETCH01\ntemperature: 85.2\n"
        records = parse_yaml(content)
        self.assertGreater(len(records), 0)
        self.assertEqual(records[0]["tool_id"], "ETCH01")


class TestUnstructuredParsers(unittest.TestCase):
    def test_keyvalue_parser(self):
        from parsing.unstructured_parser import parse_keyvalue
        lines = ["tool=ETCH01 alarm=E334 temp=72C pressure=0.9Pa wafer_id=WFR_001"]
        records = parse_keyvalue(lines)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["tool"], "ETCH01")
        self.assertEqual(records[0]["alarm"], "E334")

    def test_plaintext_parser(self):
        from parsing.unstructured_parser import parse_plaintext
        lines = ["2024-04-15 12:03:20 TOOL ETCH01 TEMP 85.2"]
        records = parse_plaintext(lines)
        self.assertEqual(len(records), 1)
        self.assertIn("raw_text", records[0])

    def test_delimiter_parser(self):
        from parsing.unstructured_parser import parse_delimiter
        lines = [
            "timestamp|tool_id|temperature|alarm_code",
            "2024-04-15T12:03:20|ETCH01|85.2|E334",
            "2024-04-15T12:05:00|CVD02|420.5|",
        ]
        records = parse_delimiter(lines)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["tool_id"], "ETCH01")


class TestSyslogParser(unittest.TestCase):
    def test_rfc5424(self):
        from parsing.syslog_parser import parse_syslog
        lines = ["<34>1 2024-04-15T12:03:20Z ETCH01 tool-monitor 1234 - - ALARM E334 TEMP=85.2C"]
        records = parse_syslog(lines)
        self.assertEqual(len(records), 1)
        self.assertIn("message", records[0])

    def test_bsd(self):
        from parsing.syslog_parser import parse_syslog
        lines = ["<34>2024-04-15T12:03:20Z ETCH01 tool-monitor[1234]: ALARM E334 TEMP=85.2C"]
        records = parse_syslog(lines)
        self.assertEqual(len(records), 1)
        self.assertIsNotNone(records[0].get("app_name") or records[0].get("message"))


class TestTrie(unittest.TestCase):
    def test_insert_and_exact_match(self):
        from caching.trie import LogTemplateTrie
        trie = LogTemplateTrie()
        trie.insert("TOOL ETCH01 ALARM <*> SEVERITY <*>", {0: "alarm_code", 1: "alarm_severity"}, "sig001")
        result = trie.match("TOOL ETCH01 ALARM E334 SEVERITY CRITICAL")
        self.assertIsNotNone(result)

    def test_no_match(self):
        from caching.trie import LogTemplateTrie
        trie = LogTemplateTrie()
        trie.insert("TOOL ETCH01 ALARM <*>", {0: "alarm_code"}, "sig001")
        result = trie.match("COMPLETELY DIFFERENT LINE FORMAT HERE")
        self.assertIsNone(result)

    def test_extract_values(self):
        from caching.trie import LogTemplateTrie
        trie = LogTemplateTrie()
        template = "TOOL ETCH01 ALARM <*> SEVERITY <*>"
        position_map = {0: "alarm_code", 1: "alarm_severity"}
        trie.insert(template, position_map, "sig001")
        extracted = trie.extract_values("TOOL ETCH01 ALARM E334 SEVERITY CRITICAL", template, position_map)
        self.assertEqual(extracted.get("alarm_code"), "E334")
        self.assertEqual(extracted.get("alarm_severity"), "CRITICAL")


class TestHashTable(unittest.TestCase):
    def test_initial_mappings(self):
        from caching.hash_table import FieldNameHashTable
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            ht = FieldNameHashTable(path=path)
            self.assertEqual(ht.lookup("temp"), "temperature")
            self.assertEqual(ht.lookup("TEMP"), "temperature")  # case-insensitive
            self.assertEqual(ht.lookup("ts"), "timestamp")
            self.assertEqual(ht.lookup("alarm"), "alarm_code")
        finally:
            os.unlink(path)

    def test_store_and_lookup(self):
        from caching.hash_table import FieldNameHashTable
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            ht = FieldNameHashTable(path=path)
            ht.store("MyVendorTemp", "temperature")
            self.assertEqual(ht.lookup("myvendortemp"), "temperature")
        finally:
            os.unlink(path)


class TestMappingRegistry(unittest.TestCase):
    def test_store_and_lookup(self):
        from caching.registry import MappingRegistry
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        try:
            reg = MappingRegistry(db_path=path)
            patterns = {"temperature": r"TEMP=(\d+(?:\.\d+)?)", "alarm_code": r"ALARM=(\w+)"}
            reg.store("abc123", "keyvalue", patterns, 0.95)
            result = reg.lookup("abc123")
            self.assertIsNotNone(result)
            self.assertIn("temperature", result)
            reg.close()
        finally:
            os.unlink(path)

    def test_cache_miss_returns_none(self):
        from caching.registry import MappingRegistry
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        try:
            reg = MappingRegistry(db_path=path)
            self.assertIsNone(reg.lookup("nonexistent"))
            reg.close()
        finally:
            os.unlink(path)


class TestRegexEngine(unittest.TestCase):
    def test_apply_mapping_hit(self):
        from parsing.regex_engine import apply_mapping
        patterns = {
            "tool_id":     r"TOOL=(\w+)",
            "alarm_code":  r"ALARM=(\w+)",
            "temperature": r"TEMP=(\d+(?:\.\d+)?)",
        }
        line = "TOOL=ETCH01 ALARM=E334 TEMP=85.2"
        result, flags = apply_mapping(line, patterns)
        self.assertEqual(result["tool_id"], "ETCH01")
        self.assertEqual(result["alarm_code"], "E334")
        self.assertEqual(result["temperature"], "85.2")
        self.assertEqual(flags, [])

    def test_apply_mapping_miss(self):
        from parsing.regex_engine import apply_mapping
        patterns = {"temperature": r"TEMP=(\d+(?:\.\d+)?)"}
        line = "TOOL=ETCH01 STATUS=IDLE"
        result, flags = apply_mapping(line, patterns)
        self.assertIsNone(result["temperature"])
        self.assertIn("pattern_miss:temperature", flags)


class TestUnitNormaliser(unittest.TestCase):
    def test_celsius_passthrough(self):
        from normalisation.unit_normaliser import normalise_units
        rec = {"temperature": "85.2C", "parse_flags": []}
        normalise_units(rec)
        self.assertAlmostEqual(rec["temperature"], 85.2)

    def test_fahrenheit_conversion(self):
        from normalisation.unit_normaliser import normalise_units
        rec = {"temperature": "185.36F", "parse_flags": []}
        normalise_units(rec)
        self.assertAlmostEqual(rec["temperature"], 85.2, places=1)

    def test_kelvin_conversion(self):
        from normalisation.unit_normaliser import normalise_units
        rec = {"temperature": "358.35K", "parse_flags": []}
        normalise_units(rec)
        self.assertAlmostEqual(rec["temperature"], 85.2, places=1)

    def test_mtorr_to_pascal(self):
        from normalisation.unit_normaliser import normalise_units
        rec = {"pressure": "6749.93mTorr", "parse_flags": []}
        normalise_units(rec)
        self.assertAlmostEqual(rec["pressure"], 900.0, places=0)

    def test_unknown_unit_flagged(self):
        from normalisation.unit_normaliser import normalise_units
        rec = {"temperature": "85.2X", "parse_flags": []}
        normalise_units(rec)
        self.assertTrue(any("unknown_unit" in f for f in rec["parse_flags"]))

    def test_out_of_range_flagged(self):
        from normalisation.unit_normaliser import normalise_units
        rec = {"temperature": "9999C", "parse_flags": []}
        normalise_units(rec)
        self.assertIsNone(rec["temperature"])
        self.assertTrue(any("out_of_range" in f for f in rec["parse_flags"]))


class TestCleaner(unittest.TestCase):
    def test_dedup_key_generated(self):
        from normalisation.cleaner import clean_record
        rec = {
            "tool_id": "ETCH01",
            "timestamp": "2024-04-15T12:03:20",
            "raw_line": "TOOL ETCH01 TEMP 85.2",
            "parse_flags": [],
        }
        clean_record(rec)
        self.assertIn("dedup_key", rec)
        self.assertIsNotNone(rec["dedup_key"])

    def test_timestamp_standardised(self):
        from normalisation.cleaner import clean_record
        rec = {"raw_line": "test", "timestamp": "04/15/2024 12:03:20", "parse_flags": []}
        clean_record(rec)
        self.assertIn("T", rec["timestamp"])

    def test_unix_epoch_timestamp(self):
        from normalisation.cleaner import clean_record
        rec = {"raw_line": "test", "timestamp": 1713182600, "parse_flags": []}
        clean_record(rec)
        self.assertIn("T", rec["timestamp"])

    def test_missing_raw_line_flagged(self):
        from normalisation.cleaner import clean_record
        rec = {"parse_flags": []}
        clean_record(rec)
        self.assertTrue(any("missing_required:raw_line" in f for f in rec["parse_flags"]))


class TestSchemaValidator(unittest.TestCase):
    def test_valid_record(self):
        from config import validate_record
        rec = {
            "record_id": "abc",
            "raw_line": "test line",
            "log_type": "sensor",
            "mapping_confidence": 0.9,
            "parse_flags": [],
            "extra_fields": {},
        }
        is_valid, errors = validate_record(rec)
        self.assertTrue(is_valid, errors)

    def test_invalid_log_type(self):
        from config import validate_record
        rec = {"raw_line": "x", "log_type": "bogus", "parse_flags": [], "extra_fields": {}}
        is_valid, errors = validate_record(rec)
        self.assertFalse(is_valid)
        self.assertTrue(any("invalid_log_type" in e for e in errors))

    def test_confidence_out_of_range(self):
        from config import validate_record
        rec = {"raw_line": "x", "mapping_confidence": 1.5, "parse_flags": [], "extra_fields": {}}
        is_valid, errors = validate_record(rec)
        self.assertFalse(is_valid)


class TestAnomalyDetector(unittest.TestCase):
    def test_returns_zero_when_not_fitted(self):
        from anomaly.detector import AnomalyDetector
        with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as f:
            path = f.name
        os.unlink(path)  # ensure it doesn't exist
        det = AnomalyDetector(model_path=path)
        score = det.score({"temperature": 85.2, "pressure": 900.0})
        self.assertEqual(score, 0.0)

    def test_fit_and_score(self):
        from anomaly.detector import AnomalyDetector
        with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as f:
            path = f.name
        os.unlink(path)
        det = AnomalyDetector(model_path=path)

        records = [
            {"temperature": 85.0 + i * 0.1, "pressure": 900.0 + i, "rf_power": 150.0, "flow_rate": 40.0}
            for i in range(20)
        ]
        det.fit(records)
        self.assertTrue(det.is_fitted)

        score = det.score({"temperature": 85.5, "pressure": 901.0, "rf_power": 150.0, "flow_rate": 40.0})
        self.assertIsInstance(score, float)
        self.assertGreaterEqual(score, -1.0)
        self.assertLessEqual(score, 1.0)

        if os.path.exists(path):
            os.unlink(path)


# ---------------------------------------------------------------------------
# Integration test — full batch pipeline with mocked LLM
# ---------------------------------------------------------------------------

class TestBatchPipelineIntegration(unittest.TestCase):
    """
    Runs the full batch pipeline on the synthetic CSV file.
    LLM calls are mocked to return a plausible mapping so the test
    doesn't require an API key.
    """

    def _make_mock_pipeline(self, db_path: str, registry_path: str) -> object:
        """Build a Pipeline with SQLite pointed at temp files."""
        import importlib
        import config as cfg

        # Patch DB paths
        cfg.DB_PATH = db_path
        cfg.REGISTRY_DB_PATH = registry_path
        cfg.FIELD_MAPPINGS_PATH = db_path + "_fields.json"
        cfg.ANOMALY_MODEL_PATH  = db_path + "_anomaly.joblib"
        cfg.CANDIDATE_POOL_DIR  = db_path + "_pool"

        # Re-import modules that cached the old path
        import database.connection as dbc
        import database.models as dbm
        import caching.registry as reg
        import anomaly.detector as det

        # Reload to pick up new paths
        importlib.reload(dbc)
        importlib.reload(dbm)
        importlib.reload(reg)
        importlib.reload(det)

        from ingestion.batch import Pipeline
        return Pipeline()

    @patch("llm.client.LLMClient.batch_generate")
    def test_csv_batch_end_to_end(self, mock_llm):
        """Process test_csv.csv through the full pipeline."""
        # Mock LLM to return pre-built mappings
        def fake_llm(lines, sigs, pool):
            return [
                {
                    "signature": sig,
                    "fields": {
                        "timestamp":      r"^([^\,]+)",
                        "tool_id":        r"[^\,]+,([^\,]+)",
                        "alarm_code":     r"[^\,]*,[^\,]*,([^\,]*)",
                    },
                    "confidence": 0.85,
                    "parse_flags": [],
                }
                for sig in sigs
            ]
        mock_llm.side_effect = fake_llm

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path  = os.path.join(tmpdir, "test_out.db")
            reg_path = os.path.join(tmpdir, "test_reg.db")

            import config as cfg
            cfg.DB_PATH         = db_path
            cfg.REGISTRY_DB_PATH = reg_path
            cfg.FIELD_MAPPINGS_PATH = os.path.join(tmpdir, "fields.json")
            cfg.ANOMALY_MODEL_PATH  = os.path.join(tmpdir, "anomaly.joblib")
            cfg.CANDIDATE_POOL_DIR  = os.path.join(tmpdir, "pool")

            from database.models import init_db
            from database.connection import close_connection
            init_db(db_path)

            from ingestion.batch import Pipeline, process_batch
            pipeline = Pipeline()

            csv_path = os.path.join(LOG_DIR, "logs.csv")
            result = process_batch(csv_path, pipeline, source="TEST_CSV")

            self.assertGreater(result.total, 0, "Should have parsed records")
            self.assertGreater(result.success + result.cache_hits, 0, "Should have inserted records")

            # Close database connections to allow temp directory cleanup
            close_connection(db_path)
            close_connection(reg_path)
            pipeline.registry.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
