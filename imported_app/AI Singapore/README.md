# Semiconductor Log Processing Pipeline

An end-to-end log processing system for semiconductor manufacturing equipment (CVD, ETCH, PVD tools). Raw equipment logs in any format are cleaned, structured, mapped to a canonical semiconductor schema, and stored in a queryable SQLite database with anomaly detection.

---

## Architecture

Two-layer pipeline:
- **Format Parsing Layer** (`format_parsing/`) — format detection & parsing (11 formats, stdlib only)
- **Semantic Pipeline** (`ingestion/`, `llm/`, `normalisation/`, etc.) — AI-assisted field mapping, unit conversion, anomaly scoring, SQLite storage

```
Raw Equipment Log (any format)
         │
         ▼
  ┌─────────────────────────────────────────────────────────┐
  │  Format Parsing Layer  (format_parsing/)                │
  │                                                         │
  │  Format Detection → Format-Specific Parser →            │
  │  Encoding Normalisation → Corruption Detection →        │
  │  Deduplication                                          │
  └───────────────────────┬─────────────────────────────────┘
                          │  clean LogRecord objects
                          ▼
  ┌─────────────────────────────────────────────────────────┐
  │  Semantic Pipeline  (ingestion/, llm/, normalisation/, …)│
  │                                                         │
  │  Signature Generation →                                 │
  │  Registry Cache Lookup → LLM Field Mapping (on miss) →  │
  │  Unit Normalisation → Anomaly Detection →               │
  │  SQLite Database (output.db)                            │
  └─────────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Set your Anthropic API key
Open the `.env` file in the project root and paste your key:
```
OPENAI_API_KEY=sk-ant-your-key-here
```
Get your key at [https://platform.openai.com/api-keys].

### 3. Run the full pipeline
```bash
py main.py batch --input logs/logs.csv
py main.py batch --input logs/logs.csv --source ETCH01
py main.py batch --input logs/              # processes all files in the directory
```

Expected output:
```
Processing: logs/logs.csv
Done.
  Records processed : 120
  Inserted to DB    : 118
  Failed / corrupted: 2
  Cache hits        : 85
  LLM calls         : 3

  Open in SQLiteStudio: C:\...\output.db
  Tables: parsed_logs | parse_failures | trie_templates | field_mappings | llm_failures
```

---

## Entry Points

### `main.py` — CLI (all commands)
```bash
# Process a log file or entire directory (primary use)
py main.py batch --input logs/logs.csv
py main.py batch --input logs/logs.csv --source ETCH01
py main.py batch --input logs/

# Pre-populate caches from historical logs (run once before first use)
py main.py init --historical logs/

# Process staging area NDJSON through the semantic pipeline
py main.py from-staging --input staging_area/cleaned_logs.ndjson

# Start HTTP streaming server
py main.py stream --port 8080
# GET  /         endpoint listing
# POST /ingest   body: {"line": "...", "source": "TOOL01"}
# POST /flush    force-flush buffer to DB
# GET  /health   registry stats
```

### `format_parsing/app.py` — Web UI
Browser-based interface for the format parsing layer. Upload log files, inspect records, detect formats, and export cleaned data. No API key needed.
```bash
py format_parsing/app.py
# Visit: http://localhost:5000
```

### Run tests (no API key needed)
```bash
py -m pytest tests/test_pipeline.py -v -k "not Integration"
# 45 tests pass; LLM calls are mocked
```

### Format parsing demo (no API key needed)
```bash
py format_parsing/ETLdemo.py
```
Runs all 11 log formats through the format parsing layer and writes output to `staging_area/cleaned_logs.ndjson` and `staging_area/cleaned_logs.csv`.

---

## Project Structure

```
AI Singapore/
├── main.py                       ← CLI entry point (batch/init/from-staging/stream)
├── config.py                     ← canonical schema, unit maps, DB paths
├── requirements.txt              ← pip install -r requirements.txt
├── field_mappings.json           ← 90+ vendor→canonical field name mappings
├── anomaly_model.joblib          ← persisted Isolation Forest model
├── output.db                     ← OUTPUT DATABASE (open this in SQLiteStudio)
├── registry.db                   ← LLM mapping cache (by log signature)
│
├── logs/                         ← real log files (11 formats, one per format)
│   ├── logs.csv
│   ├── logs.jsonl
│   ├── logs.xml
│   ├── logs.yaml
│   ├── logs.tsv
│   ├── logs.syslog
│   ├── logs.logfmt
│   ├── logs.bin
│   ├── key_value.log
│   ├── delimiter.log
│   └── plain_text.log
│
├── staging_area/                 ← staging area export (cleaned_logs.ndjson / .csv)
│
├── format_parsing/               ← FORMAT PARSING LAYER (11 formats, stdlib only)
│   ├── app.py                    ← web UI for format parsing (Flask, port 5000)
│   ├── ingestion.py              ← LogIngestionService — main format parsing API
│   ├── detector.py               ← format auto-detection (extension + content + regex)
│   ├── base_processor.py         ← LogRecord dataclass + abstract base class
│   ├── normalizer.py             ← encoding → UTF-8, timestamps → ISO-8601, whitespace
│   ├── staging.py                ← StagingArea — dedup (SHA1) + NDJSON/CSV export
│   ├── ETLdemo.py                ← standalone demo: runs all 11 formats
│   └── processors/               ← one processor class per format
│       ├── json_proc.py          ← JSON / NDJSON
│       ├── csv_proc.py           ← CSV
│       ├── tsv_proc.py           ← TSV (tab-separated)
│       ├── xml_proc.py           ← XML
│       ├── yaml_proc.py          ← YAML
│       ├── syslog_proc.py        ← RFC 3164 & RFC 5424 syslog
│       ├── keyvalue.py           ← key=value pairs
│       ├── logfmt_proc.py        ← logfmt (key="value" style)
│       ├── delimiter.py          ← custom delimiters (|, ;, etc.)
│       ├── plaintext.py          ← freeform text
│       └── binary_proc.py        ← binary/hex detection
│
├── ingestion/                    ← SEMANTIC PIPELINE ADAPTERS
│   ├── batch.py                  ← process_batch() — full pipeline entry point
│   └── stream.py                 ← StreamProcessor — HTTP streaming server
│
├── parsing/                      ← SEMANTIC PARSERS (used internally by batch.py)
│   ├── format_router.py          ← format detection + parser dispatch
│   ├── structured_parser.py      ← JSON, XML, CSV, TSV, YAML, logfmt → dicts
│   ├── unstructured_parser.py    ← key-value, delimiter, plaintext → dicts
│   ├── syslog_parser.py          ← RFC 3164 & 5424
│   └── regex_engine.py           ← apply LLM-generated regex patterns
│
├── signature/
│   └── generator.py              ← mask <NUM>/<IP>/<UUID>/etc → stable 16-char hash
│
├── caching/
│   ├── registry.py               ← SQLite cache: signature → regex patterns
│   ├── hash_table.py             ← in-memory: vendor field → canonical field
│   └── trie.py                   ← prefix trie with <*> wildcards
│
├── llm/
│   ├── client.py                 ← OpenAI API wrapper (batched, 5-retry)
│   ├── prompt_builder.py         ← builds ICL prompts with candidate examples
│   └── response_parser.py        ← parses LLM JSON response
│
├── normalisation/
│   ├── unit_normaliser.py        ← unit conversion (°F→°C, Torr→Pa, slm→sccm, etc.)
│   └── cleaner.py                ← type validation, dedup key (SHA256)
│
├── clustering/
│   ├── vectoriser.py             ← TF-IDF vectorizer
│   ├── dbscan.py                 ← DBSCAN clustering (cosine distance)
│   └── candidate_pool.py         ← kNN pool for LLM in-context examples
│
├── partitioning/
│   └── iterative_partitioner.py  ← offline template discovery → populates trie
│
├── anomaly/
│   └── detector.py               ← Isolation Forest scorer (temp, pressure, rf, flow)
│
├── database/
│   ├── connection.py             ← SQLite WAL-mode, thread-local connections
│   ├── models.py                 ← CREATE TABLE statements for all 5 tables
│   └── writer.py                 ← bulk_insert, insert_failure, update_anomaly_scores
│
└── tests/
    └── test_pipeline.py          ← 45 unit tests (LLM mocked, no API key needed)
```

---

## Pipeline Flow

```
Raw log file (any of 11 formats)
        │
        ▼
Format detected (extension 40% + content 35% + regex 25%)
        │
        ▼
Format-specific parser extracts fields
  (JSON, XML, CSV, TSV, YAML, syslog, key=value, logfmt, delimiter, plaintext, binary)
        │
        ▼
Encoding → UTF-8, timestamps → ISO-8601 UTC, whitespace cleaned
        │
        ▼
Deduplication (SHA1), written to StagingArea
        │
        ▼
Signature generated (mask numbers/IPs/paths/UUIDs → stable 16-char hash)
        │
        ▼
Registry lookup (SQLite cache: signature → regex patterns)
  ├─ HIT: apply cached regex immediately
  └─ MISS: batch up to 20 lines → send to Claude (claude-opus-4-6)
        │
        ▼
Hash table lookup (90+ vendor field name → canonical field name)
        │
        ▼
Regex patterns applied → field values extracted
        │
        ▼
Unit conversion (°F→°C, Torr→Pa, slm→sccm, dBm→W, etc.)
        │
        ▼
Type validation + dedup key (SHA256)
        │
        ▼
Bulk insert to parsed_logs (or parse_failures on error)
        │
        ▼
Isolation Forest anomaly scoring on [temperature, pressure, rf_power, flow_rate]
        │
        ▼
anomaly_score written back to parsed_logs
        │
        ▼
output.db — open in SQLiteStudio
```

---

## Results in SQLiteStudio

1. Open **SQLiteStudio**
2. Go to **Database → Add a database**
3. Navigate to `output.db` in this folder and click OK
4. Expand the database in the left panel → expand **Tables**
5. Click any table → click the **Data** tab on the right

### Useful queries (Tools → Open SQL Editor)

```sql
-- All parsed records
SELECT * FROM parsed_logs LIMIT 100;

-- Anomalous records (score < -0.5 = abnormal)
SELECT timestamp, tool_id, temperature, pressure, rf_power, anomaly_score
FROM parsed_logs WHERE anomaly_score < -0.5
ORDER BY anomaly_score ASC;

-- Alarm events
SELECT timestamp, tool_id, alarm_code, alarm_severity, event_description
FROM parsed_logs WHERE alarm_code IS NOT NULL
ORDER BY timestamp DESC;

-- Records by tool
SELECT tool_id, COUNT(*) as count FROM parsed_logs GROUP BY tool_id;

-- Parse failures (corrupted or unreadable records)
SELECT * FROM parse_failures;

-- Low-confidence mappings worth reviewing
SELECT tool_id, raw_line, mapping_confidence, parse_flags
FROM parsed_logs WHERE mapping_confidence < 0.7
ORDER BY mapping_confidence ASC;

-- Learned field name mappings (vendor → canonical)
SELECT * FROM field_mappings ORDER BY created_at DESC;

-- LLM permanent failures (need manual attention)
SELECT signature, raw_line, error, attempts FROM llm_failures;
```

For the mapping cache, open **`registry.db`**:
```sql
SELECT signature, format_type, confidence, hit_count
FROM mapping_registry ORDER BY hit_count DESC;
```

---

## Database Tables (`output.db`)

### `parsed_logs` — successfully processed records

| Column | Type | Description |
|--------|------|-------------|
| record_id | TEXT | UUID primary key |
| timestamp | TEXT | ISO-8601 UTC |
| tool_id | TEXT | equipment identifier |
| log_source | TEXT | source filename |
| log_type | TEXT | alarm / sensor / process / event / unknown |
| raw_line | TEXT | original log line (audit trail) |
| recipe_name | TEXT | process recipe |
| process_step | TEXT | step within recipe |
| wafer_id | TEXT | wafer identifier |
| lot_id | TEXT | lot identifier |
| chamber_id | TEXT | chamber identifier |
| temperature | REAL | degrees Celsius |
| pressure | REAL | Pascals |
| rf_power | REAL | Watts |
| flow_rate | REAL | sccm |
| voltage | REAL | Volts |
| current | REAL | Amperes |
| alarm_code | TEXT | alarm/error code |
| alarm_severity | TEXT | critical / warning / info |
| event_description | TEXT | human-readable description |
| status | TEXT | process status |
| extra_fields | TEXT | JSON: vendor fields not in canonical schema |
| mapping_confidence | REAL | 0–1 confidence from LLM |
| parse_flags | TEXT | JSON array of warnings |
| anomaly_score | REAL | -1 (anomalous) to +1 (normal) |
| dedup_key | TEXT | SHA256 for deduplication |
| ingested_at | TEXT | when this record was inserted |

### `parse_failures` — records that could not be parsed

| Column | Description |
|--------|-------------|
| raw_line | original log line |
| log_source | source filename |
| error | reason for failure |
| failed_at | timestamp |

### `trie_templates` — learned log template patterns

| Column | Description |
|--------|-------------|
| signature | 16-char template hash |
| template | token string with `<*>` wildcards |
| position_map | JSON: wildcard position → field name |
| hit_count | how many log lines matched this template |

### `field_mappings` — vendor field name → canonical field name

| Column | Description |
|--------|-------------|
| vendor_field | e.g. `temp`, `pres_pa`, `rf` |
| canonical_field | e.g. `temperature`, `pressure`, `rf_power` |
| auto_discovered | 1 if learned by LLM, 0 if pre-mapped |

### `llm_failures` — permanent LLM mapping failures

| Column | Description |
|--------|-------------|
| signature | log template signature |
| raw_line | original line |
| error | API error message |
| attempts | number of retries (max 5) |

---

## Canonical Schema (`parsed_logs`)

| Field | Type | Notes |
|-------|------|-------|
| `record_id` | UUID | Auto-generated |
| `timestamp` | ISO 8601 string | UTC |
| `tool_id` | string | Equipment identifier |
| `log_source` | string | Filename or stream name |
| `log_type` | string | `alarm` / `sensor` / `process` / `event` / `unknown` |
| `temperature` | float | °C |
| `pressure` | float | Pa |
| `rf_power` | float | Watts |
| `flow_rate` | float | sccm |
| `voltage` | float | V |
| `current` | float | A |
| `alarm_code` | string | Vendor alarm ID |
| `alarm_severity` | string | `critical` / `warning` / `info` |
| `wafer_id` | string | |
| `lot_id` | string | |
| `chamber_id` | string | |
| `recipe_name` | string | |
| `process_step` | string | |
| `event_description` | string | |
| `status` | string | |
| `extra_fields` | JSON dict | Vendor fields not in canonical schema |
| `mapping_confidence` | float 0–1 | LLM confidence |
| `parse_flags` | JSON array | Warnings during parsing |
| `anomaly_score` | float –1 to 1 | Isolation Forest (–1 = most anomalous) |
| `dedup_key` | SHA256 hex | |
| `ingested_at` | ISO 8601 string | UTC |

---

## Unit Conversions Applied Automatically

| Field | Input units accepted | Stored as |
|-------|---------------------|-----------|
| temperature | °C, °F, K | Celsius |
| pressure | Pa, kPa, MPa, mTorr, Torr, psi, bar, mbar, atm | Pascal |
| rf_power | W, kW, mW, dBm | Watts |
| flow_rate | sccm, slm, slpm, ccm | sccm |
| voltage | V, mV, kV | Volts |
| current | A, mA, µA | Amperes |

---

## Anomaly Score Guide

| Score range | Meaning |
|-------------|---------|
| > 0 | Normal operating range |
| -0.5 to 0 | Borderline — worth reviewing |
| < -0.5 | Flagged anomaly — investigate |
| -1.0 | Most anomalous |

Score is computed by Isolation Forest on temperature, pressure, rf_power, and flow_rate together. Returns 0.0 if fewer than 10 records are available to train the model.

---

## LLM Reliability — 5-Retry Loop


When the OpenAI API call fails, the pipeline retries automatically up to **5 times** with exponential backoff (1 s, 2 s, 4 s, 8 s).

- **If any attempt succeeds:** processing continues normally.
- **If all 5 fail:** lines are written to the `llm_failures` table for engineer review. The pipeline never crashes — it continues with zero-confidence placeholders.

Without the API key, the pipeline still runs — format parsing and any cached mappings work fine. Only cache-miss records that need new LLM mapping will fail, and those get logged to `llm_failures`.

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | Yes (for LLM) | Your OpenAI API key |
| `LLM_MODEL` | No | Override model (default: `claude-opus-4-6`) |
| `PIPELINE_DB_PATH` | No | Override output DB path (default: `output.db`) |
| `PIPELINE_REGISTRY_PATH` | No | Override registry DB path (default: `registry.db`) |
| `PIPELINE_FIELD_MAPPINGS` | No | Override field mappings path (default: `field_mappings.json`) |
| `PIPELINE_ANOMALY_MODEL` | No | Override anomaly model path (default: `anomaly_model.joblib`) |
