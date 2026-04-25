"""
app.py — Flask web application for the Log Data Preparation Pipeline.

Setup:
    pip install flask
    python app.py

Visit:  http://localhost:5000
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

_HERE   = Path(__file__).parent          # …/log_pipeline_updated/
_PARENT = _HERE.parent                   # …/log_pipeline_updated-…/
_PKG    = _HERE.name                     # "log_pipeline_updated"

# Add the parent so we can import the package by its folder name
sys.path.insert(0, str(_PARENT))

try:
    from flask import Flask, jsonify, request, send_file, render_template
    from werkzeug.utils import secure_filename
except ImportError:
    print("Flask not installed. Run: pip install flask")
    sys.exit(1)

_pkg = __import__(_PKG)
LogIngestionService = _pkg.ingestion.LogIngestionService
StagingArea         = _pkg.staging.StagingArea
LogFormatDetector   = _pkg.detector.LogFormatDetector
LogRecord           = _pkg.base_processor.LogRecord

# ---------------------------------------------------------------------------
# App + directories
# ---------------------------------------------------------------------------

app = Flask(__name__, template_folder=str(_HERE / "templates"))
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

LOGS_DIR    = _HERE / "logs"
STAGING_DIR = _HERE / "staging_area"
LOGS_DIR.mkdir(exist_ok=True)
STAGING_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Global pipeline state
# ---------------------------------------------------------------------------

_staging: StagingArea         = StagingArea(deduplicate=True)
_service: LogIngestionService = LogIngestionService(staging=_staging)
_detector: LogFormatDetector  = LogFormatDetector()
_history: list[dict]          = []


def _reset() -> None:
    global _staging, _service, _history
    _staging = StagingArea(deduplicate=True)
    _service = LogIngestionService(staging=_staging)
    _history = []
    # Remove exported files so staging_area/ reflects the cleared state
    for fname in ("cleaned_logs.ndjson", "cleaned_logs.csv"):
        p = STAGING_DIR / fname
        if p.exists():
            p.unlink()


def _flush_staging() -> None:
    """Write current staging area contents to staging_area/ on disk."""
    try:
        _staging.to_ndjson(str(STAGING_DIR / "cleaned_logs.ndjson"), include_corrupted=False)
        _staging.to_csv(str(STAGING_DIR / "cleaned_logs.csv"), include_corrupted=False)
    except Exception:
        pass  # never block a process response due to export failure


def _rec_dict(r: LogRecord) -> dict:
    d = r.to_dict()
    d["raw"] = r.raw[:1000]
    return d


def _apply_filters(records: list[LogRecord], args) -> list[LogRecord]:
    search    = args.get("search", "").lower().strip()
    level     = args.get("level", "").upper().strip()
    fmt       = args.get("format", "").strip()
    corrupted = args.get("corrupted", "all").lower()
    out = []
    for r in records:
        if corrupted == "true"  and not r.corrupted: continue
        if corrupted == "false" and r.corrupted:     continue
        if level and r.level != level:               continue
        if fmt   and r.format != fmt:                continue
        if search and search not in f"{r.message} {r.raw} {r.source}".lower():
            continue
        out.append(r)
    return out

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

@app.route("/api/files")
def list_files():
    files = []
    for f in sorted(LOGS_DIR.iterdir()):
        if f.is_file():
            s = f.stat()
            files.append({"name": f.name, "size": s.st_size, "ext": f.suffix.lstrip(".")})
    return jsonify({"files": files})


@app.route("/api/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    file = request.files["file"]
    if not file.filename:
        return jsonify({"error": "Empty filename"}), 400
    filename = secure_filename(file.filename)
    dest = LOGS_DIR / filename
    file.save(str(dest))
    detected = _detector.detect_from_bytes(dest.read_bytes(), filename=filename)
    return jsonify({
        "success": True,
        "filename": filename,
        "size": dest.stat().st_size,
        "detected_format": detected,
    })


@app.route("/api/files/<filename>", methods=["DELETE"])
def delete_file(filename: str):
    path = LOGS_DIR / secure_filename(filename)
    if not path.exists():
        return jsonify({"error": "Not found"}), 404
    path.unlink()
    return jsonify({"success": True})

# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

@app.route("/api/process", methods=["POST"])
def process_file():
    data = request.get_json() or {}
    filename       = data.get("filename")
    fmt_override   = data.get("format_override") or None
    if not filename:
        return jsonify({"error": "filename required"}), 400
    path = LOGS_DIR / filename
    if not path.exists():
        return jsonify({"error": f"File not found: {filename}"}), 404
    try:
        from ingestion.batch import Pipeline, process_batch

        pipeline = Pipeline()

        r = process_batch(str(path), pipeline, source=filename)

        result = {
            "source": filename,
            "total_records": r.total,
            "clean_records": r.success,
            "corrupted_records": r.failed,
            "success_rate": round((r.success / r.total) * 100, 1) if r.total else 0,
            "cache_hits": r.cache_hits,
            "llm_calls": r.llm_calls,
        }
        
        _history.append(result)
        _flush_staging()
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/process/all", methods=["POST"])
def process_all():
    results, errors = [], []
    for f in sorted(LOGS_DIR.iterdir()):
        if not f.is_file():
            continue
        try:
            result = _service.ingest_file(f, source_label=f.name)
            r = {
                "source":            f.name,
                "detected_format":   result.detected_format,
                "total_records":     result.total_records,
                "clean_records":     result.clean_records,
                "corrupted_records": result.corrupted_records,
                "success_rate":      round(result.success_rate * 100, 1),
                "duration_ms":       round(result.duration_seconds * 1000, 1),
            }
            results.append(r)
            _history.append(r)
        except Exception as exc:
            errors.append({"filename": f.name, "error": str(exc)})
    _flush_staging()
    return jsonify({"results": results, "errors": errors})

# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

@app.route("/api/records")
def get_records():
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(200, max(10, int(request.args.get("per_page", 50))))

    all_recs = list(_staging.all_records(include_corrupted=True))
    filtered = _apply_filters(all_recs, request.args)

    sort_by  = request.args.get("sort_by", "line_number")
    sort_dir = request.args.get("sort_dir", "asc")
    try:
        filtered.sort(
            key=lambda r: (getattr(r, sort_by) or ""),
            reverse=(sort_dir == "desc"),
        )
    except Exception:
        pass

    total     = len(filtered)
    start     = (page - 1) * per_page
    page_recs = filtered[start: start + per_page]

    return jsonify({
        "records":     [_rec_dict(r) for r in page_recs],
        "total":       total,
        "page":        page,
        "per_page":    per_page,
        "total_pages": max(1, (total + per_page - 1) // per_page),
    })

# ---------------------------------------------------------------------------
# Stats & detection
# ---------------------------------------------------------------------------

@app.route("/api/stats")
def get_stats():
    return jsonify(_staging.stats())


@app.route("/api/detect", methods=["POST"])
def detect_format():
    if "file" in request.files:
        f        = request.files["file"]
        data     = f.read()
        filename = f.filename or ""
    else:
        body     = request.get_json() or {}
        data     = body.get("text", "").encode("utf-8")
        filename = body.get("filename", "")
    detected = _detector.detect_from_bytes(data, filename=filename)
    return jsonify({"detected_format": detected})

# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

@app.route("/api/export/ndjson")
def export_ndjson():
    inc = request.args.get("include_corrupted", "false").lower() == "true"
    content = _staging.to_ndjson(include_corrupted=inc)
    buf = io.BytesIO(content.encode("utf-8"))
    return send_file(buf, mimetype="application/x-ndjson", as_attachment=True,
                     download_name="cleaned_logs.ndjson")


@app.route("/api/export/csv")
def export_csv():
    inc = request.args.get("include_corrupted", "false").lower() == "true"
    content = _staging.to_csv(include_corrupted=inc) or ""
    buf = io.BytesIO(content.encode("utf-8"))
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name="cleaned_logs.csv")

# ---------------------------------------------------------------------------
# Staging management
# ---------------------------------------------------------------------------

@app.route("/api/staging", methods=["DELETE"])
def clear_staging():
    _reset()
    return jsonify({"success": True})


@app.route("/api/formats")
def get_formats():
    return jsonify({"formats": list(_service.supported_formats)})


@app.route("/api/history")
def get_history():
    return jsonify({"history": _history})

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"\n  Log Pipeline Web App")
    print(f"  Logs dir  : {LOGS_DIR}")
    print(f"  Staging   : {STAGING_DIR}")
    print(f"  Visit     : http://localhost:5000\n")
    app.run(debug=True, port=5000)
