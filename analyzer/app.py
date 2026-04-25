from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

ANALYZER_DIR = Path(__file__).resolve().parent
ROOT_DIR = ANALYZER_DIR.parent
DEFAULT_DB_PATH = ROOT_DIR / "phase2_registry.db"
UPLOAD_DIR = ROOT_DIR / "uploaded_dbs"
MAX_LIMIT = 1000
ALLOWED_UPLOAD_SUFFIXES = {".db", ".sqlite", ".sqlite3", ".db3"}
DEFAULT_DB_ID = "default"

app = Flask(
    __name__,
    template_folder=str(ANALYZER_DIR / "templates"),
    static_folder=str(ANALYZER_DIR / "static"),
)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def database_path() -> Path:
    return Path(os.environ.get("SQLITE_DB_PATH", DEFAULT_DB_PATH)).expanduser()


def uploaded_database_files() -> list[Path]:
    return [path for path in sorted(UPLOAD_DIR.iterdir()) if path.is_file()]


def database_entries() -> list[dict[str, Any]]:
    entries = [
        {
            "id": DEFAULT_DB_ID,
            "name": database_path().name,
            "path": str(database_path()),
            "uploaded": False,
            "deletable": False,
        }
    ]
    for path in uploaded_database_files():
        entries.append(
            {
                "id": path.name,
                "name": path.name,
                "path": str(path),
                "uploaded": True,
                "deletable": True,
            }
        )
    return entries


def resolve_database(db_id: str | None = None) -> dict[str, Any]:
    selected_id = (db_id or request.args.get("db") or DEFAULT_DB_ID).strip() or DEFAULT_DB_ID
    for entry in database_entries():
        if entry["id"] == selected_id:
            db_path = Path(entry["path"])
            if not db_path.exists():
                raise FileNotFoundError(f"SQLite database not found: {db_path}")
            return entry
    raise ValueError(f"Unknown database: {selected_id}")


def connect(db_id: str | None = None) -> sqlite3.Connection:
    db_entry = resolve_database(db_id)
    db_path = Path(db_entry["path"])
    uri = f"file:{db_path.as_posix()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def json_safe_value(value: Any) -> Any:
    if isinstance(value, bytes):
        preview = value.hex()
        if len(preview) > 96:
            preview = f"{preview[:96]}..."
        return f"0x{preview}"
    return value


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [{key: json_safe_value(value) for key, value in dict(row).items()} for row in rows]


def table_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [row["name"] for row in rows]


def require_table(conn: sqlite3.Connection, table: str) -> None:
    if table not in table_names(conn):
        raise ValueError(f"Unknown table: {table}")


def table_columns(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    require_table(conn, table)
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return rows_to_dicts(rows)


def require_column(conn: sqlite3.Connection, table: str, column: str) -> None:
    valid_columns = {col["name"] for col in table_columns(conn, table)}
    if column not in valid_columns:
        raise ValueError(f"Unknown column for {table}: {column}")


def quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def numeric_columns(columns: list[dict[str, Any]]) -> list[str]:
    numeric_markers = ("INT", "REAL", "NUM", "DEC", "FLOAT", "DOUBLE")
    return [
        col["name"]
        for col in columns
        if any(marker in (col["type"] or "").upper() for marker in numeric_markers)
    ]


def parse_numeric(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_pattern_fields(value: Any) -> list[str]:
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, dict):
        return [str(key) for key in parsed.keys()]
    return []


def iso_like_text(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def analyzer_payload(conn: sqlite3.Connection, table: str) -> dict[str, Any]:
    columns = table_columns(conn, table)
    column_names = [column["name"] for column in columns]
    numeric = set(numeric_columns(columns))
    row_count = conn.execute(f"SELECT COUNT(*) AS count FROM {quoted(table)}").fetchone()["count"]
    rows = rows_to_dicts(conn.execute(f"SELECT * FROM {quoted(table)} LIMIT 5000").fetchall())

    confidence_col = "confidence" if "confidence" in column_names else next((name for name in numeric if "confidence" in name.lower()), None)
    hit_col = "hit_count" if "hit_count" in column_names else next((name for name in numeric if "hit" in name.lower() or "count" in name.lower()), None)
    format_col = "format_type" if "format_type" in column_names else next((name for name in column_names if "format" in name.lower() or "type" in name.lower()), None)
    updated_col = "updated_at" if "updated_at" in column_names else next((name for name in column_names if "updated" in name.lower() or "modified" in name.lower()), None)
    created_col = "created_at" if "created_at" in column_names else next((name for name in column_names if "created" in name.lower()), None)
    signature_col = "signature" if "signature" in column_names else next((name for name in column_names if "signature" in name.lower() or name.lower() == "id"), None)
    patterns_col = "regex_patterns" if "regex_patterns" in column_names else next((name for name in column_names if "pattern" in name.lower() or "regex" in name.lower()), None)

    confidence_values = [value for row in rows if (value := parse_numeric(row.get(confidence_col))) is not None] if confidence_col else []
    hit_values = [value for row in rows if (value := parse_numeric(row.get(hit_col))) is not None] if hit_col else []

    avg_confidence = sum(confidence_values) / len(confidence_values) if confidence_values else None
    total_hits = int(sum(hit_values)) if hit_values else None

    confidence_bands = {"high": 0, "medium": 0, "low": 0}
    for value in confidence_values:
        if value >= 0.9:
            confidence_bands["high"] += 1
        elif value >= 0.75:
            confidence_bands["medium"] += 1
        else:
            confidence_bands["low"] += 1

    format_counts: dict[str, int] = {}
    if format_col:
        for row in rows:
            label = str(row.get(format_col) or "unknown")
            format_counts[label] = format_counts.get(label, 0) + 1

    signal_counts: dict[str, int] = {}
    if patterns_col:
        for row in rows:
            for field in parse_pattern_fields(row.get(patterns_col)):
                signal_counts[field] = signal_counts.get(field, 0) + 1

    tracked_signals = ["timestamp", "tool_id", "alarm_code", "event_code", "lot_id", "recipe", "chamber", "wafer_id"]
    coverage = []
    for signal in tracked_signals:
        count = signal_counts.get(signal, 0)
        pct = (count / row_count * 100) if row_count else 0
        coverage.append({"signal": signal, "count": count, "coverage_pct": round(pct, 1)})

    recent_activity = []
    if updated_col or created_col:
        sort_key = updated_col or created_col
        sortable_rows = [row for row in rows if iso_like_text(row.get(sort_key))]
        sortable_rows.sort(key=lambda row: iso_like_text(row.get(sort_key)) or "", reverse=True)
        for row in sortable_rows[:8]:
            recent_activity.append(
                {
                    "signature": str(row.get(signature_col) or "unknown"),
                    "format": str(row.get(format_col) or "unknown"),
                    "confidence": parse_numeric(row.get(confidence_col)),
                    "hit_count": int(parse_numeric(row.get(hit_col)) or 0),
                    "updated_at": iso_like_text(row.get(updated_col) or row.get(created_col)),
                }
            )

    watchlist = []
    for row in rows:
        confidence = parse_numeric(row.get(confidence_col))
        hit_count = int(parse_numeric(row.get(hit_col)) or 0)
        reasons = []
        if confidence is not None and confidence < 0.75:
            reasons.append("low-confidence mapping")
        if confidence is not None and hit_count >= 10 and confidence < 0.9:
            reasons.append("high-volume signature needs tuning")
        if patterns_col and len(parse_pattern_fields(row.get(patterns_col))) <= 1:
            reasons.append("thin field extraction")
        if reasons:
            watchlist.append(
                {
                    "signature": str(row.get(signature_col) or "unknown"),
                    "format": str(row.get(format_col) or "unknown"),
                    "confidence": confidence,
                    "hit_count": hit_count,
                    "reason": ", ".join(reasons[:2]),
                }
            )
    watchlist.sort(key=lambda item: ((item["confidence"] if item["confidence"] is not None else 1.0), -item["hit_count"]))

    return {
        "row_count": row_count,
        "sampled_rows": len(rows),
        "avg_confidence": avg_confidence,
        "total_hits": total_hits,
        "signals_detected": sorted(signal_counts.items(), key=lambda item: (-item[1], item[0]))[:12],
        "signal_coverage": coverage,
        "formats": sorted([{"label": label, "count": count} for label, count in format_counts.items()], key=lambda item: (-item["count"], item["label"]))[:8],
        "confidence_bands": confidence_bands,
        "recent_activity": recent_activity,
        "watchlist": watchlist[:8],
        "latest_update": recent_activity[0]["updated_at"] if recent_activity else None,
    }


def sanitize_upload_name(filename: str) -> str:
    candidate = Path(filename or "database.sqlite").name.strip()
    safe = "".join(ch for ch in candidate if ch.isalnum() or ch in {"-", "_", "."})
    safe = safe.lstrip(".") or "database.sqlite"
    suffix = Path(safe).suffix.lower()
    if suffix not in ALLOWED_UPLOAD_SUFFIXES:
        raise ValueError("Upload a SQLite file with .db, .sqlite, .sqlite3, or .db3 extension")
    return safe


def unique_upload_path(filename: str) -> Path:
    candidate = sanitize_upload_name(filename)
    stem = Path(candidate).stem or "database"
    suffix = Path(candidate).suffix
    target = UPLOAD_DIR / candidate
    counter = 2
    while target.exists():
        target = UPLOAD_DIR / f"{stem}-{counter}{suffix}"
        counter += 1
    return target


def validate_sqlite_file(path: Path) -> None:
    try:
        conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro&immutable=1", uri=True)
        conn.execute("PRAGMA schema_version").fetchone()
        conn.close()
    except sqlite3.Error as exc:
        try:
            path.unlink()
        except OSError:
            pass
        raise ValueError(f"Uploaded file is not a readable SQLite database: {exc}") from exc


@app.errorhandler(FileNotFoundError)
@app.errorhandler(ValueError)
def handle_bad_request(error: Exception):
    return jsonify({"error": str(error)}), 400


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/databases")
def api_databases():
    current = resolve_database(request.args.get("db"))
    return jsonify({"databases": database_entries(), "current_database": current})


@app.route("/api/databases/upload", methods=["POST"])
def api_upload_database():
    upload = request.files.get("database")
    if upload is None or not upload.filename:
        raise ValueError("Choose a SQLite database file to upload")

    target = unique_upload_path(upload.filename)
    upload.save(target)
    validate_sqlite_file(target)

    current = next(entry for entry in database_entries() if entry["id"] == target.name)
    return jsonify({"message": f"Uploaded {target.name}", "databases": database_entries(), "current_database": current}), 201


@app.route("/api/databases/<db_id>", methods=["DELETE"])
def api_delete_database(db_id: str):
    entry = resolve_database(db_id)
    if not entry["deletable"]:
        raise ValueError("The default workspace database cannot be removed")

    path = Path(entry["path"])
    if path.exists():
        path.unlink()

    current = resolve_database(DEFAULT_DB_ID)
    return jsonify({"message": f"Removed {entry['name']}", "databases": database_entries(), "current_database": current})


@app.route("/api/tables")
def api_tables():
    db = resolve_database(request.args.get("db"))
    with connect(db["id"]) as conn:
        tables = []
        for name in table_names(conn):
            count = conn.execute(f"SELECT COUNT(*) AS count FROM {quoted(name)}").fetchone()
            tables.append({"name": name, "row_count": count["count"]})
        return jsonify({"database": db, "tables": tables})


@app.route("/api/schema/<table>")
def api_schema(table: str):
    db = resolve_database(request.args.get("db"))
    with connect(db["id"]) as conn:
        return jsonify({"database": db, "table": table, "columns": table_columns(conn, table)})


@app.route("/api/rows/<table>")
def api_rows(table: str):
    limit = min(max(int(request.args.get("limit", 100)), 1), MAX_LIMIT)
    search = request.args.get("search", "").strip()
    db = resolve_database(request.args.get("db"))

    with connect(db["id"]) as conn:
        columns = table_columns(conn, table)
        where = ""
        params: list[Any] = []

        if search:
            text_columns = [col["name"] for col in columns]
            where_parts = [f"CAST({quoted(col)} AS TEXT) LIKE ?" for col in text_columns]
            where = "WHERE " + " OR ".join(where_parts)
            params = [f"%{search}%"] * len(where_parts)

        total = conn.execute(f"SELECT COUNT(*) AS count FROM {quoted(table)} {where}", params).fetchone()
        rows = conn.execute(f"SELECT * FROM {quoted(table)} {where} LIMIT ?", [*params, limit]).fetchall()

    return jsonify({"database": db, "table": table, "columns": [col["name"] for col in columns], "rows": rows_to_dicts(rows), "total": total["count"], "limit": limit})


@app.route("/api/profile/<table>")
def api_profile(table: str):
    db = resolve_database(request.args.get("db"))
    with connect(db["id"]) as conn:
        columns = table_columns(conn, table)
        row_count = conn.execute(f"SELECT COUNT(*) AS count FROM {quoted(table)}").fetchone()["count"]
        profile = []

        for col in columns:
            name = col["name"]
            q_name = quoted(name)
            stats = conn.execute(
                f"""
                SELECT
                    COUNT({q_name}) AS non_null,
                    COUNT(DISTINCT {q_name}) AS distinct_count,
                    MIN({q_name}) AS min_value,
                    MAX({q_name}) AS max_value
                FROM {quoted(table)}
                """
            ).fetchone()
            profile.append(
                {
                    "name": name,
                    "type": col["type"],
                    "non_null": stats["non_null"],
                    "null_count": row_count - stats["non_null"],
                    "distinct_count": stats["distinct_count"],
                    "min_value": json_safe_value(stats["min_value"]),
                    "max_value": json_safe_value(stats["max_value"]),
                }
            )

    return jsonify({"database": db, "table": table, "row_count": row_count, "columns": profile})


@app.route("/api/chart/<table>")
def api_chart(table: str):
    dimension = request.args.get("dimension", "")
    metric = request.args.get("metric", "")
    aggregation = request.args.get("aggregation", "count").lower()
    chart_limit = min(max(int(request.args.get("limit", 25)), 1), 100)
    db = resolve_database(request.args.get("db"))

    with connect(db["id"]) as conn:
        require_column(conn, table, dimension)
        columns = table_columns(conn, table)
        numeric = set(numeric_columns(columns))

        if aggregation == "count":
            metric_expr = "COUNT(*)"
            label = "Count"
        else:
            if metric not in numeric:
                raise ValueError("Metric must be a numeric column for this aggregation")
            require_column(conn, table, metric)
            if aggregation not in {"sum", "avg", "min", "max"}:
                raise ValueError("Aggregation must be one of: count, sum, avg, min, max")
            metric_expr = f"{aggregation.upper()}({quoted(metric)})"
            label = f"{aggregation.upper()} {metric}"

        rows = conn.execute(
            f"""
            SELECT CAST({quoted(dimension)} AS TEXT) AS label, {metric_expr} AS value
            FROM {quoted(table)}
            GROUP BY {quoted(dimension)}
            ORDER BY value DESC
            LIMIT ?
            """,
            [chart_limit],
        ).fetchall()

    return jsonify({"database": db, "table": table, "dimension": dimension, "metric": metric, "aggregation": aggregation, "value_label": label, "data": rows_to_dicts(rows)})


@app.route("/api/analyzer/<table>")
def api_analyzer(table: str):
    db = resolve_database(request.args.get("db"))
    with connect(db["id"]) as conn:
        return jsonify({"database": db, "table": table, "analysis": analyzer_payload(conn, table)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(debug=True, port=port)
