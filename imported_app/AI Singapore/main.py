"""
main.py — Pipeline CLI entry point.

Usage:
  python main.py batch        --input logs/logs.csv [--source ETCH01]
  python main.py batch        --input logs/
  python main.py init         --historical logs/
  python main.py from-staging --input staging_area/cleaned_logs.ndjson
  python main.py stream       --port 8080
"""

import argparse
import logging
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=False)
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")


def cmd_init(args) -> None:
    """
    Pre-populate the trie and candidate pool from a directory of historical logs.
    """
    from ingestion.batch import Pipeline
    from partitioning.iterative_partitioner import populate_trie_from_lines
    from clustering.candidate_pool import CandidatePool

    historical_dir = args.historical
    if not os.path.isdir(historical_dir):
        logger.error("Not a directory: %s", historical_dir)
        sys.exit(1)

    pipeline = Pipeline()
    all_lines: list[str] = []

    for fname in os.listdir(historical_dir):
        fpath = os.path.join(historical_dir, fname)
        if not os.path.isfile(fpath):
            continue
        try:
            with open(fpath, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
            all_lines.extend(lines)
            logger.info("Loaded %d lines from %s", len(lines), fname)
        except OSError as exc:
            logger.warning("Could not read %s: %s", fpath, exc)

    if not all_lines:
        logger.warning("No lines found in %s", historical_dir)
        return

    # Run iterative partitioning → populate trie
    n_templates = populate_trie_from_lines(all_lines, pipeline.trie)
    logger.info("init: %d templates discovered", n_templates)

    # Build candidate pool for ICL
    pool = CandidatePool()
    pool.build(all_lines)
    logger.info("init: candidate pool built with %d candidates", len(pool.candidates))


def cmd_batch(args) -> None:
    """Process a log file or directory of log files."""
    from ingestion.batch import Pipeline, process_batch

    inp = args.input
    if os.path.isdir(inp):
        files = sorted(
            os.path.join(inp, f)
            for f in os.listdir(inp)
            if os.path.isfile(os.path.join(inp, f))
        )
    elif os.path.isfile(inp):
        files = [inp]
    else:
        logger.error("Not a file or directory: %s", inp)
        sys.exit(1)

    if not files:
        print("No files found in:", inp)
        sys.exit(1)

    pipeline = Pipeline()
    total = success = failed = cache_hits = llm_calls = 0

    for f in files:
        print(f"Processing: {f}")
        r = process_batch(f, pipeline, source=args.source or os.path.basename(f))
        total      += r.total
        success    += r.success
        failed     += r.failed
        cache_hits += r.cache_hits
        llm_calls  += r.llm_calls

    root = os.path.dirname(os.path.abspath(__file__))
    db   = os.path.join(root, "output.db")
    print(f"\nDone.")
    print(f"  Records processed : {total}")
    print(f"  Inserted to DB    : {success}")
    print(f"  Failed / corrupted: {failed}")
    print(f"  Cache hits        : {cache_hits}")
    print(f"  LLM calls         : {llm_calls}")
    print(f"\n  Open in SQLiteStudio: {db}")
    print(f"  Tables: parsed_logs | parse_failures | trie_templates | field_mappings | llm_failures")


def cmd_from_staging(args) -> None:
    """Process staging area NDJSON through the semantic pipeline."""
    from ingestion.batch import Pipeline, process_from_phase1_staging

    if not os.path.isfile(args.input):
        logger.error("File not found: %s", args.input)
        sys.exit(1)

    pipeline = Pipeline()
    result = process_from_phase1_staging(
        ndjson_path=args.input,
        pipeline=pipeline,
        source=args.source or None,
    )

    print(f"\nFrom-staging result:")
    print(f"  Total (clean)   : {result.total}")
    print(f"  Inserted        : {result.success}")
    print(f"  Failed/corrupted: {result.failed}")
    print(f"  Cache hits      : {result.cache_hits}")
    print(f"  LLM calls made  : {result.llm_calls}")


def cmd_stream(args) -> None:
    """Start HTTP server that accepts log lines on POST /ingest."""
    try:
        from flask import Flask, request, jsonify, make_response
    except ImportError:
        logger.error("Flask not installed. Run: pip install flask")
        sys.exit(1)

    from ingestion.batch import Pipeline
    from ingestion.stream import StreamProcessor

    pipeline  = Pipeline()
    processor = StreamProcessor(pipeline)

    app = Flask("pipeline-stream")

    @app.route("/", methods=["GET"])
    def root():
        html = """<!DOCTYPE html>
<html>
<head>
    <title>Semiconductor Log Pipeline — Stream Server</title>
    <style>
        body { font-family: monospace; max-width: 700px; margin: 40px auto; padding: 0 20px; }
        h1 { font-size: 1.2em; }
        table { border-collapse: collapse; width: 100%; margin: 12px 0; }
        td, th { border: 1px solid #ccc; padding: 6px 10px; text-align: left; }
        th { background: #f0f0f0; }
        pre { background: #f6f6f6; padding: 12px; overflow-x: auto; }
        .ok { color: green; } .warn { color: orange; }
    </style>
</head>
<body>
    <h1>Semiconductor Log Pipeline — Stream Server</h1>
    <p>Status: <span class="ok">OPENAI_API_KEY status not shown here</span></p>

    <h2>Endpoints</h2>
    <table>
        <tr><th>Method</th><th>Path</th><th>Description</th></tr>
        <tr><td>GET</td><td>/</td><td>This page</td></tr>
        <tr><td>POST</td><td>/ingest</td><td>Submit a single log line for processing</td></tr>
        <tr><td>POST</td><td>/flush</td><td>Force-flush buffer to the database</td></tr>
        <tr><td>GET</td><td>/health</td><td>Registry stats and service status</td></tr>
    </table>

    <h2>Example — POST /ingest</h2>
    <pre>curl -X POST http://localhost:8080/ingest \
       -H "Content-Type: application/json" \
       -d '{"line": "timestamp=2024-01-01 tool=ETCH01 temp=350.2", "source": "ETCH01"}'</pre>
</body>
</html>"""
        return make_response(html, 200, {"Content-Type": "text/html"})

    @app.route("/ingest", methods=["POST"])
    def ingest():
        data = request.get_json(force=True, silent=True)
        if not data or "line" not in data:
            return jsonify({"error": "Missing 'line' field"}), 400

        line   = data["line"]
        source = data.get("source", "stream")
        result = processor.ingest(line, source)
        return jsonify(result), 200

    @app.route("/flush", methods=["POST"])
    def flush():
        inserted = processor.flush()
        return jsonify({"inserted": inserted}), 200

    @app.route("/health", methods=["GET"])
    def health():
        reg_stats = pipeline.registry.stats()
        return jsonify({"status": "ok", "registry": reg_stats}), 200

    # Start Flask server
    host = args.host or "0.0.0.0"
    port = int(getattr(args, "port", 8080))
    logger.info("Starting stream server on %s:%d", host, port)
    app.run(host=host, port=port)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="main.py")
    sub = parser.add_subparsers(dest="command", required=True)

    # init
    p_init = sub.add_parser("init", help="Initialise trie and candidate pool")
    p_init.add_argument("--historical", required=True, help="Directory of historical logs")

    # batch
    p_batch = sub.add_parser("batch", help="Process files or directory in batch mode")
    p_batch.add_argument("--input", required=True, help="Input file or directory")
    p_batch.add_argument("--source", help="Override source identifier for all records")

    # from-staging
    p_from = sub.add_parser("from-staging", help="Process NDJSON staging area")
    p_from.add_argument("--input", required=True, help="NDJSON input file")
    p_from.add_argument("--source", help="Override source identifier for all records")

    # stream
    p_stream = sub.add_parser("stream", help="Start HTTP streaming server")
    p_stream.add_argument("--port", type=int, default=8080, help="Port to listen on")
    p_stream.add_argument("--host", default="0.0.0.0", help="Host to bind")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Ensure project root is on sys.path and set as working directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    os.chdir(script_dir)

    if args.command == "init":
        cmd_init(args)
    elif args.command == "batch":
        cmd_batch(args)
    elif args.command == "from-staging":
        cmd_from_staging(args)
    elif args.command == "stream":
        cmd_stream(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
