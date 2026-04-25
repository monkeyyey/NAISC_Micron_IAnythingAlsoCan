from __future__ import annotations

import json
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUTPUT_DB = ROOT / "uploaded_dbs" / "mixed_machinery_logs_5000.sqlite"
ROW_COUNT = 5000
SEED = 334455

TOOLS = ["ETCH01", "ETCH02", "ETCH03", "DEP01", "CMP02", "LITHO04", "IMPLANT02", "METRO01"]
CHAMBERS = ["A", "B", "C", "D", "LL1", "LL2"]
RECIPES = ["OXIDE_MAIN", "POLY_OPEN", "CONTACT_STRIP", "AL_PAD", "GATE_CLEAN", "RESIST_ASH"]
HOSTS = ["fab-hmi-01", "fab-hmi-02", "plc-etch-01", "mes-gateway-01", "hist-node-02"]
ALARMS = ["E334", "E201", "W145", "P509", "V221", "M778", "H042", "RF91"]
SEVERITIES = ["INFO", "NOTICE", "WARN", "ERROR", "CRITICAL"]
FORMAT_TYPES = [
    "plain_text",
    "key_value",
    "delimiter",
    "csv",
    "tsv",
    "json",
    "xml",
    "yaml",
    "binary",
    "syslog",
    "logfmt",
]
DELIMITERS = ["|", ";", ":"]


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def random_record(index: int, rng: random.Random) -> dict[str, object]:
    ts = datetime(2026, 4, 22, 0, 0, 0, tzinfo=timezone.utc) + timedelta(
        seconds=index * 19 + rng.randint(0, 11)
    )
    tool = rng.choice(TOOLS)
    chamber = rng.choice(CHAMBERS)
    alarm = rng.choice(ALARMS)
    severity = rng.choices(SEVERITIES, weights=[12, 8, 15, 10, 3], k=1)[0]
    temp_c = round(rng.uniform(48.0, 92.0), 1)
    pressure_mtorr = round(rng.uniform(4.0, 42.0), 2)
    rf_power_w = round(rng.uniform(180.0, 980.0), 1)
    lot_id = f"LOT{rng.randint(1200, 1899)}"
    wafer_id = f"W{rng.randint(1, 25):02d}"
    recipe = rng.choice(RECIPES)
    host = rng.choice(HOSTS)
    signature = f"SIG{index:05d}"
    format_type = FORMAT_TYPES[index % len(FORMAT_TYPES)]

    return {
        "event_ts": iso_utc(ts),
        "tool_id": tool,
        "chamber_id": chamber,
        "lot_id": lot_id,
        "wafer_id": wafer_id,
        "recipe": recipe,
        "alarm_code": alarm,
        "severity": severity,
        "temperature_c": temp_c,
        "pressure_mtorr": pressure_mtorr,
        "rf_power_w": rf_power_w,
        "host": host,
        "signature": signature,
        "format_type": format_type,
        "sequence_no": index + 1,
    }


def build_payload(record: dict[str, object], rng: random.Random) -> tuple[str, bytes | None]:
    ts = str(record["event_ts"])
    tool = str(record["tool_id"])
    chamber = str(record["chamber_id"])
    lot_id = str(record["lot_id"])
    wafer_id = str(record["wafer_id"])
    recipe = str(record["recipe"])
    alarm = str(record["alarm_code"])
    severity = str(record["severity"])
    host = str(record["host"])
    signature = str(record["signature"])
    temp_c = record["temperature_c"]
    pressure = record["pressure_mtorr"]
    power = record["rf_power_w"]
    fmt = str(record["format_type"])

    if fmt == "plain_text":
        return (
            f"{ts} Tool {tool} chamber {chamber} triggered {severity} alarm {alarm} on {lot_id} "
            f"{wafer_id} recipe {recipe} at {temp_c}C pressure {pressure}mT RF {power}W host {host}",
            None,
        )
    if fmt == "key_value":
        return (
            f"timestamp={ts} tool={tool} chamber={chamber} lot={lot_id} wafer={wafer_id} "
            f"recipe={recipe} alarm={alarm} severity={severity} temp={temp_c} pressure={pressure} rf_power={power}",
            None,
        )
    if fmt == "delimiter":
        delim = DELIMITERS[record["sequence_no"] % len(DELIMITERS)]
        return (
            delim.join(
                [ts, tool, chamber, lot_id, wafer_id, recipe, alarm, severity, str(temp_c), str(pressure), str(power)]
            ),
            None,
        )
    if fmt == "csv":
        return (
            f"{ts},{tool},{chamber},{lot_id},{wafer_id},{recipe},{alarm},{severity},{temp_c},{pressure},{power},{host}",
            None,
        )
    if fmt == "tsv":
        return (
            f"{ts}\t{tool}\t{chamber}\t{lot_id}\t{wafer_id}\t{recipe}\t{alarm}\t{severity}\t{temp_c}\t{pressure}\t{power}\t{host}",
            None,
        )
    if fmt == "json":
        return (
            json.dumps(
                {
                    "timestamp": ts,
                    "tool": tool,
                    "chamber": chamber,
                    "lot": lot_id,
                    "wafer": wafer_id,
                    "recipe": recipe,
                    "alarm": alarm,
                    "severity": severity,
                    "temp_c": temp_c,
                    "pressure_mtorr": pressure,
                    "rf_power_w": power,
                    "host": host,
                    "signature": signature,
                },
                separators=(",", ":"),
            ),
            None,
        )
    if fmt == "xml":
        return (
            f'<log timestamp="{ts}" tool="{tool}" chamber="{chamber}" lot="{lot_id}" wafer="{wafer_id}" '
            f'recipe="{recipe}" alarm="{alarm}" severity="{severity}" temp_c="{temp_c}" '
            f'pressure_mtorr="{pressure}" rf_power_w="{power}" host="{host}" signature="{signature}" />',
            None,
        )
    if fmt == "yaml":
        return (
            "\n".join(
                [
                    f"timestamp: {ts}",
                    f"tool: {tool}",
                    f"chamber: {chamber}",
                    f"lot: {lot_id}",
                    f"wafer: {wafer_id}",
                    f"recipe: {recipe}",
                    f"alarm: {alarm}",
                    f"severity: {severity}",
                    f"temp_c: {temp_c}",
                    f"pressure_mtorr: {pressure}",
                    f"rf_power_w: {power}",
                    f"host: {host}",
                    f"signature: {signature}",
                ]
            ),
            None,
        )
    if fmt == "binary":
        blob = (
            f"{tool}|{chamber}|{alarm}|{severity}|{temp_c}|{pressure}|{power}|{lot_id}|{wafer_id}|{recipe}|{signature}"
        ).encode("utf-8")
        blob = bytes(rng.randint(0, 255) for _ in range(4)) + blob + bytes(rng.randint(0, 255) for _ in range(4))
        return (blob.hex(), blob)
    if fmt == "syslog":
        return (
            f"<34>{ts} {host} {tool}[{rng.randint(1000, 9999)}]: chamber={chamber} lot={lot_id} wafer={wafer_id} "
            f"recipe={recipe} severity={severity} alarm={alarm} temp={temp_c}C pressure={pressure}mT rf={power}W",
            None,
        )
    return (
        f"timestamp={ts} tool={tool.lower()} chamber={chamber.lower()} lot={lot_id.lower()} wafer={wafer_id.lower()} "
        f"recipe={recipe.lower()} alarm={alarm} severity={severity.lower()} temp={temp_c} pressure={pressure} rf_power={power} host={host}",
        None,
    )


def main() -> None:
    rng = random.Random(SEED)
    OUTPUT_DB.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_DB.exists():
        OUTPUT_DB.unlink()

    conn = sqlite3.connect(OUTPUT_DB)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        """
        CREATE TABLE mixed_tool_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sequence_no INTEGER NOT NULL,
            event_ts TEXT NOT NULL,
            tool_id TEXT NOT NULL,
            chamber_id TEXT NOT NULL,
            lot_id TEXT NOT NULL,
            wafer_id TEXT NOT NULL,
            recipe TEXT NOT NULL,
            alarm_code TEXT NOT NULL,
            severity TEXT NOT NULL,
            temperature_c REAL NOT NULL,
            pressure_mtorr REAL NOT NULL,
            rf_power_w REAL NOT NULL,
            host TEXT NOT NULL,
            signature TEXT NOT NULL,
            format_type TEXT NOT NULL,
            raw_log TEXT NOT NULL,
            raw_payload BLOB,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX idx_mixed_tool_logs_ts ON mixed_tool_logs(event_ts)")
    conn.execute("CREATE INDEX idx_mixed_tool_logs_format ON mixed_tool_logs(format_type)")
    conn.execute("CREATE INDEX idx_mixed_tool_logs_tool ON mixed_tool_logs(tool_id)")
    conn.execute("CREATE INDEX idx_mixed_tool_logs_alarm ON mixed_tool_logs(alarm_code)")

    rows = []
    for index in range(ROW_COUNT):
        record = random_record(index, rng)
        raw_log, raw_payload = build_payload(record, rng)
        rows.append(
            (
                record["sequence_no"],
                record["event_ts"],
                record["tool_id"],
                record["chamber_id"],
                record["lot_id"],
                record["wafer_id"],
                record["recipe"],
                record["alarm_code"],
                record["severity"],
                record["temperature_c"],
                record["pressure_mtorr"],
                record["rf_power_w"],
                record["host"],
                record["signature"],
                record["format_type"],
                raw_log,
                raw_payload,
                record["event_ts"],
            )
        )

    conn.executemany(
        """
        INSERT INTO mixed_tool_logs (
            sequence_no, event_ts, tool_id, chamber_id, lot_id, wafer_id, recipe,
            alarm_code, severity, temperature_c, pressure_mtorr, rf_power_w,
            host, signature, format_type, raw_log, raw_payload, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()
    print(f"Created {OUTPUT_DB} with {ROW_COUNT} mixed log rows.")


if __name__ == "__main__":
    main()
