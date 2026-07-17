import json
import subprocess
import sys
from pathlib import Path


def read_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def test_generator_creates_expected_sources(tmp_path):
    output = tmp_path / "raw_sample"
    command = [
        sys.executable,
        "data_generator/generate_raw_data.py",
        "--output",
        str(output),
        "--customers",
        "25",
        "--meters",
        "25",
        "--days",
        "2",
        "--readings-per-meter-per-day",
        "4",
        "--duplicate-rate",
        "0.10",
        "--invalid-rate",
        "0.10",
        "--outage-rate",
        "0.10",
        "--seed",
        "7",
    ]
    subprocess.run(command, check=True)

    assert (output / "gis_zones" / "zones.csv").exists()
    assert list((output / "meter_readings").glob("ingest_date=*/batch_000.jsonl"))
    assert list((output / "outage_events").glob("ingest_date=*/batch_000.jsonl"))
    assert list((output / "customer_cdc").glob("ingest_date=*/batch_000.jsonl"))
    assert list((output / "billing").glob("billing_month=*/batch_000.jsonl"))


def test_generator_includes_invalid_and_duplicate_meter_records(tmp_path):
    output = tmp_path / "raw_sample"
    command = [
        sys.executable,
        "data_generator/generate_raw_data.py",
        "--output",
        str(output),
        "--customers",
        "30",
        "--meters",
        "30",
        "--days",
        "1",
        "--readings-per-meter-per-day",
        "10",
        "--duplicate-rate",
        "0.20",
        "--invalid-rate",
        "0.20",
        "--seed",
        "11",
    ]
    subprocess.run(command, check=True)

    meter_file = next((output / "meter_readings").glob("ingest_date=*/batch_000.jsonl"))
    records = read_jsonl(meter_file)
    event_ids = [r["event_id"] for r in records]

    assert any(r.get("is_intentionally_invalid") is True for r in records)
    assert len(event_ids) > len(set(event_ids))


def test_customer_cdc_contains_expected_operations(tmp_path):
    output = tmp_path / "raw_sample"
    command = [
        sys.executable,
        "data_generator/generate_raw_data.py",
        "--output",
        str(output),
        "--customers",
        "100",
        "--meters",
        "100",
        "--days",
        "4",
        "--readings-per-meter-per-day",
        "1",
        "--seed",
        "21",
    ]
    subprocess.run(command, check=True)

    operations = set()
    for cdc_file in (output / "customer_cdc").glob("ingest_date=*/batch_000.jsonl"):
        operations.update(r["operation"] for r in read_jsonl(cdc_file))

    assert "insert" in operations
    assert "update" in operations
    assert "delete" in operations
