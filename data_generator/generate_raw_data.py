#!/usr/bin/env python3
"""Generate synthetic raw data for the HydroFlow Databricks Lakehouse project.

The output is intentionally shaped like a cloud object-storage landing zone so that
Databricks Auto Loader can ingest it later with cloudFiles.

No third-party libraries are required.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
from uuid import uuid4


@dataclass(frozen=True)
class GeneratorConfig:
    output: Path
    customers: int
    meters: int
    days: int
    readings_per_meter_per_day: int
    duplicate_rate: float
    invalid_rate: float
    outage_rate: float
    seed: int
    start_date: datetime


ZONE_NAMES = [
    "North Grid",
    "South Grid",
    "East Grid",
    "West Grid",
    "Central Grid",
    "Reservoir Edge",
    "Industrial Belt",
    "University Zone",
]

CUSTOMER_SEGMENTS = ["residential", "commercial", "industrial", "public_sector"]
METER_TYPES = ["water", "electric"]
STATUSES = ["active", "maintenance", "offline"]
CDC_OPS = ["insert", "update", "delete"]


def parse_args() -> GeneratorConfig:
    parser = argparse.ArgumentParser(description="Generate HydroFlow synthetic raw data")
    parser.add_argument("--output", type=Path, default=Path("data/raw"), help="Output root directory")
    parser.add_argument("--customers", type=int, default=1000, help="Number of synthetic customers")
    parser.add_argument("--meters", type=int, default=1000, help="Number of synthetic meters")
    parser.add_argument("--days", type=int, default=3, help="Number of event days to generate")
    parser.add_argument("--readings-per-meter-per-day", type=int, default=24, help="Readings per meter per day")
    parser.add_argument("--duplicate-rate", type=float, default=0.02, help="Fraction of meter readings duplicated")
    parser.add_argument("--invalid-rate", type=float, default=0.01, help="Fraction of meter readings made invalid")
    parser.add_argument("--outage-rate", type=float, default=0.003, help="Outage/leak event probability per meter per day")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-07-01",
        help="Start date in YYYY-MM-DD format",
    )
    args = parser.parse_args()

    if args.customers <= 0 or args.meters <= 0 or args.days <= 0 or args.readings_per_meter_per_day <= 0:
        raise ValueError("customers, meters, days, and readings-per-meter-per-day must be positive")
    if not 0 <= args.duplicate_rate <= 1:
        raise ValueError("duplicate-rate must be between 0 and 1")
    if not 0 <= args.invalid_rate <= 1:
        raise ValueError("invalid-rate must be between 0 and 1")
    if not 0 <= args.outage_rate <= 1:
        raise ValueError("outage-rate must be between 0 and 1")

    start_date = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)

    return GeneratorConfig(
        output=args.output,
        customers=args.customers,
        meters=args.meters,
        days=args.days,
        readings_per_meter_per_day=args.readings_per_meter_per_day,
        duplicate_rate=args.duplicate_rate,
        invalid_rate=args.invalid_rate,
        outage_rate=args.outage_rate,
        seed=args.seed,
        start_date=start_date,
    )


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, records: Iterable[Dict[str, Any]]) -> int:
    ensure_dir(path.parent)
    count = 0
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, separators=(",", ":")) + "\n")
            count += 1
    return count


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> int:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def build_zones(rng: random.Random) -> List[Dict[str, Any]]:
    zones = []
    for idx, name in enumerate(ZONE_NAMES, start=1):
        zones.append(
            {
                "zone_id": f"zone_{idx:03d}",
                "zone_name": name,
                "city": rng.choice(["Tempe", "Mesa", "Chandler", "Phoenix"]),
                "state": "AZ",
                "pressure_band": rng.choice(["low", "medium", "high"]),
                "service_priority": rng.choice(["standard", "critical", "industrial"]),
                "centroid_latitude": round(rng.uniform(33.25, 33.55), 6),
                "centroid_longitude": round(rng.uniform(-112.15, -111.75), 6),
            }
        )
    return zones


def build_customers(config: GeneratorConfig, zones: List[Dict[str, Any]], rng: random.Random) -> List[Dict[str, Any]]:
    customers = []
    for i in range(1, config.customers + 1):
        zone = rng.choice(zones)
        customers.append(
            {
                "customer_id": f"cust_{i:07d}",
                "customer_name": f"Synthetic Customer {i}",
                "segment": rng.choice(CUSTOMER_SEGMENTS),
                "service_zone_id": zone["zone_id"],
                "email": f"customer{i}@example.com",
                "phone": f"555-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
                "address_line1": f"{rng.randint(100,9999)} Synthetic Ave",
                "city": zone["city"],
                "state": "AZ",
                "postal_code": str(rng.randint(85001, 85299)),
                "account_status": rng.choice(["active", "active", "active", "delinquent"]),
                "created_at": iso(config.start_date - timedelta(days=rng.randint(30, 1000))),
            }
        )
    return customers


def build_meter_inventory(config: GeneratorConfig, customers: List[Dict[str, Any]], rng: random.Random) -> List[Dict[str, Any]]:
    meters = []
    for i in range(1, config.meters + 1):
        customer = customers[(i - 1) % len(customers)]
        meters.append(
            {
                "meter_id": f"meter_{i:07d}",
                "customer_id": customer["customer_id"],
                "service_zone_id": customer["service_zone_id"],
                "meter_type": rng.choice(METER_TYPES),
                "install_date": iso(config.start_date - timedelta(days=rng.randint(30, 1500))),
                "firmware_version": rng.choice(["1.2.0", "1.3.4", "2.0.1", "2.1.0"]),
            }
        )
    return meters


def make_meter_reading(
    meter: Dict[str, Any],
    event_ts: datetime,
    rng: random.Random,
    invalid: bool = False,
) -> Dict[str, Any]:
    base_usage = rng.uniform(3.0, 60.0) if meter["meter_type"] == "water" else rng.uniform(0.5, 12.0)
    pressure = rng.uniform(35.0, 90.0)
    status = rng.choice(STATUSES)

    record = {
        "event_id": str(uuid4()),
        "source_system": "ami_meter_gateway",
        "source_entity": "meter_readings",
        "event_timestamp": iso(event_ts),
        "meter_id": meter["meter_id"],
        "customer_id": meter["customer_id"],
        "service_zone_id": meter["service_zone_id"],
        "meter_type": meter["meter_type"],
        "usage_value": round(base_usage, 3),
        "usage_unit": "gallons" if meter["meter_type"] == "water" else "kwh",
        "pressure_psi": round(pressure, 2) if meter["meter_type"] == "water" else None,
        "battery_pct": rng.randint(5, 100),
        "firmware_version": meter["firmware_version"],
        "meter_status": status,
        "ingestion_hint": "synthetic_stream",
    }

    if invalid:
        invalid_case = rng.choice(["null_meter", "negative_usage", "bad_timestamp", "bad_status"])
        if invalid_case == "null_meter":
            record["meter_id"] = None
        elif invalid_case == "negative_usage":
            record["usage_value"] = round(-abs(base_usage), 3)
        elif invalid_case == "bad_timestamp":
            record["event_timestamp"] = "not-a-timestamp"
        elif invalid_case == "bad_status":
            record["meter_status"] = "unknown_status"
        record["is_intentionally_invalid"] = True
    else:
        record["is_intentionally_invalid"] = False

    return record


def generate_meter_readings(config: GeneratorConfig, meters: List[Dict[str, Any]], rng: random.Random) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for day_offset in range(config.days):
        event_day = config.start_date + timedelta(days=day_offset)
        ingest_date = event_day.date().isoformat()
        records: List[Dict[str, Any]] = []
        for meter in meters:
            for reading_idx in range(config.readings_per_meter_per_day):
                minutes_between = int(24 * 60 / config.readings_per_meter_per_day)
                event_ts = event_day + timedelta(minutes=reading_idx * minutes_between)
                event_ts += timedelta(seconds=rng.randint(0, 300))
                invalid = rng.random() < config.invalid_rate
                record = make_meter_reading(meter, event_ts, rng, invalid=invalid)
                records.append(record)
                if rng.random() < config.duplicate_rate:
                    duplicated = dict(record)
                    duplicated["duplicate_copy"] = True
                    records.append(duplicated)
        rng.shuffle(records)
        path = config.output / "meter_readings" / f"ingest_date={ingest_date}" / "batch_000.jsonl"
        counts[str(path)] = write_jsonl(path, records)
    return counts


def generate_outage_events(config: GeneratorConfig, meters: List[Dict[str, Any]], rng: random.Random) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for day_offset in range(config.days):
        event_day = config.start_date + timedelta(days=day_offset)
        ingest_date = event_day.date().isoformat()
        records: List[Dict[str, Any]] = []
        for meter in meters:
            if rng.random() < config.outage_rate:
                start_ts = event_day + timedelta(hours=rng.randint(0, 23), minutes=rng.randint(0, 59))
                duration_minutes = rng.randint(10, 240)
                records.append(
                    {
                        "event_id": str(uuid4()),
                        "source_system": "outage_management_system",
                        "source_entity": "outage_events",
                        "event_timestamp": iso(start_ts),
                        "outage_id": f"outage_{uuid4().hex[:12]}",
                        "meter_id": meter["meter_id"],
                        "customer_id": meter["customer_id"],
                        "service_zone_id": meter["service_zone_id"],
                        "event_type": rng.choice(["pressure_drop", "suspected_leak", "meter_offline", "service_interruption"]),
                        "severity": rng.choice(["low", "medium", "high", "critical"]),
                        "estimated_duration_minutes": duration_minutes,
                        "resolved_timestamp": iso(start_ts + timedelta(minutes=duration_minutes)),
                    }
                )
        path = config.output / "outage_events" / f"ingest_date={ingest_date}" / "batch_000.jsonl"
        counts[str(path)] = write_jsonl(path, records)
    return counts


def generate_customer_cdc(config: GeneratorConfig, customers: List[Dict[str, Any]], zones: List[Dict[str, Any]], rng: random.Random) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    sample_size = min(len(customers), max(10, int(len(customers) * 0.1)))
    for day_offset in range(config.days):
        event_day = config.start_date + timedelta(days=day_offset)
        ingest_date = event_day.date().isoformat()
        records: List[Dict[str, Any]] = []

        # Inserts on day 0 for a subset; updates/deletes on later days.
        selected_customers = rng.sample(customers, sample_size)
        for customer in selected_customers:
            if day_offset == 0:
                op = "insert"
            else:
                op = rng.choices(CDC_OPS, weights=[0.10, 0.75, 0.15], k=1)[0]

            changed = dict(customer)
            if op == "update":
                changed["service_zone_id"] = rng.choice(zones)["zone_id"]
                changed["account_status"] = rng.choice(["active", "delinquent", "closed"])
                changed["email"] = f"updated_{customer['customer_id']}@example.com"
            elif op == "delete":
                changed["account_status"] = "closed"

            records.append(
                {
                    "cdc_event_id": str(uuid4()),
                    "source_system": "customer_information_system",
                    "source_entity": "customer_cdc",
                    "operation": op,
                    "sequence_num": day_offset * 1_000_000 + rng.randint(1, 999_999),
                    "event_timestamp": iso(event_day + timedelta(hours=rng.randint(0, 23), minutes=rng.randint(0, 59))),
                    "customer": changed,
                }
            )
        records.sort(key=lambda x: x["sequence_num"])
        path = config.output / "customer_cdc" / f"ingest_date={ingest_date}" / "batch_000.jsonl"
        counts[str(path)] = write_jsonl(path, records)
    return counts


def generate_billing(config: GeneratorConfig, customers: List[Dict[str, Any]], rng: random.Random) -> Dict[str, int]:
    records: List[Dict[str, Any]] = []
    billing_month = config.start_date.replace(day=1)
    for customer in customers:
        usage_gallons = round(rng.uniform(1000, 25000), 2)
        bill_amount = round(15.0 + usage_gallons * rng.uniform(0.003, 0.009), 2)
        records.append(
            {
                "bill_id": f"bill_{uuid4().hex[:14]}",
                "source_system": "billing_platform",
                "source_entity": "billing",
                "customer_id": customer["customer_id"],
                "service_zone_id": customer["service_zone_id"],
                "billing_month": billing_month.date().isoformat(),
                "usage_gallons": usage_gallons,
                "bill_amount_usd": bill_amount,
                "payment_status": rng.choice(["paid", "paid", "paid", "late", "unpaid"]),
                "generated_timestamp": iso(config.start_date + timedelta(days=config.days, hours=2)),
            }
        )
    path = config.output / "billing" / f"billing_month={billing_month.date().isoformat()}" / "batch_000.jsonl"
    return {str(path): write_jsonl(path, records)}


def generate_gis_zones(config: GeneratorConfig, zones: List[Dict[str, Any]]) -> Dict[str, int]:
    path = config.output / "gis_zones" / "zones.csv"
    fields = [
        "zone_id",
        "zone_name",
        "city",
        "state",
        "pressure_band",
        "service_priority",
        "centroid_latitude",
        "centroid_longitude",
    ]
    return {str(path): write_csv(path, zones, fields)}


def main() -> None:
    config = parse_args()
    rng = random.Random(config.seed)
    ensure_dir(config.output)

    zones = build_zones(rng)
    customers = build_customers(config, zones, rng)
    meters = build_meter_inventory(config, customers, rng)

    counts: Dict[str, int] = {}
    counts.update(generate_gis_zones(config, zones))
    counts.update(generate_customer_cdc(config, customers, zones, rng))
    counts.update(generate_meter_readings(config, meters, rng))
    counts.update(generate_outage_events(config, meters, rng))
    counts.update(generate_billing(config, customers, rng))

    print("HydroFlow synthetic raw data generation complete.")
    print(f"Output root: {config.output}")
    total = 0
    for path, count in sorted(counts.items()):
        total += count
        print(f"{count:>10} records -> {path}")
    print(f"{total:>10} total records")


if __name__ == "__main__":
    main()
