"""
Data preparation utilities for Malaysia transportation dashboard.

Outputs produced in the data/ directory:
  - state_bus_counts.json : Bus terminal density per state with coordinates.
  - motor_vehicles_trend.json : Yearly counts for each vehicle class.
  - rail_monthly_ridership.json : Average monthly ridership by rail mode.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SOURCE_DIR = ROOT / "source"

BUS_STATE_COORDS: Dict[str, Tuple[float, float]] = {
    "Johor": (1.4847, 103.7618),
    "Kedah": (6.1184, 100.3685),
    "Kelantan": (5.2852, 102.0030),
    "Kuala Lumpur": (3.1390, 101.6869),
    "Melaka": (2.1896, 102.2501),
    "Negeri Sembilan": (2.7258, 102.1400),
    "Pahang": (3.7956, 102.4381),
    "Penang": (5.4141, 100.3290),
    "Perak": (4.5921, 101.0901),
    "Perlis": (6.4440, 100.2040),
    "Sabah": (5.9788, 116.0753),
    "Sarawak": (1.5533, 110.3592),
    "Selangor": (3.0738, 101.5183),
    "Terengganu": (5.3290, 103.1412),
}


def load_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def build_state_bus_counts(rows: Iterable[Dict[str, str]]) -> List[Dict[str, object]]:
    grouped: Dict[str, List[str]] = defaultdict(list)
    for row in rows:
        state = row.get("State", "").strip()
        terminal = row.get("Terminal / Station", "").strip()
        if not state or not terminal:
            continue
        grouped[state].append(terminal)

    records: List[Dict[str, object]] = []
    for state, terminals in grouped.items():
        coords = BUS_STATE_COORDS.get(state)
        if not coords:
            continue
        lat, lon = coords
        terminals_sorted = sorted(terminals)
        records.append(
            {
                "state": state,
                "terminal_count": len(terminals),
                "sample_terminals": terminals_sorted[:5],
                "latitude": lat,
                "longitude": lon,
            }
        )

    records.sort(key=lambda item: item["terminal_count"], reverse=True)
    return records


def build_motor_vehicle_trend(rows: Iterable[Dict[str, str]]) -> List[Dict[str, object]]:
    tidy: List[Dict[str, object]] = []
    for row in rows:
        year_raw = row.get("Yea") or row.get("Year")
        vehicle_type = (row.get("Type of vehicle") or "").strip()
        value_raw = row.get("Value")
        if not year_raw or not vehicle_type or not value_raw:
            continue
        try:
            year = int(year_raw)
            value = int(value_raw)
        except ValueError:
            continue
        tidy.append(
            {
                "year": year,
                "vehicle_type": vehicle_type,
                "count": value,
            }
        )
    tidy.sort(key=lambda item: (item["vehicle_type"], item["year"]))
    return tidy


RAIL_FIELDS = [
    ("rail_lrt_ampang", "LRT Ampang"),
    ("rail_lrt_kj", "LRT Kelana Jaya"),
    ("rail_mrt_kajang", "MRT Kajang"),
    ("rail_mrt_pjy", "MRT Putrajaya"),
    ("rail_monorail", "KL Monorail"),
    ("rail_komuter", "KTM Komuter"),
    ("rail_komuter_utara", "KTM Komuter Utara"),
    ("rail_ets", "ETS"),
    ("rail_intercity", "Intercity"),
    ("rail_tebrau", "Shuttle Tebrau"),
]


def build_rail_monthly_ridership(rows: Iterable[Dict[str, str]]) -> List[Dict[str, object]]:
    aggregates: Dict[Tuple[str, str], Tuple[int, int]] = {}
    for row in rows:
        date_raw = (row.get("date") or "").strip()
        if not date_raw:
            continue

        try:
            date_obj = datetime.strptime(date_raw, "%m/%d/%Y")
        except ValueError:
            try:
                date_obj = datetime.strptime(date_raw, "%d/%m/%Y")
            except ValueError:
                continue

        month_key = date_obj.strftime("%Y-%m")

        for field_name, label in RAIL_FIELDS:
            value_raw = (row.get(field_name) or "").replace(",", "").strip()
            if not value_raw:
                continue
            try:
                value = int(float(value_raw))
            except ValueError:
                continue
            key = (label, month_key)
            total, count = aggregates.get(key, (0, 0))
            aggregates[key] = (total + value, count + 1)

    tidy: List[Dict[str, object]] = []
    for (label, month_key), (total, count) in aggregates.items():
        if count == 0:
            continue
        tidy.append(
            {
                "month": month_key,
                "mode": label,
                "average_ridership": round(total / count),
            }
        )

    tidy.sort(key=lambda item: (item["mode"], item["month"]))
    return tidy


def write_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)

    # Bus terminals
    bus_path = SOURCE_DIR / "bus_domestic.csv"
    if not bus_path.exists():
        bus_path = ROOT / "bus_domestic.csv"
    bus_rows = load_csv(bus_path)
    bus_records = build_state_bus_counts(bus_rows)
    write_json(DATA_DIR / "state_bus_counts.json", bus_records)

    # Motor vehicles trend
    vehicles_path = SOURCE_DIR / "2000 2021 Number of Cumulative Motor Vehicles Regi.csv"
    if not vehicles_path.exists():
        vehicles_path = ROOT / "2000 2021 Number of Cumulative Motor Vehicles Regi.csv"
    vehicle_rows = load_csv(vehicles_path)
    vehicle_records = build_motor_vehicle_trend(vehicle_rows)
    write_json(DATA_DIR / "motor_vehicles_trend.json", vehicle_records)

    # Rail ridership monthly averages
    ridership_path = SOURCE_DIR / "ridership_headline.csv"
    if not ridership_path.exists():
        ridership_path = ROOT / "ridership_headline.csv"
    ridership_rows = load_csv(ridership_path)
    ridership_records = build_rail_monthly_ridership(ridership_rows)
    write_json(DATA_DIR / "rail_monthly_ridership.json", ridership_records)

    summary = {
        "bus_states": len(bus_records),
        "vehicle_records": len(vehicle_records),
        "ridership_records": len(ridership_records),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
