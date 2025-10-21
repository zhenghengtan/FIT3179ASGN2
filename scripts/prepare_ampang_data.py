"""
Utility script to reshape LRT Ampang line fare data and align station coordinates.

Outputs:
  - data/fare_long.csv / data/fare_long.json: tidy origin-destination fare records.
  - data/ampang_station_locations.json: coordinates matched to the fare matrix stations.
  - data/ampang_route.json: ordered coordinates for plotting the line path.
"""

from __future__ import annotations

import csv
import json
import math
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
FARE_PATH = ROOT / "Fare.csv"
STATIONS_PATH = ROOT / "lrt-malaysia.csv"
DATA_DIR = ROOT / "data"


def read_fare_matrix(path: Path) -> Tuple[List[str], List[Dict[str, object]]]:
    """Return station order and tidy fare rows."""
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("Fare.csv is empty")

        destinations = [col.strip() for col in header[1:]]
        station_order: List[str] = []
        tidy_rows: List[Dict[str, object]] = []

        for row in reader:
            if not row:
                continue
            origin = row[0].strip()
            if not origin:
                continue

            if origin not in station_order:
                station_order.append(origin)

            for dest, value in zip(destinations, row[1:]):
                dest = dest.strip()
                if not dest or value == "":
                    continue
                try:
                    fare = float(value)
                except ValueError:
                    # Skip malformed entries
                    continue
                tidy_rows.append(
                    {"origin": origin, "destination": dest, "fare": fare}
                )

        # Ensure all header destinations are represented in station_order
        for dest in destinations:
            if dest and dest not in station_order:
                station_order.append(dest)

    return station_order, tidy_rows


TOKENS_TO_REMOVE = {
    "stesen",
    "station",
    "stations",
    "lrt",
    "mrt",
    "monorail",
    "line",
    "rapidkl",
    "putra",
    "klia",
    "malaysia",
}

LINE_ABBREV = {"kjl", "sbk", "agc", "ssp", "kelana", "ampang"}


def canonical_name(label: str) -> str:
    text = label.lower()
    text = text.replace("&", " and ")
    for punct in ("'", "’"):
        text = text.replace(punct, "")
    # Expose tokens inside parentheses instead of dropping outright
    text = text.replace("(", " ").replace(")", " ")
    cleaned_tokens: List[str] = []
    for token in text.split():
        if token in TOKENS_TO_REMOVE:
            continue
        if token in LINE_ABBREV:
            continue
        cleaned_tokens.append(token)
    return " ".join(cleaned_tokens).strip()


def build_station_lookup(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    lookup: Dict[str, Dict[str, str]] = {}
    for row in rows:
        name = row.get("station_name", "").strip()
        if not name:
            continue
        key = canonical_name(name)
        if not key:
            continue
        lookup.setdefault(key, row)
    return lookup


MANUAL_ALIASES: Dict[str, str] = {
    "maluri sbk": "maluri",
    "pasar seni kjl": "pasar seni",
    "pasar seni sbk": "pasar seni",
    "masjid jamek kjl": "masjid jamek",
    "masjid jamek sbk": "masjid jamek",
    "putra heights kjl": "putra heights",
    "pasar seni": "pasar seni",
    "kajang": "kajang",
}

MANUAL_COORDINATES: Dict[str, Tuple[float, float]] = {
    "kl sentral": (3.1346, 101.6865),
    "bangsar": (3.1282, 101.6790),
    "taman paramount": (3.1075, 101.6222),
    "taman bahagia": (3.1116, 101.6144),
    "jaya": (3.1093, 101.6151),  # Kelana Jaya
    "ss 15": (3.0817, 101.5863),
    "ss 18": (3.0744, 101.5931),
    "usj 7": (3.0482, 101.5934),
    "usj 21": (3.0279, 101.5853),
    "sungai buloh": (3.1979, 101.5771),
    "kampung selamat": (3.2106, 101.5703),
    "kwasa damansara": (3.1804, 101.5666),
    "kwasa sentral": (3.1735, 101.5732),
    "kota damansara": (3.1575, 101.5853),
    "surian": (3.1457, 101.5937),
    "mutiara damansara": (3.1443, 101.6096),
    "bandar utama": (3.1460, 101.6157),
    "ttdi": (3.1374, 101.6278),
    "phileo damansara": (3.1313, 101.6408),
    "pusat bandar damansara": (3.1485, 101.6632),
    "semantan": (3.1540, 101.6610),
    "muzium negara": (3.1277, 101.6876),
    "merdeka": (3.1411, 101.6983),
    "bukit bintang": (3.1470, 101.7082),
    "tun razak exchange trx": (3.1399, 101.7188),
    "cochrane": (3.1317, 101.7329),
    "taman pertama": (3.1140, 101.7419),
    "taman midah": (3.1066, 101.7441),
    "taman mutiara": (3.0956, 101.7506),
    "taman connaught": (3.0847, 101.7542),
    "taman suntex": (3.0705, 101.7712),
    "sri raya": (3.0608, 101.7813),
    "bandar tun hussein onn": (3.0451, 101.7871),
    "batu 11 cheras": (3.0348, 101.7904),
    "bukit dukung": (3.0246, 101.7933),
    "sungai jernih": (3.0148, 101.7936),
    "stadium kajang": (3.0087, 101.7938),
    "kajang": (3.0033, 101.7896),
}


def match_station_locations(
    station_order: List[str], station_csv: Path
) -> Tuple[List[Dict[str, object]], List[str]]:
    with station_csv.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        lookup = build_station_lookup(reader)

    matched: List[Dict[str, object]] = []
    unmatched: List[str] = []

    for index, station in enumerate(station_order, start=1):
        key = canonical_name(station)
        if key in MANUAL_ALIASES:
            alias_key = MANUAL_ALIASES[key]
            row = lookup.get(alias_key)
        else:
            row = lookup.get(key)

        manual_coords = MANUAL_COORDINATES.get(key)

        if not row and not manual_coords:
            unmatched.append(station)
            continue

        lat = lon = None

        if row:
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
            except (TypeError, ValueError):
                lat = lon = None

            if lat is not None and lon is not None:
                if not (1.0 <= lat <= 7.5 and 99.0 <= lon <= 105.0):
                    lat = lon = None

        if (lat is None or lon is None) and manual_coords:
            lat, lon = manual_coords

        if lat is None or lon is None:
            unmatched.append(station)
            continue


        matched.append(
            {
                "station": station,
                "order": index,
                "latitude": lat,
                "longitude": lon,
            }
        )

    return matched, unmatched


def write_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    DATA_DIR.mkdir(exist_ok=True)

    station_order, tidy_rows = read_fare_matrix(FARE_PATH)

    write_json(DATA_DIR / "fare_long.json", tidy_rows)
    write_csv(DATA_DIR / "fare_long.csv", tidy_rows)

    station_locations, unmatched = match_station_locations(station_order, STATIONS_PATH)

    write_json(DATA_DIR / "ampang_station_locations.json", station_locations)
    write_json(DATA_DIR / "ampang_route.json", station_locations)
    write_json(DATA_DIR / "ampang_station_list.json", station_order)

    summary = {
        "stations_in_matrix": len(station_order),
        "matched_locations": len(station_locations),
        "unmatched": unmatched,
        "fare_records": len(tidy_rows),
    }
    print(json.dumps(summary, indent=2))

    if unmatched:
        print(
            f"WARNING: {len(unmatched)} stations were not matched to coordinates.",
            file=sys.stderr,
        )
        for name in unmatched:
            print(f"  - {name}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
