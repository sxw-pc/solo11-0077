"""Deterministic synthetic fixtures for the offline Sharding workbench demo."""

from __future__ import annotations

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from .canonical import file_sha256, fingerprint
from .config_validation import validate_document
from .router import Router

DEMO_SEED = 20260102
GENERATED_AT = "2026-01-02T03:04:05Z"
TABLE_COLUMNS = {
    "orders": ["row_id", "tenant_id", "amount", "channel", "note"],
    "events": ["row_id", "event_day", "actor_id", "kind", "note"],
    "profiles": ["row_id", "region", "display_name", "segment"],
}
ANOMALIES = {
    "orders": {"misplaced": [17, 53], "duplicate": [71, 108], "missing": [145]},
    "events": {"misplaced": [22], "duplicate": [], "missing": []},
    "profiles": {"misplaced": [], "duplicate": [], "missing": []},
}
QUERIES = [
    {
        "name": "orders_by_tenant",
        "table": "orders",
        "description": "Exact tenant lookup should touch only the routed shard.",
        "filters": [{"field": "tenant_id", "op": "eq", "value": "tenant_demo_007"}],
    },
    {
        "name": "events_by_day",
        "table": "events",
        "description": "A half-open day window may overlap more than one range shard.",
        "filters": [
            {"field": "event_day", "op": "gte", "value": "2024-04-15"},
            {"field": "event_day", "op": "lt", "value": "2024-07-15"},
        ],
    },
    {
        "name": "profiles_by_region",
        "table": "profiles",
        "description": "Region membership resolves through the list map.",
        "filters": [
            {"field": "region", "op": "in", "value": ["region_alpha", "region_gamma"]}
        ],
    },
]

def _topology(new: bool) -> dict[str, Any]:
    suffix = "new" if new else "old"
    orders_buckets = 32 if new else 16
    orders_map = (
        [f"shard_{index // 8:02d}" for index in range(orders_buckets)]
        if new
        else [f"shard_{index // 4:02d}" for index in range(16)]
    )
    if new:
        ranges = [
            {"lower": "2024-01-01", "upper": "2024-03-01", "shard": "shard_10"},
            {"lower": "2024-03-01", "upper": "2024-06-01", "shard": "shard_11"},
            {"lower": "2024-06-01", "upper": "2024-09-01", "shard": "shard_12"},
            {"lower": "2024-09-01", "upper": None, "shard": "shard_13"},
        ]
        regions = {
            "region_alpha": "shard_20",
            "region_beta": "shard_22",
            "region_gamma": "shard_21",
            "region_delta": "shard_23",
        }
        default_region = "shard_20"
    else:
        ranges = [
            {"lower": "2024-01-01", "upper": "2024-04-01", "shard": "shard_10"},
            {"lower": "2024-04-01", "upper": "2024-07-01", "shard": "shard_11"},
            {"lower": "2024-07-01", "upper": "2024-10-01", "shard": "shard_12"},
            {"lower": "2024-10-01", "upper": None, "shard": "shard_13"},
        ]
        regions = {
            "region_alpha": "shard_20",
            "region_beta": "shard_21",
            "region_gamma": "shard_22",
        }
        default_region = "shard_23"
    shards = [f"shard_{index:02d}" for index in range(4)]
    shards += [f"shard_1{index}" for index in range(4)]
    shards += [f"shard_2{index}" for index in range(4)]
    return {
        "version": 1,
        "generated_at": GENERATED_AT,
        "description": f"Synthetic demo topology ({suffix}); not connected to any datastore.",
        "shards": shards,
        "tables": [
            {
                "table": "orders",
                "router": "hash",
                "key": "tenant_id",
                "null_policy": "route_default",
                "default_shard": "shard_00",
                "buckets": orders_buckets,
                "bucket_map": orders_map,
                "case_sensitive": True,
            },
            {
                "table": "events",
                "router": "range",
                "key": "event_day",
                "null_policy": "error",
                "default_shard": None,
                "ranges": ranges,
                "case_sensitive": True,
            },
            {
                "table": "profiles",
                "router": "list",
                "key": "region",
                "null_policy": "route_default",
                "default_shard": default_region,
                "list_map": regions,
                "case_sensitive": True,
            },
        ],
    }

def make_rows(seed: int) -> dict[str, list[dict[str, str]]]:
    rng = random.Random(seed)
    orders = []
    for index in range(1, 201):
        orders.append(
            {
                "row_id": f"ord_demo_{index:04d}",
                "tenant_id": f"tenant_demo_{rng.randint(1, 24):03d}",
                "amount": f"{rng.randint(100, 50000) / 100:.2f}",
                "channel": rng.choice(["web_demo", "store_demo", "partner_demo"]),
                "note": "synthetic order fixture",
            }
        )
    events = []
    base_day = date(2024, 1, 1)
    for index in range(1, 121):
        day = base_day + timedelta(days=rng.randrange(355))
        events.append(
            {
                "row_id": f"evt_demo_{index:04d}",
                "event_day": day.isoformat(),
                "actor_id": f"actor_demo_{rng.randint(1, 40):03d}",
                "kind": rng.choice(["checkout_demo", "view_demo", "return_demo"]),
                "note": "synthetic event fixture",
            }
        )
    regions = ["region_alpha"] * 3 + ["region_beta"] * 3 + ["region_gamma"] * 3 + ["region_omega"]
    profiles = []
    for index in range(1, 81):
        profiles.append(
            {
                "row_id": f"prf_demo_{index:04d}",
                "region": rng.choice(regions),
                "display_name": f"Sample Profile {index:03d}",
                "segment": rng.choice(["segment_demo_a", "segment_demo_b"]),
            }
        )
    return {"orders": orders, "events": events, "profiles": profiles}

def _json_write(path: Path, value: Any) -> None:
    text = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8"))

def _csv_write(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})

def _alternate(shards: list[str], shard: str) -> str:
    index = shards.index(shard)
    return shards[(index + 1) % len(shards)]

def _write_physical(
    root: Path, topology: Any, rows: dict[str, list[dict[str, str]]]
) -> dict[str, Any]:
    router = Router(topology)
    manifests: dict[tuple[str, str], dict[str, Any]] = {}
    physical_counts: dict[str, int] = {}
    for table in topology.table_names():
        table_topology = topology.table(table)
        shards = table_topology.table_shards()
        base: dict[str, list[dict[str, str]]] = {shard: [] for shard in shards}
        extra: dict[str, list[dict[str, str]]] = {shard: [] for shard in shards}
        anomaly = ANOMALIES[table]
        misplaced = set(anomaly["misplaced"])
        duplicate = set(anomaly["duplicate"])
        missing = set(anomaly["missing"])
        for index, row in enumerate(rows[table]):
            shard = router.route_values(table, row).target_shard
            assert shard is not None
            alternate = _alternate(shards, shard)
            if index in missing:
                continue
            if index in misplaced:
                extra[alternate].append(row)
                continue
            base[shard].append(row)
            if index in duplicate:
                extra[alternate].append(row)
        for shard in shards:
            directory = root / table / shard
            files = [("part-000.csv", base[shard])]
            if extra[shard]:
                files.append(("part-001.csv", extra[shard]))
            entries = []
            for name, file_rows in files:
                _csv_write(directory / name, file_rows, TABLE_COLUMNS[table])
                entries.append(
                    {
                        "name": name,
                        "rows": len(file_rows),
                        "sha256": file_sha256(directory / name),
                    }
                )
            manifests[(table, shard)] = {
                "format_version": 1,
                "table": table,
                "shard": shard,
                "generated_at": GENERATED_AT,
                "row_count": sum(entry["rows"] for entry in entries),
                "files": entries,
            }
            physical_counts[table] = physical_counts.get(table, 0) + sum(
                entry["rows"] for entry in entries
            )
    manifests[("events", "shard_11")]["row_count"] += 1
    orphan_dir = root / "profiles" / "shard_20"
    _csv_write(orphan_dir / "orphan-part-009.csv", [], TABLE_COLUMNS["profiles"])
    for (table, shard), manifest in sorted(manifests.items()):
        _json_write(root / table / shard / "_manifest.json", manifest)
    return {"physical_rows": physical_counts, "manifests": len(manifests)}

def build_fixtures(root: str | Path, seed: int = DEMO_SEED) -> dict[str, Any]:
    root = Path(root)
    for relative in ("config", "logical", "queries", "shards"):
        (root / relative).mkdir(parents=True, exist_ok=True)
    old_document, new_document = _topology(False), _topology(True)
    _json_write(root / "config" / "topology_old.json", old_document)
    _json_write(root / "config" / "topology_new.json", new_document)
    rows = make_rows(seed)
    for table in ("orders", "events", "profiles"):
        _csv_write(root / "logical" / f"{table}.csv", rows[table], TABLE_COLUMNS[table])
    for query in QUERIES:
        _json_write(root / "queries" / f"{query['name']}.json", query)
    topology, issues, _ = validate_document(old_document)
    assert topology is not None and not issues
    physical = _write_physical(root / "shards", topology, rows)
    tree = [
        {"path": path.relative_to(root).as_posix(), "sha256": file_sha256(path)}
        for path in sorted(root.rglob("*"))
        if path.is_file()
    ]
    return {
        "seed": seed,
        "logical_rows": {table: len(rows[table]) for table in sorted(rows)},
        "physical_rows": physical["physical_rows"],
        "manifest_count": physical["manifests"],
        "fixture_fingerprint": fingerprint(tree),
    }
