"""Regenerate the committed parent/child relation fixture workspace.

Run from the repository root:

    python tests/fixtures/relations/generate.py

The output is deterministic; re-running produces byte-identical CSV/JSONL,
topology and manifest files.  The script only writes inside its own directory
and never touches a datastore.
"""
import csv, hashlib, json, shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
for part in ("config", "logical", "shards"):
    target = ROOT / part
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)

SHARDS = ["shard_a", "shard_b", "shard_c"]
OLD_MAP = {"region_alpha": "shard_a", "region_beta": "shard_b", "region_gamma": "shard_c"}
ACCOUNT_NEW = {"region_alpha": "shard_a", "region_beta": "shard_a", "region_gamma": "shard_c"}
ITEM_NEW = {"region_alpha": "shard_b", "region_beta": "shard_c", "region_gamma": "shard_c"}

def topology(new: bool):
    return {
        "version": 1,
        "generated_at": "2026-01-02T03:04:05Z",
        "description": "Synthetic parent/child relation fixture; not connected to any datastore.",
        "shards": SHARDS,
        "tables": [
            {"table": "accounts", "router": "list", "key": "home_region",
             "null_policy": "route_default", "default_shard": "shard_c",
             "list_map": ACCOUNT_NEW if new else OLD_MAP, "case_sensitive": True},
            {"table": "orders", "router": "list", "key": "region",
             "null_policy": "route_default", "default_shard": "shard_c",
             "list_map": OLD_MAP, "case_sensitive": True},
            {"table": "catalogs", "router": "list", "key": "catalog_region",
             "null_policy": "route_default", "default_shard": "shard_c",
             "list_map": OLD_MAP, "case_sensitive": True},
            {"table": "order_items", "router": "list", "key": "region",
             "null_policy": "route_default", "default_shard": "shard_c",
             "list_map": ITEM_NEW if new else OLD_MAP, "case_sensitive": True},
        ],
        "relations": [
            {"name": "orders_accounts", "parent_table": "accounts",
             "parent_key": "account_code", "child_table": "orders",
             "child_key": "account_ref", "mode": "local_join"},
            {"name": "items_catalogs", "parent_table": "catalogs",
             "parent_key": "catalog_code", "child_table": "order_items",
             "child_key": "catalog_ref", "mode": "replicated_parent"},
        ],
    }

for suffix, new in (("old", False), ("new", True)):
    text = json.dumps(topology(new), sort_keys=True, indent=2) + "\n"
    (ROOT / "config" / f"topology_{suffix}.json").write_text(text, encoding="utf-8")

CSV_COLUMNS = {
    "accounts": ["row_id", "account_code", "home_region", "account_name"],
    "orders": ["row_id", "region", "account_ref", "amount"],
    "catalogs": ["row_id", "catalog_code", "catalog_region", "catalog_name"],
}
JSONL_COLUMNS = ["row_id", "region", "catalog_ref", "sku"]

ACCOUNTS = [
    {"row_id": "acc_alpha", "account_code": "ACC_ALPHA", "home_region": "region_alpha", "account_name": "Account Alpha"},
    {"row_id": "acc_split", "account_code": "ACC_SPLIT", "home_region": "region_beta", "account_name": "Account Split"},
    {"row_id": "acc_dup_1", "account_code": "ACC_BETA", "home_region": "region_beta", "account_name": "Account Beta One"},
    {"row_id": "acc_dup_2", "account_code": "ACC_BETA", "home_region": "region_beta", "account_name": "Account Beta Two"},
    {"row_id": "acc_gamma", "account_code": "ACC_GAMMA", "home_region": "region_gamma", "account_name": "Account Gamma v1"},
]
ORDERS = [
    {"row_id": "ord_ok", "region": "region_alpha", "account_ref": "ACC_ALPHA", "amount": "10.00"},
    {"row_id": "ord_repeat_1", "region": "region_alpha", "account_ref": "ACC_ALPHA", "amount": "11.00"},
    {"row_id": "ord_repeat_2", "region": "region_alpha", "account_ref": "ACC_ALPHA", "amount": "12.00"},
    {"row_id": "ord_split", "region": "region_beta", "account_ref": "ACC_SPLIT", "amount": "20.00"},
    {"row_id": "ord_missing", "region": "region_alpha", "account_ref": "ACC_MISSING", "amount": "30.00"},
    {"row_id": "ord_dup", "region": "region_beta", "account_ref": "ACC_BETA", "amount": "40.00"},
    {"row_id": "ord_drift", "region": "region_gamma", "account_ref": "ACC_GAMMA", "amount": "50.00"},
    {"row_id": "ord_noref", "region": "region_alpha", "account_ref": "", "amount": "60.00"},
]
CATALOGS = [
    {"row_id": "cat_alpha", "catalog_code": "CAT_ALPHA", "catalog_region": "region_alpha", "catalog_name": "Catalog Alpha"},
    {"row_id": "cat_beta", "catalog_code": "CAT_BETA", "catalog_region": "region_beta", "catalog_name": "Catalog Beta"},
    {"row_id": "cat_gamma", "catalog_code": "CAT_GAMMA", "catalog_region": "region_gamma", "catalog_name": "Catalog Gamma v1"},
]
ITEMS = [
    {"row_id": "it_alpha", "region": "region_alpha", "catalog_ref": "CAT_ALPHA", "sku": "sku_alpha"},
    {"row_id": "it_peer", "region": "region_beta", "catalog_ref": "CAT_ALPHA", "sku": "sku_peer"},
    {"row_id": "it_target", "region": "region_beta", "catalog_ref": "CAT_BETA", "sku": "sku_target"},
    {"row_id": "it_drift", "region": "region_gamma", "catalog_ref": "CAT_GAMMA", "sku": "sku_drift"},
    {"row_id": "it_replicated", "region": "region_gamma", "catalog_ref": "CAT_ALPHA", "sku": "sku_rep"},
    {"row_id": "it_noref", "region": "region_alpha", "catalog_ref": "", "sku": "sku_noref"},
]

def drifted(row, column, value):
    return {**row, column: value}

PHYSICAL = {
    "accounts": {
        "shard_a": [ACCOUNTS[0]],
        "shard_b": [ACCOUNTS[1], ACCOUNTS[2], ACCOUNTS[3]],
        "shard_c": [drifted(ACCOUNTS[4], "account_name", "Account Gamma stale")],
    },
    "orders": {
        "shard_a": [ORDERS[0], ORDERS[1], ORDERS[2], ORDERS[4], ORDERS[7]],
        "shard_b": [ORDERS[3], ORDERS[5]],
        "shard_c": [ORDERS[6]],
    },
    # CAT_ALPHA is replicated into shard_b because it_peer sits there.
    "catalogs": {
        "shard_a": [CATALOGS[0]],
        "shard_b": [CATALOGS[1], CATALOGS[0]],
        "shard_c": [drifted(CATALOGS[2], "catalog_name", "Catalog Gamma stale"), CATALOGS[0]],
    },
    "order_items": {
        "shard_a": [ITEMS[0], ITEMS[5]],
        "shard_b": [ITEMS[1], ITEMS[2]],
        "shard_c": [ITEMS[3], ITEMS[4]],
    },
}

def write_csv(path, rows, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})

def write_jsonl(path, rows, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps({column: row.get(column, "") for column in columns}, sort_keys=True)
        for row in rows
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

# Logical snapshots: three CSV tables and one JSONL table.
for table in ("accounts", "orders", "catalogs"):
    write_csv(ROOT / "logical" / f"{table}.csv", {"accounts": ACCOUNTS, "orders": ORDERS, "catalogs": CATALOGS}[table], CSV_COLUMNS[table])
write_jsonl(ROOT / "logical" / "order_items.jsonl", ITEMS, JSONL_COLUMNS)

manifests = {}
for table, shards in PHYSICAL.items():
    for shard, rows in shards.items():
        directory = ROOT / "shards" / table / shard
        if table == "order_items":
            name = "part-000.jsonl"
            write_jsonl(directory / name, rows, JSONL_COLUMNS)
        else:
            name = "part-000.csv"
            write_csv(directory / name, rows, CSV_COLUMNS[table])
        manifests[(table, shard)] = {
            "format_version": 1, "table": table, "shard": shard,
            "generated_at": "2026-01-02T03:04:05Z",
            "row_count": len(rows),
            "files": [{"name": name, "rows": len(rows), "sha256": sha(directory / name)}],
        }
for (table, shard), manifest in sorted(manifests.items()):
    (ROOT / "shards" / table / shard / "_manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
print("generated", sum(len(v) for v in PHYSICAL.values()), "physical rows", len(manifests), "manifests")
