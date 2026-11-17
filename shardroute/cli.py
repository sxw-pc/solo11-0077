"""Command-line entry points for validation, demo, audit, planning and serving."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from . import __version__
from .audit import run_audit
from .config import load_topology, read_json, validation_report
from .demo import run_demo, run_pipeline
from .errors import CLI_USAGE, ConfigValidationError, ShardRouteError
from .planner import build_plan, search_layouts
from .query_explain import explain_file
from .report import (
    build_layout_search_report,
    write_layout_search_report,
)
from .router import Router
from .scanner import scan_workspace

def _emit(value: Any) -> None:
    sys.stdout.write(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n")

def _topology_paths(workspace: str) -> tuple[Path, Path]:
    root = Path(workspace)
    return root / "config" / "topology_old.json", root / "config" / "topology_new.json"

def _cmd_validate(args: argparse.Namespace) -> int:
    result = validation_report(args.config)
    _emit(result)
    return 0 if result["valid"] else 2

def _cmd_route(args: argparse.Namespace) -> int:
    topology = load_topology(args.config)
    if args.key_json is not None:
        try:
            value = json.loads(args.key_json)
        except ValueError as exc:
            raise ShardRouteError(CLI_USAGE, f"invalid --key-json: {exc}") from exc
    else:
        value = args.key
    trace = Router(topology).route(args.table, value)
    _emit(trace.as_dict())
    return 0

def _cmd_audit(args: argparse.Namespace) -> int:
    old, _ = _topology_paths(args.workspace)
    topology = load_topology(args.config or old)
    root = Path(args.workspace)
    scan = scan_workspace(root / "logical", root / "shards", topology.table_names())
    result = run_audit(topology, scan)
    _emit(result)
    return 0

def _cmd_plan(args: argparse.Namespace) -> int:
    old_path, new_path = _topology_paths(args.workspace)
    old, new = load_topology(old_path), load_topology(new_path)
    root = Path(args.workspace)
    scan = scan_workspace(root / "logical", root / "shards", old.table_names())
    audit = run_audit(old, scan)
    _emit(build_plan(old, new, scan.logical_rows, audit))
    return 0

def _cmd_explain(args: argparse.Namespace) -> int:
    old_path, _ = _topology_paths(args.workspace)
    topology = load_topology(args.config or old_path)
    if args.query:
        _emit(explain_file(topology, args.query).as_dict())
    else:
        paths = sorted((Path(args.workspace) / "queries").glob("*.json"), key=lambda p: p.name)
        _emit([explain_file(topology, path).as_dict() for path in paths])
    return 0

def _cmd_report(args: argparse.Namespace) -> int:
    _emit(run_pipeline(args.workspace, args.seed))
    return 0

def _load_layout_spec(path: str) -> dict[str, Any]:
    payload = read_json(Path(path))
    if not isinstance(payload, dict):
        raise ShardRouteError(CLI_USAGE, "layout search spec must be a JSON object", path)
    return payload

def _load_workload(spec: dict[str, Any], workspace: str) -> list[dict[str, Any]]:
    if "workload" in spec:
        raw_workload = spec["workload"]
        if not isinstance(raw_workload, list):
            raise ShardRouteError(CLI_USAGE, "workload must be a list")
        entries = []
        for index, item in enumerate(raw_workload):
            if not isinstance(item, dict) or "query" not in item:
                raise ShardRouteError(
                    CLI_USAGE,
                    "each workload entry must be an object containing a query",
                    f"workload[{index}]",
                )
            frequency = item.get("frequency", 1)
            if isinstance(frequency, bool) or not isinstance(frequency, int) or frequency < 0:
                raise ShardRouteError(
                    CLI_USAGE,
                    "workload frequency must be a non-negative integer",
                    f"workload[{index}]",
                )
            entries.append({"frequency": frequency, "query": item["query"]})
        return entries
    query_dir = Path(spec.get("query_dir", str(Path(workspace) / "queries")))
    paths = sorted(query_dir.glob("*.json"), key=lambda item: item.name)
    return [{"frequency": 1, "query": read_json(path)} for path in paths]

def _cmd_layout_search(args: argparse.Namespace) -> int:
    spec = _load_layout_spec(args.spec)
    config_path = args.config or _topology_paths(args.workspace)[0]
    old = load_topology(config_path)
    root = Path(args.workspace)
    scan = scan_workspace(root / "logical", root / "shards", old.table_names())
    audit = run_audit(old, scan)
    workload = _load_workload(spec, args.workspace)
    search = search_layouts(
        old,
        spec["table"],
        scan.logical_rows,
        spec["domains"],
        fixed_keys=spec.get("fixed_keys", []),
        move_budget_rows=int(spec["move_budget_rows"]),
        shard_read_caps=spec["shard_read_caps"],
        workload=workload,
        audit=audit,
        max_evaluations=int(spec.get("max_evaluations", args.max_evaluations)),
    )
    if args.write_report:
        wrapped = build_layout_search_report(
            old=old,
            search=search,
            logical_fingerprint=scan.logical_fingerprint,
            physical_fingerprint=scan.physical_fingerprint,
            seed=args.seed,
        )
        artifacts = write_layout_search_report(wrapped, args.workspace)
        _emit({"artifacts": artifacts, "result": search})
    else:
        _emit(search)
    return 0

def _cmd_demo(args: argparse.Namespace) -> int:
    _emit(run_demo(args.workspace, args.seed))
    return 0

def _cmd_serve(args: argparse.Namespace) -> int:
    from .server import serve
    serve(args.workspace, args.host, args.port)
    return 0

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shardroute",
        description="Offline shard routing verification workbench (no datastore access).",
    )
    parser.add_argument("--version", action="version", version=f"shardroute {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
    demo = sub.add_parser("demo", help="generate deterministic fixtures and run the pipeline")
    demo.add_argument("--workspace", default=".shardroute-demo")
    demo.add_argument("--seed", type=int, default=20260102)
    demo.set_defaults(handler=_cmd_demo)
    validate = sub.add_parser("validate", help="validate a topology JSON file")
    validate.add_argument("--config", required=True)
    validate.set_defaults(handler=_cmd_validate)
    route = sub.add_parser("route", help="route one key and print its trace")
    route.add_argument("--config", required=True)
    route.add_argument("--table", required=True)
    group = route.add_mutually_exclusive_group(required=True)
    group.add_argument("--key", help="single key as text")
    group.add_argument("--key-json", help="JSON scalar or component array")
    route.set_defaults(handler=_cmd_route)
    audit = sub.add_parser("audit", help="compare logical rows with a physical shard tree")
    audit.add_argument("--workspace", required=True)
    audit.add_argument("--config", help="override the workspace old topology")
    audit.set_defaults(handler=_cmd_audit)
    plan = sub.add_parser("plan", help="print a dry-run resharding plan")
    plan.add_argument("--workspace", required=True)
    plan.set_defaults(handler=_cmd_plan)
    layout = sub.add_parser(
        "layout-search",
        help="search feasible list_map layouts against a query workload (offline, read-only)",
    )
    layout.add_argument("--workspace", required=True)
    layout.add_argument("--spec", required=True, help="layout search spec JSON path")
    layout.add_argument("--config", help="override the workspace old topology")
    layout.add_argument("--seed", type=int, default=20260102)
    layout.add_argument(
        "--max-evaluations", type=int, default=100000,
        help="leaf-evaluation budget; exhaustion yields status=unknown",
    )
    layout.add_argument(
        "--write-report", action="store_true",
        help="also write reports/layout_search.{json,md} into the workspace",
    )
    layout.set_defaults(handler=_cmd_layout_search)
    explain = sub.add_parser("explain", help="explain query shard pruning")
    explain.add_argument("--workspace", required=True)
    explain.add_argument("--config", help="override the workspace old topology")
    explain.add_argument("--query", help="query JSON path; omit to explain all workspace queries")
    explain.set_defaults(handler=_cmd_explain)
    report = sub.add_parser("report", help="rebuild reports and ledger for an existing workspace")
    report.add_argument("--workspace", required=True)
    report.add_argument("--seed", type=int, default=20260102)
    report.set_defaults(handler=_cmd_report)
    serve = sub.add_parser("serve", help="start the read-only loopback UI")
    serve.add_argument("--workspace", default=".shardroute-demo")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.set_defaults(handler=_cmd_serve)
    return parser

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except ConfigValidationError as exc:
        sys.stderr.write(json.dumps(exc.as_dict(), sort_keys=True, indent=2, ensure_ascii=False) + "\n")
        return 2
    except ShardRouteError as exc:
        sys.stderr.write(json.dumps(exc.as_dict(), sort_keys=True, indent=2, ensure_ascii=False) + "\n")
        return 1
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"[CLI] {exc}\n")
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
