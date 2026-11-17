"""Inline-only HTML views for the read-only loopback review desk."""

from __future__ import annotations

import html
import json
from typing import Any

STYLE = """
:root{color-scheme:light dark;--bg:#fff;--ink:#1a222c;--muted:#62707d;--line:#d9e0e6;
--panel:#f5f7f9;--accent:#0b62b6;--bad:#a4262c;--warn:#8a5a00;--ok:#1d6b3d}
@media(prefers-color-scheme:dark){:root{--bg:#111820;--ink:#e7edf3;--muted:#9aa8b5;
--line:#34404b;--panel:#1b232c;--accent:#69adff;--bad:#ff9292;--warn:#ffd166;--ok:#7edca4}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}main{max-width:1150px;margin:auto;padding:30px 22px 60px}
h1{font-size:1.75rem;margin:0}h2{font-size:1.2rem;border-bottom:1px solid var(--line);padding-bottom:5px;margin-top:30px}
h3{font-size:1rem;margin-top:22px}p{margin:7px 0}a{color:var(--accent)}code{font-family:ui-monospace,Consolas,monospace}
table{width:100%;border-collapse:collapse;font-size:.88rem;margin:8px 0 18px}th,td{text-align:left;
border-bottom:1px solid var(--line);padding:7px 8px;vertical-align:top}th{background:var(--panel);font-size:.75rem;
text-transform:uppercase;letter-spacing:.03em}.wrap{overflow-x:auto}.muted{color:var(--muted)}.tag{display:inline-block;
border:1px solid currentColor;border-radius:999px;padding:1px 8px;font-size:.75rem;font-weight:700}
.fail,.critical,.high{color:var(--bad)}.attention,.medium{color:var(--warn)}.pass,.ok{color:var(--ok)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:16px 0}
.stat{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:11px}
.stat b{display:block;font-size:1.35rem}.stat span{font-size:.8rem;color:var(--muted)}
form{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0}input,select,button{font:inherit;padding:5px 8px;
border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--ink)}
button{background:var(--panel);cursor:pointer}.notice{background:var(--panel);border-left:4px solid var(--accent);
padding:10px 13px;margin:16px 0}ul{padding-left:22px}.empty{color:var(--muted);font-style:italic}
.nav{display:flex;gap:14px;flex-wrap:wrap;margin:12px 0}.nav a{font-size:.9rem}
"""


def _e(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _tag(value: str) -> str:
    css = value.lower() if value.lower() in {"pass", "fail", "attention", "critical", "high", "medium"} else ""
    return f'<span class="tag {css}">{_e(value)}</span>'


def _shell(title: str, body: str) -> str:
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_e(title)}</title><style>{STYLE}</style></head><body><main>{body}</main></body></html>"""


def render_index(runs: list[dict[str, Any]]) -> str:
    rows = []
    for run in runs:
        rows.append(
            f"<tr><td><a href=\"/runs/{_e(run['run_id'])}\"><code>{_e(run['run_id'])}</code></a></td>"
            f"<td>{_tag(run['status'])}</td><td>{_e(run['generated_at'])}</td>"
            f"<td>{run['summary'].get('audit_findings', 0)}</td>"
            f"<td>{run['summary'].get('rows_moved', 0)}</td>"
            f"<td><a href=\"/api/runs/{_e(run['run_id'])}\">JSON</a></td></tr>"
        )
    table = (
        "<div class=\"wrap\"><table><thead><tr><th>Run</th><th>Status</th><th>Generated (UTC)</th>"
        "<th>Findings</th><th>Rows moved</th><th>API</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
    )
    if not runs:
        table = "<p class=\"empty\">No runs in the ledger. Run <code>python -m shardroute demo</code> first.</p>"
    body = f"""<h1>ShardRoute review desk</h1>
<p class="muted">Read-only, loopback-only inspection of archived routing audit runs.</p>
<span class="tag ok">read only</span> <span class="tag">GET / HEAD</span>
<div class="notice">This interface never connects to a database, never executes SQL against a datastore,
and never moves shard files. It only reads the local SQLite ledger.</div>
<h2>Runs</h2>{table}
<p class="nav"><a href="/api/health">Health</a><a href="/api/runs">Runs API</a></p>
<p class="muted">Boundary: synthetic fixtures, offline verification, dry-run planning only.</p>"""
    return _shell("ShardRoute review desk", body)


def render_run(
    run: dict[str, Any], findings: list[dict[str, Any]], queries: list[dict[str, Any]]
) -> str:
    summary, plan = run["summary"], run["plan"]
    stats = "".join(
        f"<div class=\"stat\"><b>{_e(value)}</b><span>{_e(label)}</span></div>"
        for label, value in (
            ("audit findings", summary.get("audit_findings", 0)),
            ("rows moved", summary.get("rows_moved", 0)),
            ("batches", summary.get("batches", 0)),
            ("blockers", summary.get("blockers", 0)),
            ("queries pruned", summary.get("pruned_queries", 0)),
        )
    )
    table_rows = "".join(
        f"<tr><td>{_e(name)}</td><td>{stats_item.get('logical_rows', 0)}</td>"
        f"<td>{stats_item.get('physical_rows', 0)}</td><td>{stats_item.get('findings', 0)}</td></tr>"
        for name, stats_item in sorted(run.get("tables", {}).items())
    )
    finding_rows = "".join(
        f"<tr><td>{_tag(item['severity'])}</td><td><code>{_e(item['code'])}</code></td>"
        f"<td>{_e(item['table'])}</td><td>{_e(item['shard'] or '-')}</td>"
        f"<td><code>{_e(item['row_id'] or '-')}</code></td><td>{_e(item['message'])}</td></tr>"
        for item in findings
    )
    blockers = "".join(
        f"<li>{_tag(item.get('severity', 'review'))} <code>{_e(item['code'])}</code>: {_e(item['message'])}</li>"
        for item in plan.get("blockers", [])
    ) or "<li class=\"empty\">No blockers.</li>"
    future_blockers = "".join(
        f"<li>{_tag(item.get('severity', 'review'))} <code>{_e(item['code'])}</code>: {_e(item['message'])}</li>"
        for item in plan.get("future_write_blockers", [])
    ) or "<li class=\"empty\">No future-write blockers.</li>"
    certificate = plan.get("compatibility_certificate") or {}
    compat_status = certificate.get("status", "not recorded")
    future_allowed = plan.get("future_writes_allowed")
    permission = plan.get("future_write_permission", "not recorded")
    moves = "".join(
        f"<tr><td>{_e(cell['table'])}</td><td><code>{_e(cell['from_shard'])}</code></td>"
        f"<td><code>{_e(cell['to_shard'])}</code></td><td>{cell['row_count']}</td></tr>"
        for cell in plan.get("move_cells", [])
    )
    cert_rows = "".join(
        f"<tr><td>{_e(record['table'])}</td><td>{_tag(record['status'])}</td>"
        f"<td><code>{_e(record['permission'])}</code></td>"
        f"<td>{len(record.get('counterexamples', []))}</td>"
        f"<td>{_e('; '.join(record.get('unknown_reasons', [])) or '-')}</td></tr>"
        for record in certificate.get("tables", [])
    )
    witness_rows = "".join(
        f"<tr><td>{_e(record['table'])}</td>"
        f"<td><code>{_e(json.dumps(witness['raw_key'], ensure_ascii=False))}</code></td>"
        f"<td><code>{_e(witness['old']['target_shard'])}</code></td>"
        f"<td><code>{_e(witness['new']['target_shard'])}</code></td>"
        f"<td>{_e(witness['reason'])}</td></tr>"
        for record in certificate.get("tables", [])
        for witness in record.get("counterexamples", [])
    )
    query_rows = "".join(
        f"<tr><td>{_e(item['name'])}</td><td>{_e(item['table'])}</td>"
        f"<td><code>{_e(item['decision'])}</code></td>"
        f"<td>{_e(', '.join(item['targeted_shards']) or 'none')}</td><td>{_e(item['reason'])}</td></tr>"
        for item in queries
    )
    artifacts = "".join(
        f"<li><code>{_e(item['path'])}</code> · {_e(item['sha256'][:16])}… · {item['bytes']} bytes</li>"
        for item in run.get("_artifacts", [])
    )
    body = f"""<p class="nav"><a href="/">← all runs</a><a href="/api/runs/{_e(run['run_id'])}">Run JSON</a>
<a href="/api/runs/{_e(run['run_id'])}/findings">Findings JSON</a>
<a href="/api/runs/{_e(run['run_id'])}/plan">Plan JSON</a></p>
<h1>Run <code>{_e(run['run_id'])}</code></h1>
<p>{_tag(run['status'])} <span class="muted">generated {_e(run['generated_at'])} · seed {_e(run['seed'])}</span></p>
<div class="grid">{stats}</div>
<h2>Audit by table</h2><div class="wrap"><table><thead><tr><th>Table</th><th>Logical</th>
<th>Physical</th><th>Findings</th></tr></thead><tbody>{table_rows}</tbody></table></div>
<h2>Findings</h2>
<form method="get" action="/runs/{_e(run['run_id'])}">
<input name="severity" placeholder="severity" size="10"><input name="code" placeholder="code" size="18">
<input name="table" placeholder="table" size="12"><button type="submit">Filter</button>
</form>
<div class="wrap"><table><thead><tr><th>Severity</th><th>Code</th><th>Table</th><th>Shard</th>
<th>row_id</th><th>Message</th></tr></thead><tbody>
{''.join(finding_rows) or '<tr><td colspan="6" class="empty">No findings for this filter.</td></tr>'}
</tbody></table></div>
<h2>Reshard plan (dry run)</h2>
<p>Safe to execute: <strong>{_e('yes' if plan.get('safe_to_execute') else 'no')}</strong> ·
{_e(plan.get('moved_rows', 0))} moved · {_e(plan.get('unchanged_rows', 0))} unchanged</p>
<h3>Blockers</h3><ul>{blockers}</ul>
<h3>Move matrix</h3><div class="wrap"><table><thead><tr><th>Table</th><th>From</th><th>To</th>
<th>Rows</th></tr></thead><tbody>{moves or '<tr><td colspan="4" class="empty">No moves.</td></tr>'}</tbody></table></div>
<h3>Future-key compatibility certificate</h3>
<p>Current rows: safe to execute <strong>{_e('yes' if plan.get('safe_to_execute') else 'no')}</strong>.
Unseen future keys: status <strong>{_e(compat_status)}</strong>, permission
<strong><code>{_e(permission)}</code></strong>, writes allowed
<strong>{_e('yes' if future_allowed else 'no')}</strong>.<br>
<span class="muted">These are independent verdicts; a zero-row move matrix does not prove future-key equivalence.</span></p>
<ul>{future_blockers}</ul>
<div class="wrap"><table><thead><tr><th>Table</th><th>Status</th><th>Permission</th>
<th>Witnesses</th><th>Unknown reason</th></tr></thead><tbody>
{cert_rows or '<tr><td colspan="5" class="empty">No certificate recorded in this run.</td></tr>'}
</tbody></table></div>
<div class="wrap"><table><thead><tr><th>Table</th><th>Raw key</th><th>Old shard</th>
<th>New shard</th><th>Reason</th></tr></thead><tbody>
{witness_rows or '<tr><td colspan="5" class="empty">No replayed counterexamples.</td></tr>'}
</tbody></table></div>
<h2>Queries</h2><div class="wrap"><table><thead><tr><th>Name</th><th>Table</th><th>Decision</th>
<th>Targets</th><th>Reason</th></tr></thead><tbody>{query_rows}</tbody></table></div>
<h2>Artifacts</h2><ul>{artifacts or '<li class="empty">No files recorded.</li>'}</ul>
<div class="notice">Read-only archive view. No migration action can be triggered from this page.</div>"""
    return _shell(f"ShardRoute {run['run_id']}", body)
