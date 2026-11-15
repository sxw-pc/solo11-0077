"""A dependency-free, self-contained HTML renderer for ShardRoute reports."""

from __future__ import annotations

import html
from typing import Any

STYLE = """
:root{color-scheme:light dark;--ink:#17202a;--muted:#5b6770;--line:#d8dee4;--panel:#f6f8fa;
--accent:#0b6bcb;--bad:#a4262c;--warn:#8a5a00;--ok:#1c6b3c;--bg:#ffffff}
@media (prefers-color-scheme:dark){:root{--ink:#e7edf3;--muted:#9aa8b5;--line:#34404b;
--panel:#1b232c;--accent:#63a8ff;--bad:#ff8b8b;--warn:#ffd166;--ok:#7ddba3;--bg:#111820}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:15px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1180px;margin:0 auto;padding:32px 24px 64px}h1{font-size:1.9rem;margin:0 0 4px}
h2{font-size:1.25rem;margin:32px 0 10px;border-bottom:1px solid var(--line);padding-bottom:6px}
h3{font-size:1.02rem;margin:22px 0 8px}p{margin:8px 0}.muted{color:var(--muted)}
.badge{display:inline-block;padding:2px 9px;border-radius:999px;font-weight:700;font-size:.78rem;
border:1px solid currentColor}.pass,.ok{color:var(--ok)}.fail,.critical,.high{color:var(--bad)}
.attention,.medium,.warn{color:var(--warn)}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
gap:10px;margin:14px 0}.stat{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:12px}
.stat b{display:block;font-size:1.45rem}.stat span{color:var(--muted);font-size:.85rem}
table{width:100%;border-collapse:collapse;margin:8px 0 18px;font-size:.9rem}
th,td{text-align:left;vertical-align:top;border-bottom:1px solid var(--line);padding:7px 8px}
th{background:var(--panel);font-size:.78rem;text-transform:uppercase;letter-spacing:.03em}
code,pre{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:.86em}
pre{white-space:pre-wrap;overflow-wrap:anywhere;background:var(--panel);padding:12px;border-radius:8px}
.wrap{overflow-x:auto}.kv{display:grid;grid-template-columns:210px 1fr;gap:4px 14px}
.kv dt{color:var(--muted)}.kv dd{margin:0;overflow-wrap:anywhere}
.notice{background:var(--panel);border-left:4px solid var(--accent);padding:12px 14px;margin:18px 0}
ul,ol{padding-left:24px}li{margin:4px 0}.empty{color:var(--muted);font-style:italic}
a{color:var(--accent)}.anchors{display:flex;gap:12px;flex-wrap:wrap;margin:16px 0 0}
.anchors a{font-size:.9rem}@media print{.anchors{display:none}main{max-width:none}}
"""


def _e(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _badge(value: str) -> str:
    css = value.lower() if value.lower() in {"pass", "fail", "attention", "critical", "high", "medium", "low"} else ""
    return f'<span class="badge {css}">{_e(value)}</span>'


def _kv(items: list[tuple[str, Any]]) -> str:
    return "<dl class=\"kv\">" + "".join(
        f"<dt>{_e(key)}</dt><dd>{_e(value)}</dd>" for key, value in items
    ) + "</dl>"


def render_html(report: dict[str, Any]) -> str:
    summary = report["summary"]
    audit = report["audit"]
    plan = report["plan"]
    find_rows = []
    for finding in audit["findings"]:
        find_rows.append(
            "<tr>"
            f"<td>{_badge(finding['severity'])}</td><td><code>{_e(finding['code'])}</code></td>"
            f"<td>{_e(finding['table'])}</td><td>{_e(finding['shard'] or '-')}</td>"
            f"<td><code>{_e(finding['row_id'] or '-')}</code></td><td>{_e(finding['message'])}</td>"
            "</tr>"
        )
    table_rows = []
    for table, stats in sorted(audit["tables"].items()):
        table_rows.append(
            f"<tr><td>{_e(table)}</td><td>{stats['logical_rows']}</td>"
            f"<td>{stats['physical_rows']}</td><td>{stats['findings']}</td>"
            f"<td>{stats['critical']}</td></tr>"
        )
    move_rows = [
        f"<tr><td>{_e(cell['table'])}</td><td><code>{_e(cell['from_shard'])}</code></td>"
        f"<td><code>{_e(cell['to_shard'])}</code></td><td>{cell['row_count']}</td></tr>"
        for cell in plan["move_cells"]
    ]
    batch_rows = [
        f"<tr><td><code>{_e(batch['batch_id'])}</code></td><td>{_e(batch['phase'])}</td>"
        f"<td>{_e(batch['table'])}</td><td>{_e(batch['source'])}</td>"
        f"<td>{_e(batch['target'])}</td><td>{batch['row_count']}</td></tr>"
        for batch in plan["batches"]
    ]
    blockers = "".join(
        f"<li>{_badge(item['severity'])} <code>{_e(item['code'])}</code> "
        f"({_e(item['table'] or 'all')}): {_e(item['message'])} [count={item['count']}]</li>"
        for item in plan["blockers"]
    ) or "<li class=\"empty\">No blockers. Execution still requires an external change process.</li>"
    rollback = "".join(
        f"<li>{_e(item['action'])} <span class=\"muted\">Verify: {_e(item['verification'])}</span></li>"
        for item in plan["rollback_checklist"]
    )
    query_rows = []
    for query in report["queries"]:
        targets = ", ".join(query["targeted_shards"]) or "none"
        query_rows.append(
            f"<tr><td>{_e(query['name'])}</td><td>{_e(query['table'])}</td>"
            f"<td>{_badge(query['decision'])}</td><td>{_e(targets)}</td>"
            f"<td>{_e(query['reason'])}</td></tr>"
        )
    details = "".join(
        f"<details><summary>{_e(query['name'])} pruning trace</summary>"
        f"<pre>{_e(_json(query['trace']))}</pre></details>"
        for query in report["queries"]
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>ShardRoute report {_e(report['run_id'])}</title>
<style>{STYLE}</style></head>
<body><main>
<header>
<h1>ShardRoute routing verification</h1>
<p class="muted">Offline audit, reshard dry-run and query-pruning explanation · run <code>{_e(report['run_id'])}</code></p>
<p>{_badge(report['status'])} <span class="muted">generated {_e(report['generated_at'])} · seed {_e(report['seed'])}</span></p>
<nav class="anchors"><a href="#summary">Summary</a><a href="#findings">Findings</a>
<a href="#plan">Reshard plan</a><a href="#queries">Queries</a><a href="#rollback">Rollback</a></nav>
</header>
<section id="summary"><h2>Executive summary</h2>
<div class="grid">
<div class="stat"><b>{summary['audit_findings']}</b><span>audit findings</span></div>
<div class="stat"><b>{summary['critical_or_high']}</b><span>critical or high</span></div>
<div class="stat"><b>{summary['rows_moved']}</b><span>logical rows moved</span></div>
<div class="stat"><b>{summary['batches']}</b><span>shadow batches</span></div>
<div class="stat"><b>{summary['blockers']}</b><span>blockers</span></div>
<div class="stat"><b>{summary['pruned_queries']}/{summary['queries']}</b><span>queries pruned</span></div>
</div>
<h3>Configuration fingerprints</h3>
{_kv([('Old topology', report['fingerprints']['topology_old']),
      ('New topology', report['fingerprints']['topology_new']),
      ('Logical snapshot', report['fingerprints']['logical']),
      ('Physical shards', report['fingerprints']['physical'])])}
<div class="wrap"><table><thead><tr><th>Table</th><th>Logical rows</th><th>Physical rows</th>
<th>Findings</th><th>Critical</th></tr></thead><tbody>{''.join(table_rows)}</tbody></table></div>
</section>
<section id="findings"><h2>Findings</h2>
<div class="wrap"><table><thead><tr><th>Severity</th><th>Code</th><th>Table</th>
<th>Shard</th><th>row_id</th><th>Message</th></tr></thead><tbody>
{''.join(find_rows) or '<tr><td colspan="6" class="empty">No findings.</td></tr>'}
</tbody></table></div></section>
<section id="plan"><h2>Reshard plan (dry run)</h2>
<p><strong>{plan['moved_rows']}</strong> rows move; <strong>{plan['unchanged_rows']}</strong> stay.
Safe to execute: <strong>{_e('yes' if plan['safe_to_execute'] else 'no')}</strong>.
No data was moved by this tool.</p>
<h3>Move matrix</h3><div class="wrap"><table><thead><tr><th>Table</th><th>From</th>
<th>To</th><th>Rows</th></tr></thead><tbody>
{''.join(move_rows) or '<tr><td colspan="4" class="empty">No moves.</td></tr>'}
</tbody></table></div>
<h3>Batch plan</h3><div class="wrap"><table><thead><tr><th>Batch</th><th>Phase</th>
<th>Table</th><th>Source</th><th>Target</th><th>Rows</th></tr></thead><tbody>
{''.join(batch_rows) or '<tr><td colspan="6" class="empty">No batches.</td></tr>'}
</tbody></table></div>
<h3>Blockers</h3><ul>{blockers}</ul>
</section>
<section id="queries"><h2>Query pruning</h2>
<div class="wrap"><table><thead><tr><th>Query</th><th>Table</th><th>Decision</th>
<th>Target shards</th><th>Reason</th></tr></thead><tbody>{''.join(query_rows)}</tbody></table></div>
{details}
</section>
<section id="rollback"><h2>Rollback checklist</h2><ol>{rollback}</ol></section>
<section><h2>Boundary notice</h2>
<div class="notice">{_e(report['disclaimer'])}</div>
<p class="muted">Reproduce: <code>python -m shardroute demo --workspace .shardroute-demo</code></p>
</section>
</main></body></html>
"""


def _json(value: Any) -> str:
    import json

    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False)
