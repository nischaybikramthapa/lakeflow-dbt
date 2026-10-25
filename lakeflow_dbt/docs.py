"""`lfd docs generate` — static lineage site from compiled manifest(s)."""
from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Dict, List


def load_manifests(target: Path) -> Dict[str, dict]:
    """manifest.json under target/, plus target/pipelines/*/manifest.json."""
    out: Dict[str, dict] = {}
    f = target / "manifest.json"
    if f.is_file():
        m = json.loads(f.read_text())
        out[m.get("project", "default")] = m
    for f in sorted((target / "pipelines").glob("*/manifest.json")):
        m = json.loads(f.read_text())
        out[f.parent.name] = m
    return out


def _node_card(name: str, n: dict, pipe: str) -> str:
    desc = html.escape(n.get("description") or "")
    cols = "".join(
        f"<tr><td><code>{html.escape(c)}</code></td>"
        f"<td>{html.escape((v or {}).get('description') or '')}</td>"
        f"<td>{html.escape(', '.join((v or {}).get('tests') or []))}</td></tr>"
        for c, v in (n.get("columns") or {}).items()
    )
    cols = f"<table><tr><th>column</th><th>description</th><th>tests</th></tr>{cols}</table>" if cols else ""
    tags = " ".join(f"<span class=tag>{html.escape(t)}</span>" for t in n.get("tags") or [])
    srcs = ", ".join(html.escape(s) for s in n.get("sources") or [])
    return (
        f"<div class=node id={pipe}-{name}>"
        f"<h3>{html.escape(name)} <span class=pipe>[{pipe}]</span></h3>"
        f"<p class=meta>{n.get('materialization','')} · {n.get('resource_type','')} · "
        f"<code>{html.escape(n.get('dataset',''))}</code> · {html.escape(n.get('path',''))}</p>"
        f"<p>{desc}</p>{tags}"
        f"<p class=meta>upstream: {html.escape(', '.join(n.get('refs') or []) or '—')}"
        f"{' · sources: ' + srcs if srcs else ''}</p>{cols}</div>"
    )


def generate(target: Path, out_dir: Path) -> Path:
    manifests = load_manifests(target)
    if not manifests:
        raise FileNotFoundError(f"no manifest.json under {target} — run `lfd compile` first")
    cards, edges, nodes_seen = [], [], set()
    for pipe, m in manifests.items():
        for name, n in sorted(m["nodes"].items()):
            nodes_seen.add(name)
            cards.append(_node_card(name, n, pipe))
            for r in n.get("refs") or []:
                edges.append(f"{html.escape(r)} → {html.escape(name)}")
    edge_list = "\n".join(f"<li><code>{e}</code></li>" for e in sorted(set(edges)))
    page = f"""<!doctype html><meta charset=utf-8><title>lakeflow-dbt docs</title>
<style>
 body{{font:14px/1.5 system-ui;max-width:960px;margin:2rem auto;padding:0 1rem}}
 h1{{margin-bottom:0}} .sub{{color:#666;margin-top:0}}
 .node{{border:1px solid #ddd;border-radius:8px;padding:.6rem 1rem;margin:.6rem 0}}
 .meta{{color:#777;font-size:12px}} .pipe{{color:#4a6;font-size:12px}}
 .tag{{background:#eef;border-radius:4px;padding:0 6px;font-size:12px}}
 table{{border-collapse:collapse}} td,th{{border:1px solid #ddd;padding:2px 8px;text-align:left}}
 input{{width:100%;padding:6px;margin:1rem 0}} code{{background:#f6f6f6;padding:1px 4px}}
</style>
<h1>{html.escape(next(iter(manifests.values())).get('project','project'))} — docs</h1>
<p class=sub>{sum(len(m['nodes']) for m in manifests.values())} nodes across {len(manifests)} pipeline(s)</p>
<input id=q placeholder=filter… onkeyup="for(const d of document.querySelectorAll('.node'))d.style.display=d.textContent.toLowerCase().includes(this.value.toLowerCase())?'':'none'">
<h2>Lineage</h2><ul>{edge_list}</ul>
<h2>Nodes</h2>{''.join(cards)}
"""
    out_dir.mkdir(parents=True, exist_ok=True)
    f = out_dir / "index.html"
    f.write_text(page)
    return f
