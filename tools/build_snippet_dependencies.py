#!/usr/bin/env python3
"""
Parse snippets for class/function names + imports, build a dependency graph,
and suggest which snippets belong together.
Read-only. No code execution.
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

DEFAULT_SNIPPET_ROOT = "extracted_snippets_v2"
PARSEABLE_EXTENSIONS = (".py", ".js", ".jsx", ".ts", ".tsx", ".rs", ".cpp", ".c", ".hpp", ".h")


def _parse_table_line(line: str) -> list[str]:
    if not line.strip().startswith("|"):
        return []
    placeholder = "\x00PIPE\x00"
    line = line.replace("\\|", placeholder)
    parts = [p.strip().replace(placeholder, "|") for p in line.split("|")]
    return parts[1:-1] if len(parts) >= 2 else []


def _parse_snippet_index(index_path: Path) -> list[dict]:
    text = index_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    rows, headers = [], None
    for line in lines:
        cells = _parse_table_line(line)
        if not cells:
            continue
        if headers is None and cells[0] == "snippet_title":
            headers = cells
            continue
        if headers and len(cells) >= len(headers):
            row = {h: cells[i] if i < len(cells) else "" for i, h in enumerate(headers)}
            rows.append(row)
    return rows


# --- Parsers (regex-based, minimal) ---

def _parse_python(content: str) -> dict:
    out = {"imports": [], "imported_symbols": [], "classes": [], "functions": []}
    for line in content.splitlines():
        line = line.strip()
        if m := re.match(r"^import\s+(.+)$", line):
            mods = [x.strip().split(" as ")[0] for x in m.group(1).split(",")]
            out["imports"].extend(mods)
        elif m := re.match(r"^from\s+([\w.]+)\s+import\s+(.+)$", line):
            mod = m.group(1)
            out["imports"].append(mod)
            syms = [x.strip().split(" as ")[0] for x in re.split(r"[\s,]+", m.group(2)) if x and x != "("]
            out["imported_symbols"].extend([(mod, s) for s in syms if s and s != ")"])
        elif m := re.match(r"^class\s+(\w+)", line):
            out["classes"].append(m.group(1))
        elif m := re.match(r"^def\s+(\w+)\s*\(", line):
            out["functions"].append(m.group(1))
    return out


def _parse_js_ts(content: str) -> dict:
    out = {"imports": [], "imported_symbols": [], "classes": [], "functions": []}
    for line in content.splitlines():
        line = line.strip()
        if m := re.search(r"import\s+(?:(\w+)|(\{[^}]+\}|[\*]))\s+from\s+['\"]([^'\"]+)['\"]", line):
            mod = m.group(3)
            out["imports"].append(mod)
            if m.group(1):
                out["imported_symbols"].append((mod, m.group(1)))
            elif m.group(2):
                for s in re.split(r"[\s,]+", m.group(2).strip("{}")):
                    s = s.strip().split(" as ")[0]
                    if s and s != "*":
                        out["imported_symbols"].append((mod, s))
        elif m := re.search(r"require\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", line):
            out["imports"].append(m.group(1))
        elif m := re.match(r"^class\s+(\w+)", line):
            out["classes"].append(m.group(1))
        elif m := re.match(r"^(?:export\s+)?(?:async\s+)?function\s+(\w+)", line):
            out["functions"].append(m.group(1))
        elif m := re.match(r"^(?:export\s+)?(?:async\s+)?function\s*\*\s*(\w+)", line):
            out["functions"].append(m.group(1))
        elif m := re.match(r"^(?:export\s+)?const\s+(\w+)\s*=\s*(?:async\s+)?\(", line):
            out["functions"].append(m.group(1))
    return out


def _parse_rust(content: str) -> dict:
    out = {"imports": [], "imported_symbols": [], "classes": [], "functions": []}
    for line in content.splitlines():
        line = line.strip()
        if m := re.match(r"^use\s+([\w:]+)(?:::\s*\{([^}]+)\}|::\s*(\w+))?", line):
            mod = m.group(1)
            out["imports"].append(mod)
            if m.group(2):
                for s in re.split(r"[\s,]+", m.group(2)):
                    s = s.strip().split(" as ")[0]
                    if s and s != "*":
                        out["imported_symbols"].append((mod, s))
            elif m.group(3):
                out["imported_symbols"].append((mod, m.group(3)))
        elif m := re.match(r"^pub\s+struct\s+(\w+)", line) or re.match(r"^struct\s+(\w+)", line):
            out["classes"].append(m.group(1))
        elif m := re.match(r"^pub\s+fn\s+(\w+)", line) or re.match(r"^fn\s+(\w+)", line):
            out["functions"].append(m.group(1))
    return out


def _parse_cpp(content: str) -> dict:
    out = {"imports": [], "imported_symbols": [], "classes": [], "functions": []}
    for line in content.splitlines():
        line = line.strip()
        if m := re.match(r'#include\s+[<"]([^>"]+)[>"]', line):
            out["imports"].append(m.group(1))
        elif m := re.match(r"^class\s+(\w+)", line) or re.match(r"^struct\s+(\w+)", line):
            out["classes"].append(m.group(1))
        else:
            m = re.match(r"^\w+(?:\s+\w+)*\s+(\w+)\s*\(", line)
            if m and ";" not in line[:40]:
                out["functions"].append(m.group(1))
    return out


def _parse_snippet(content: str, ext: str) -> dict:
    ext = ext.lower()
    if ext in (".py",):
        return _parse_python(content)
    if ext in (".js", ".jsx", ".ts", ".tsx"):
        return _parse_js_ts(content)
    if ext in (".rs",):
        return _parse_rust(content)
    if ext in (".cpp", ".c", ".hpp", ".h"):
        return _parse_cpp(content)
    return {"imports": [], "imported_symbols": [], "classes": [], "functions": []}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build snippet dependency graph and suggest clusters")
    parser.add_argument("--snippet-root", default=DEFAULT_SNIPPET_ROOT, help="Path to extracted_snippets_v2")
    args = parser.parse_args()
    root = Path(args.snippet_root).resolve()
    out_dir = root / "reports"
    index_path = root / "reports" / "SNIPPET_INDEX.md"

    if not root.exists() or not index_path.exists():
        print(f"Error: {root} or {index_path} not found", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    rows = _parse_snippet_index(index_path)
    processed = [r for r in rows if r.get("status") in ("RECONSTRUCTED", "CONFLICT", "LOOSE") and r.get("output_file")]

    # Build symbol index: (module, symbol) -> [snippet_ids that define it]
    symbol_definers: dict[tuple[str, str], list[str]] = defaultdict(list)
    snippet_id_to_defs: dict[str, dict] = {}
    snippet_id_to_meta: dict[str, dict] = {}
    module_to_snippets: dict[str, list[str]] = defaultdict(list)

    for row in processed:
        output_file = row.get("output_file", "").strip()
        if not output_file:
            continue
        file_path = root / output_file
        ext = file_path.suffix.lower()
        if ext not in PARSEABLE_EXTENSIONS:
            continue
        try:
            content = file_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        sid = output_file
        meta = {
            "title": row.get("snippet_title", ""),
            "status": row.get("status", ""),
            "path": row.get("path", ""),
            "source_md": row.get("source_md", ""),
            "language": row.get("language", ""),
        }
        snippet_id_to_meta[sid] = meta

        parsed = _parse_snippet(content, ext)
        snippet_id_to_defs[sid] = parsed

        for mod in parsed["imports"]:
            module_to_snippets[mod].append(sid)

        for cls in parsed["classes"]:
            symbol_definers[(meta.get("path", "").split("/")[-1].replace(".py", "") or "unknown", cls)].append(sid)
        for fn in parsed["functions"]:
            symbol_definers[(meta.get("path", "").split("/")[-1].replace(".py", "") or "unknown", fn)].append(sid)

    # Build dependency graph: snippet -> { imports: [...], defines: {...}, depends_on: [snippet_ids] }
    graph = {}
    for sid, parsed in snippet_id_to_defs.items():
        meta = snippet_id_to_meta.get(sid, {})
        depends_on = []
        for mod, sym in parsed.get("imported_symbols", []):
            if mod.startswith(".") or "/" in mod:
                mod = mod.lstrip(".").replace("/", ".")
            mod_base = mod.split(".")[0] if "." in mod else mod
            for (def_mod, def_sym), definers in symbol_definers.items():
                if def_sym == sym or (def_mod in mod or mod_base in def_mod):
                    depends_on.extend(definers)
        graph[sid] = {
            "imports": list(set(parsed.get("imports", []))),
            "imported_symbols": parsed.get("imported_symbols", []),
            "classes": parsed.get("classes", []),
            "functions": parsed.get("functions", []),
            "depends_on": list(set(d for d in depends_on if d != sid)),
            "meta": meta,
        }

    # --- Suggest clusters ---
    clusters = []

    # Cluster 1: Shared import module
    for mod, sids in sorted(module_to_snippets.items(), key=lambda x: -len(x[1])):
        if len(sids) >= 2:
            clusters.append({
                "reason": "shared_import",
                "module": mod,
                "snippets": sorted(set(sids))[:50],
                "count": len(set(sids)),
            })

    # Cluster 2: Same source_md (co-located in docs)
    source_to_snippets: dict[str, list[str]] = defaultdict(list)
    for sid, meta in snippet_id_to_meta.items():
        src = meta.get("source_md", "")
        if src:
            source_to_snippets[src].append(sid)
    for src, sids in source_to_snippets.items():
        if len(sids) >= 2:
            clusters.append({
                "reason": "same_source",
                "source_md": src,
                "snippets": sorted(sids)[:50],
                "count": len(sids),
            })

    # Cluster 3: Provider-consumer (A defines X, B imports X)
    provider_consumer = []
    for sid, data in graph.items():
        for dep in data.get("depends_on", []):
            if dep != sid:
                provider_consumer.append((dep, sid))
    if provider_consumer:
        clusters.append({
            "reason": "provider_consumer",
            "pairs": provider_consumer[:100],
            "count": len(provider_consumer),
        })

    # Cluster 4: Same reconstructed path
    path_to_snippets: dict[str, list[str]] = defaultdict(list)
    for sid, meta in snippet_id_to_meta.items():
        p = meta.get("path", "")
        if p and meta.get("status") in ("RECONSTRUCTED", "CONFLICT"):
            path_to_snippets[p].append(sid)
    for p, sids in path_to_snippets.items():
        if len(sids) >= 2:
            clusters.append({
                "reason": "same_path",
                "path": p,
                "snippets": sorted(sids),
                "count": len(sids),
            })

    # --- Write outputs ---
    graph_out = {
        "nodes": {sid: {"id": sid, **d} for sid, d in graph.items()},
        "clusters": clusters,
    }
    with open(out_dir / "DEPENDENCY_GRAPH.json", "w", encoding="utf-8") as f:
        json.dump(graph_out, f, indent=2)

    # SNIPPET_CLUSTERS.md
    with open(out_dir / "SNIPPET_CLUSTERS.md", "w", encoding="utf-8") as f:
        f.write("# Snippet Clusters — These Snippets Belong Together\n\n")
        f.write("## By shared import module\n\n")
        for c in sorted([x for x in clusters if x["reason"] == "shared_import"], key=lambda x: -x["count"])[:30]:
            f.write(f"### `{c['module']}` ({c['count']} snippets)\n\n")
            for s in c["snippets"][:15]:
                f.write(f"- `{s}` — {snippet_id_to_meta.get(s, {}).get('title', '')[:50]}\n")
            if len(c["snippets"]) > 15:
                f.write(f"- ... and {len(c['snippets']) - 15} more\n")
            f.write("\n")

        f.write("## By same source document\n\n")
        for c in sorted([x for x in clusters if x["reason"] == "same_source"], key=lambda x: -x["count"])[:20]:
            f.write(f"### `{c['source_md']}` ({c['count']} snippets)\n\n")
            for s in c["snippets"][:10]:
                f.write(f"- `{s}`\n")
            if len(c["snippets"]) > 10:
                f.write(f"- ... and {len(c['snippets']) - 10} more\n")
            f.write("\n")

        f.write("## By same reconstructed path\n\n")
        for c in sorted([x for x in clusters if x["reason"] == "same_path"], key=lambda x: -x["count"])[:20]:
            f.write(f"### `{c['path']}` ({c['count']} snippets)\n\n")
            for s in c["snippets"][:10]:
                f.write(f"- `{s}`\n")
            f.write("\n")

        f.write("## Provider-consumer pairs (defines → imports)\n\n")
        pc = next((x for x in clusters if x["reason"] == "provider_consumer"), None)
        if pc:
            for a, b in pc.get("pairs", [])[:30]:
                f.write(f"- `{a}` → `{b}`\n")

    print(f"Parsed {len(graph)} snippets, {len(clusters)} cluster types")
    return 0


if __name__ == "__main__":
    sys.exit(main())
