#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
merge_ttl.py
────────────
Merges all osm2lod TTL export files in the same directory into a single
combined TTL file: osm2lod_combined.ttl

Usage:
    python merge_ttl.py

The script looks for files matching osm_export_*.ttl in its own directory
and writes the merged graph to the same directory.

Requirements:
    pip install rdflib
"""

from pathlib import Path
from rdflib import ConjunctiveGraph, Dataset

# ── Configuration ─────────────────────────────────────────────────────────────

SCRIPT_DIR   = Path(__file__).parent
INPUT_GLOB   = "osm_export_*.ttl"
OUTPUT_FILE  = SCRIPT_DIR / "osm2lod_combined.ttl"
OUTPUT_FORMAT = "turtle"   # alternatives: "n3", "nt", "xml"

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ttl_files = sorted(SCRIPT_DIR.glob(INPUT_GLOB))

    if not ttl_files:
        print(f"❌  No files matching '{INPUT_GLOB}' found in {SCRIPT_DIR}")
        return

    print(f"📂  Found {len(ttl_files)} TTL file(s):")
    for f in ttl_files:
        print(f"    {f.name}")

    # Use a plain Graph so all triples end up in one default graph
    from rdflib import Graph
    combined = Graph()

    for path in ttl_files:
        g = Graph()
        g.parse(path, format="turtle")
        triple_count = len(g)
        combined += g
        print(f"    ✔  {path.name:55s} {triple_count:6d} triples")

    print()
    print(f"   Total triples : {len(combined)}")

    combined.serialize(OUTPUT_FILE, format=OUTPUT_FORMAT)
    size_kb = OUTPUT_FILE.stat().st_size / 1024
    print(f"✅  Written to   : {OUTPUT_FILE.name}  ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
