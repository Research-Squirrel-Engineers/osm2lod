#!/usr/bin/env python3
"""
Extract OSM IDs as Integers for osm2lod
========================================
Reads SISAL-OSM matching results and outputs clean integer OSM IDs.
"""

import pandas as pd
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
CSV_FILE = SCRIPT_DIR / "output" / "sisal_osm_matches.csv"
OUTPUT_DIR = SCRIPT_DIR / "output"

print("=" * 70)
print("OSM ID EXTRACTOR (for osm2lod)")
print("=" * 70)

# Load results
df = pd.read_csv(CSV_FILE, encoding="utf-8-sig")

# Detect correct column name (old vs new CSV format)
if "has_osm_match" in df.columns:
    match_col = "has_osm_match"
elif "matched" in df.columns:
    match_col = "matched"
else:
    print("❌ Error: Could not find match column in CSV")
    print(f"   Available columns: {list(df.columns)}")
    exit(1)

matched = df[df[match_col] == True]

print(f"\n✓ Loaded {len(matched)} matched sites from {len(df)} total")

# Extract OSM IDs
osm_ids = []

for _, row in matched.iterrows():
    osm_id_str = str(row["osm_id"]).strip()

    # Handle different formats:
    # - "node/123456" → 123456
    # - "123456.0" → 123456
    # - "123456" → 123456

    if "/" in osm_id_str:
        osm_id_str = osm_id_str.split("/")[-1]

    try:
        osm_id = int(float(osm_id_str))
        osm_ids.append(osm_id)
    except ValueError:
        print(f"Warning: Could not parse OSM ID: {row['osm_id']}")

# Remove duplicates and sort
osm_ids = sorted(set(osm_ids))

print(f"✓ Extracted {len(osm_ids)} unique OSM node IDs")

# ============================================================================
# OUTPUT 1: Plain text file (one ID per line)
# ============================================================================

txt_file = OUTPUT_DIR / "osm_ids.txt"
with open(txt_file, "w", encoding="utf-8") as f:
    for osm_id in osm_ids:
        f.write(f"{osm_id}\n")

print(f"\n✓ Saved to: {txt_file}")
print(f"  Format: One integer per line")
print(f"  Total: {len(osm_ids)} IDs")

# ============================================================================
# OUTPUT 2: Overpass Query (for osm2lod)
# ============================================================================

query_file = OUTPUT_DIR / "overpass_query_osm2lod.txt"
query = "[out:json][timeout:25];\n(\n"
for osm_id in osm_ids:
    query += f"  node({osm_id});\n"
query += ");\nout meta geom;"

with open(query_file, "w", encoding="utf-8") as f:
    f.write(query)

print(f"\n✓ Saved to: {query_file}")
print(f"  Format: Overpass QL (node IDs as integers)")

# ============================================================================
# OUTPUT 3: Python list (for copy-paste)
# ============================================================================

py_file = OUTPUT_DIR / "osm_ids_list.py"
with open(py_file, "w", encoding="utf-8") as f:
    f.write("# OSM Node IDs (integers) from SISAL-OSM matching\n")
    f.write("# Generated for osm2lod pipeline\n\n")
    f.write("osm_node_ids = [\n")
    for i, osm_id in enumerate(osm_ids):
        f.write(f"    {osm_id}")
        if i < len(osm_ids) - 1:
            f.write(",")
        f.write("\n")
    f.write("]\n")

print(f"\n✓ Saved to: {py_file}")
print(f"  Format: Python list")

# ============================================================================
# OUTPUT 4: JSON array
# ============================================================================

import json

json_file = OUTPUT_DIR / "osm_ids.json"
with open(json_file, "w", encoding="utf-8") as f:
    json.dump(osm_ids, f, indent=2)

print(f"\n✓ Saved to: {json_file}")
print(f"  Format: JSON array")

# ============================================================================
# PREVIEW
# ============================================================================

print(f"\n{'='*70}")
print("PREVIEW (first 20 IDs)")
print("=" * 70)
for i, osm_id in enumerate(osm_ids[:20], 1):
    print(f"{i:3d}. {osm_id}")

if len(osm_ids) > 20:
    print(f"... ({len(osm_ids)-20} more)")

print(f"\n{'='*70}")
print("USAGE IN osm2lod")
print("=" * 70)
print("\n1. Use osm_ids.txt for batch processing:")
print("   cat output/osm_ids.txt | xargs -I {} osm2lod --node {}")

print("\n2. Use overpass_query_osm2lod.txt for Overpass API:")
print("   curl -X POST https://overpass-api.de/api/interpreter \\")
print("        --data @output/overpass_query_osm2lod.txt")

print("\n3. Import Python list:")
print("   from osm_ids_list import osm_node_ids")

print("\n4. Load JSON:")
print("   import json")
print("   with open('output/osm_ids.json') as f:")
print("       ids = json.load(f)")

print(f"\n{'='*70}")
print("DONE!")
print("=" * 70)
