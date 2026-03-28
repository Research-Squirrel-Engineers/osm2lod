#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test-Skript: Diff-QuickStatements Generator

Testet die Diff-Generierung gegen Wikibase für Holy Wells
Schreibt Output in test/ Ordner
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests


# =================================================
# Report Logger
# =================================================


class ReportLogger:
    """Logs console output to both terminal and file."""

    def __init__(self, report_path: Path):
        self.report_path = report_path
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(report_path, "w", encoding="utf-8")
        self.terminal = sys.stdout

    def write(self, message):
        self.terminal.write(message)
        self.file.write(message)

    def flush(self):
        self.terminal.flush()
        self.file.flush()

    def close(self):
        self.file.close()

    def __enter__(self):
        sys.stdout = self
        return self

    def __exit__(self, *args):
        sys.stdout = self.terminal
        self.close()


# =================================================
# Configuration
# =================================================

# Wikibase SPARQL Endpoint
WIKIBASE_SPARQL_ENDPOINT = "https://osm2wiki.wikibase.cloud/query/sparql"
WIKIBASE_ENTITY_PREFIX = "https://osm2wiki.wikibase.cloud/entity/"

# Export types to test (mit Query Item QIDs)
TEST_EXPORTS = {
    "ogham": "Q24",
    "holywells": "Q25",
    "ci": "Q26",
    "drillcores": "Q27",
    "benchmarks": "Q890",
    "sisal": "Q894",
    "romansites": "Q895",
    "hogbacks": "Q897",
}

# Gleiche Konstante für CREATE statements
P10_QUERY_ITEM = TEST_EXPORTS

# Input/Output paths
DIST_BASE_DIR = Path("dist")


# Latest run directory (automatisch finden)
def find_latest_run_dir() -> Optional[Path]:
    """Findet den neuesten Run-Ordner (höchstes Datum)."""
    if not DIST_BASE_DIR.exists():
        return None

    run_dirs = []
    for p in DIST_BASE_DIR.iterdir():
        if p.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", p.name):
            run_dirs.append(p)

    if not run_dirs:
        return None

    # Sortiere und nimm den neuesten
    run_dirs.sort(key=lambda x: x.name)
    return run_dirs[-1]


# Property Mappings
STATIC_PROPERTIES = {"P1", "P4", "P10", "P11"}  # Niemals ändern
ALWAYS_UPDATE = {"P12"}  # Immer updaten
CONDITIONAL_UPDATE = {
    "P13": "version",
    "P16": "changeset",
    "P17": "timestamp",
    "P5": "coordinates",
    "P6": "wikidata",
    "P7": "wikipedia",
    "P8": "external_link",
    "P9": "tags",
}


# =================================================
# Helper Functions
# =================================================


def clean_value(v: Any) -> Optional[str]:
    """Bereinigt Werte (entfernt NaN, None, etc.)"""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    s = str(v).strip()
    if not s or s.lower() in {"nan", "na", "n/a", "null", "none", "nil", "-"}:
        return None
    return s


def normalize_wikipedia(value: str) -> str:
    """
    Normalisiert Wikipedia-Werte für Vergleich.

    OSM: 'en:Article_Name'
    Wikibase: 'https://en.wikipedia.org/wiki/Article_Name'

    Beide werden zu: 'https://en.wikipedia.org/wiki/Article_Name'
    """
    if not value:
        return ""

    # URL-decode falls nötig (%23 → #, %20 → space, etc.)
    from urllib.parse import unquote

    value = unquote(value)

    # Wenn schon volle URL → beibehalten
    if value.startswith("http"):
        return value

    # Wenn OSM-Format "en:Article" → konvertiere zu voller URL
    if ":" in value and not value.startswith("http"):
        parts = value.split(":", 1)
        if len(parts) == 2:
            lang = parts[0]
            article = parts[1].replace(" ", "_")
            return f"https://{lang}.wikipedia.org/wiki/{article}"

    return value


def qs_escape(s: str) -> str:
    """Escaped Strings für QuickStatements (Quotes verdoppeln)."""
    return s.replace('"', '""')


# =================================================
# Wikibase SPARQL Query
# =================================================


def fetch_wikibase_items(export_type: str, query_item_qid: str) -> List[Dict[str, Any]]:
    """
    Holt alle Items des Export-Typs aus der Wikibase via SPARQL.

    Returns:
        Liste von Dicts mit: {qid, osm_id, osm_type, label, description, ...}
    """

    sparql_query = f"""
PREFIX osmwd: <https://osm2wiki.wikibase.cloud/entity/>
PREFIX osmwdt: <https://osm2wiki.wikibase.cloud/prop/direct/>
PREFIX osmpq: <https://osm2wiki.wikibase.cloud/prop/qualifier/>

SELECT ?item ?itemLabel ?itemDescription 
       ?osmid ?osmtype ?geo ?version ?osmchangeset ?osmtimestamp
       ?wikidataid ?wikipedia ?osmurl ?tags
WHERE {{ 
  ?item osmwdt:P3 ?osmid .
  ?item osmwdt:P4 ?osmtype .
  ?item osmwdt:P5 ?geo .
  ?item osmwdt:P11 ?osmurl .
  ?item osmwdt:P13 ?version .
  ?item osmwdt:P16 ?osmchangeset .
  
  OPTIONAL {{ ?item osmwdt:P6 ?wikidataid . }}
  OPTIONAL {{ ?item osmwdt:P7 ?wikipedia . }}
  OPTIONAL {{ ?item osmwdt:P9 ?tags . }}
  OPTIONAL {{ ?item osmwdt:P17 ?osmtimestamp . }}
  
  # Filter nach Export-Typ
  ?item osmwdt:P10 osmwd:{query_item_qid} .
  
  SERVICE wikibase:label {{ 
    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". 
  }} 
}}
"""

    print(f"🔍 Querying Wikibase for {export_type} items (Q10={query_item_qid})...")

    try:
        response = requests.post(
            WIKIBASE_SPARQL_ENDPOINT,
            data={"query": sparql_query},
            headers={"Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        results = []
        bindings = data.get("results", {}).get("bindings", [])

        print(f"✅ Found {len(bindings)} bindings in Wikibase")

        # Gruppiere nach Item (wegen potentieller multi-value fields)
        items_dict: Dict[str, Dict[str, Any]] = {}

        for binding in bindings:
            qid = binding["item"]["value"].split("/")[-1]

            if qid not in items_dict:
                # Extrahiere OSM Type aus osmtype URI
                osmtype_uri = binding.get("osmtype", {}).get("value", "")
                osmtype_label = osmtype_uri.split("/")[
                    -1
                ]  # Q5 → wird später zu "node" gemappt

                # Map Q5/Q6/Q7 zu node/way/relation
                type_map = {"Q5": "node", "Q6": "way", "Q7": "relation"}
                osm_type = type_map.get(osmtype_label, osmtype_label)

                # Parse Koordinaten aus WKT Format
                # Wikibase: "Point(-7.9671344 54.0549481)" → "54.0549481/-7.9671344"
                geo_wkt = binding.get("geo", {}).get("value", "")
                coordinates = ""
                if geo_wkt:
                    # Extract lon, lat from "Point(lon lat)"
                    import re

                    match = re.match(r"Point\(([^ ]+) ([^ ]+)\)", geo_wkt)
                    if match:
                        lon, lat = match.groups()
                        coordinates = f"{lat}/{lon}"  # lat/lon format wie in OSM
                    else:
                        coordinates = geo_wkt  # Fallback

                items_dict[qid] = {
                    "qid": qid,
                    "osm_id": int(binding["osmid"]["value"]),
                    "osm_type": osm_type,
                    "label": binding.get("itemLabel", {}).get("value", ""),
                    "description": binding.get("itemDescription", {}).get("value", ""),
                    "coordinates": coordinates,
                    "version": int(binding.get("version", {}).get("value", 0)),
                    "changeset": int(binding.get("osmchangeset", {}).get("value", 0)),
                    "osm_timestamp": binding.get("osmtimestamp", {}).get("value", ""),
                    "wikidata": binding.get("wikidataid", {}).get("value", ""),
                    "wikipedia": binding.get("wikipedia", {}).get("value", ""),
                    "osm_url": binding.get("osmurl", {}).get("value", ""),
                    "tags": [],
                }

            # Sammle Tags (multi-value)
            if "tags" in binding:
                tag_value = binding["tags"]["value"]
                if tag_value and tag_value not in items_dict[qid]["tags"]:
                    items_dict[qid]["tags"].append(tag_value)

        results = list(items_dict.values())
        print(f"✅ Processed {len(results)} unique items from Wikibase")

        return results

    except Exception as e:
        print(f"❌ Error querying Wikibase: {e}")
        return []


# =================================================
# OSM CSV Parser
# =================================================


def load_osm_csv(csv_path: Path) -> pd.DataFrame:
    """Lädt die OSM Export CSV."""
    print(f"📂 Loading OSM CSV: {csv_path.name}")
    df = pd.read_csv(csv_path)
    print(f"✅ Loaded {len(df)} OSM items")
    return df


def parse_osm_item(row: pd.Series) -> Dict[str, Any]:
    """Konvertiert eine CSV-Zeile in ein Dict."""

    # Sammle alle tag:* Spalten
    tags = []
    for col in row.index:
        if col.startswith("tag:") and col not in [
            "tag:name",
            "tag:wikidata",
            "tag:wikipedia",
        ]:
            val = clean_value(row.get(col))
            if val:
                key = col.replace("tag:", "")
                tags.append(f"{key}={val}")

    return {
        "osm_id": int(row["id"]),
        "osm_type": row["type"],
        "label": clean_value(row.get("tag:name")) or "Unnamed",
        "coordinates": f"{row['lat']}/{row['lon']}",
        "version": int(row["version"]) if pd.notna(row.get("version")) else 0,
        "changeset": int(row["changeset"]) if pd.notna(row.get("changeset")) else 0,
        "osm_timestamp": row.get("timestamp", ""),
        "wikidata": clean_value(row.get("tag:wikidata")) or "",
        "wikipedia": clean_value(row.get("tag:wikipedia")) or "",
        "tags": tags,
    }


# =================================================
# Diff Generator
# =================================================


def generate_diff_quickstatements(
    osm_items: List[Dict[str, Any]],
    wikibase_items: List[Dict[str, Any]],
    export_type: str,
    output_path: Path,
) -> Tuple[int, int]:
    """
    Generiert Diff-QuickStatements: OSM vs Wikibase.

    Returns:
        (added_count, modified_count)
    """

    # Index: "type/id" -> item
    wb_index: Dict[str, Dict[str, Any]] = {}
    for item in wikibase_items:
        key = f"{item['osm_type']}/{item['osm_id']}"
        wb_index[key] = item

    osm_index: Dict[str, Dict[str, Any]] = {}
    for item in osm_items:
        key = f"{item['osm_type']}/{item['osm_id']}"
        osm_index[key] = item

    # Finde Änderungen
    osm_keys = set(osm_index.keys())
    wb_keys = set(wb_index.keys())

    added_keys = osm_keys - wb_keys
    deleted_keys = wb_keys - osm_keys
    common_keys = osm_keys & wb_keys

    print(f"\n📊 Diff Analysis:")
    print(f"   ✅ Added: {len(added_keys)}")
    print(f"   📝 Common (potential updates): {len(common_keys)}")
    print(f"   🗑️  Deleted: {len(deleted_keys)}")

    # Generiere QuickStatements
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    lines = [
        "# ===============================================",
        f"# DIFF QuickStatements: {export_type}",
        f"# Generated: {timestamp}",
        f"# Wikibase Query: {timestamp}",
        "# ",
        "# Summary:",
        f"#   - {len(added_keys)} ADDED (CREATE)",
        f"#   - {len(common_keys)} POTENTIAL UPDATES (checking...)",
        f"#   - {len(deleted_keys)} DELETED (ignored)",
        "# ===============================================",
        "",
    ]

    modified_count = 0

    # ADDED Items - vollständige CREATE statements wie in normaler QS
    if added_keys:
        lines.append("# ----------------")
        lines.append(f"# ADDED ITEMS ({len(added_keys)})")
        lines.append("# ----------------")
        lines.append("")

        for key in sorted(added_keys):
            item = osm_index[key]

            # CREATE mit allen Properties
            lines.append("CREATE")
            lines.append(f'LAST|Len|"{qs_escape(item["label"])}"')
            lines.append(f'LAST|Den|"OSM import snapshot ({export_type}) – {key}"')

            # P1 (instance of) - spezifisch für Export-Typ
            if export_type == "drillcores":
                # Maar vs Crater Lake Logik
                is_maar = any(
                    tag.lower().find("maar") >= 0 for tag in item.get("tags", [])
                )
                p1_val = "Q20" if is_maar else "Q17"
                lines.append(f"LAST|P1|{p1_val}")
            elif export_type == "ogham":
                lines.append("LAST|P1|Q12")
            elif export_type == "holywells":
                lines.append("LAST|P1|Q14")
            elif export_type == "ci":
                lines.append("LAST|P1|Q21")
                lines.append("LAST|P1|Q22")
            elif export_type == "sisal":
                lines.append("LAST|P1|Q892")
            elif export_type == "romansites":
                lines.append("LAST|P1|Q893")
            elif export_type == "hogbacks":
                lines.append("LAST|P1|Q896")

            # P3 (OSM numeric ID)
            lines.append(f"LAST|P3|{item['osm_id']}")

            # P4 (OSM type: node=Q5, way=Q6, relation=Q7)
            type_map = {"node": "Q5", "way": "Q6", "relation": "Q7"}
            p4_val = type_map.get(item["osm_type"], "Q5")
            lines.append(f"LAST|P4|{p4_val}")

            # P5 (coordinates)
            lines.append(f"LAST|P5|@{item['coordinates']}")

            # P6 (Wikidata QID) - optional
            if item.get("wikidata") and item["wikidata"].startswith("Q"):
                lines.append(f'LAST|P6|"{qs_escape(item["wikidata"])}"')

            # P7 (Wikipedia URL) - optional
            if item.get("wikipedia"):
                lines.append(f'LAST|P7|"{qs_escape(item["wikipedia"])}"')

            # P8 (External links) - nicht in CSV, skip

            # P9 (Tags) - nicht mehr importiert, skip

            # P10 (Query item)
            p10_qid = P10_QUERY_ITEM.get(export_type, "Q24")
            lines.append(f"LAST|P10|{p10_qid}")

            # P11 (OSM URL)
            osm_url = (
                f"https://www.openstreetmap.org/{item['osm_type']}/{item['osm_id']}"
            )
            lines.append(f"LAST|P11|<{osm_url}>")

            # P12 (Snapshot timestamp)
            lines.append(f'LAST|P12|"{timestamp}"')

            # P13 (OSM version)
            if item.get("version"):
                lines.append(f"LAST|P13|{item['version']}")

            # P16 (OSM changeset)
            if item.get("changeset"):
                lines.append(f"LAST|P16|{item['changeset']}")

            # P17 (OSM timestamp)
            if item.get("osm_timestamp"):
                lines.append(f'LAST|P17|"{item["osm_timestamp"]}"')

            lines.append("")

    # MODIFIED Items
    if common_keys:
        lines.append("# ---------------------")
        lines.append("# CHECKING FOR UPDATES")
        lines.append("# ---------------------")
        lines.append("")

        for key in sorted(common_keys):
            osm_item = osm_index[key]
            wb_item = wb_index[key]
            qid = wb_item["qid"]

            changes = []
            change_notes = []

            # Sammle zuerst alle ECHTEN Änderungen (ohne P12)

            # Version geändert?
            version_changed = False
            if osm_item["version"] != wb_item["version"]:
                changes.append(f"{qid}|P13|{osm_item['version']}")
                changes.append(f"{qid}|P16|{osm_item['changeset']}")
                changes.append(f'{qid}|P17|"{osm_item["osm_timestamp"]}"')
                change_notes.append(
                    f"version {wb_item['version']}→{osm_item['version']}"
                )
                version_changed = True

                # Koordinaten auch updaten wenn Version geändert
                # (bei OSM-Änderungen können sich auch Koordinaten ändern)
                if osm_item["coordinates"] != wb_item["coordinates"]:
                    changes.append(f"{qid}|P5|@{osm_item['coordinates']}")
                    change_notes.append("coordinates")

            # NOTE: Label und Description werden NICHT verglichen/aktualisiert
            # Diese werden nur beim CREATE gesetzt, nicht bei Updates

            # NOTE: Koordinaten werden NUR bei Version-Änderung aktualisiert
            # Wenn Version gleich → keine echte OSM-Änderung → SKIP

            # Wikidata geändert?
            if osm_item["wikidata"] != wb_item["wikidata"]:
                if osm_item["wikidata"]:
                    changes.append(f'{qid}|P6|"{osm_item["wikidata"]}"')
                change_notes.append("wikidata")

            # Wikipedia geändert?
            # Normalisiere Format vor Vergleich (en:Article vs https://...)
            osm_wiki_normalized = normalize_wikipedia(osm_item["wikipedia"])
            wb_wiki_normalized = normalize_wikipedia(wb_item["wikipedia"])

            if osm_wiki_normalized != wb_wiki_normalized:
                if osm_item["wikipedia"]:
                    changes.append(f'{qid}|P7|"{osm_item["wikipedia"]}"')
                change_notes.append("wikipedia")

            # NOTE: Tags (P9) werden NICHT importiert, daher nicht vergleichen!

            # Wenn es ECHTE Änderungen gibt, füge P12 hinzu und schreibe sie
            if changes:  # Wenn irgendwelche Changes vorhanden sind
                # Füge P12 als ERSTE Zeile hinzu (nach Kommentar)
                changes.insert(0, f'{qid}|P12|"{timestamp}"')

                modified_count += 1
                lines.append(f"# {key} ({qid}): {', '.join(change_notes)}")
                lines.extend(changes)
                lines.append("")

    # Update Summary
    lines.insert(7, f"#   - {modified_count} MODIFIED (UPDATE)")

    # DELETED Items (automatic deprecation)
    if deleted_keys:
        lines.append("# ===============================================")
        lines.append(f"# 🗑️  DELETED ITEMS ({len(deleted_keys)}) - DEPRECATION")
        lines.append("# ===============================================")
        lines.append("# These items exist in Wikibase but were deleted from OSM.")
        lines.append("# Automatically marking as deprecated (P18 = Q891).")
        lines.append("")

        for key in sorted(deleted_keys):
            wb_item = wb_index[key]
            qid = wb_item["qid"]
            name = wb_item["label"]
            version = wb_item["version"]

            lines.append(f'# {key} ({qid}) - "{name}" - Last OSM version: {version}')
            lines.append(f"{qid}|P18|Q891  # status = deprecated")
            lines.append("")

    # Schreibe Datei
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"\n✅ Diff QuickStatements written to: {output_path}")
    print(f"   📝 {modified_count} items with actual changes")

    return (len(added_keys), modified_count)


# =================================================
# Main
# =================================================


def main():
    import sys

    # Check if run_dir was provided as argument
    if len(sys.argv) > 1:
        run_date = sys.argv[1]
        latest_run = DIST_BASE_DIR / run_date
        if not latest_run.exists():
            print(f"❌ Run directory not found: {latest_run}")
            return
    else:
        # Finde neuesten Run (legacy behavior)
        latest_run = find_latest_run_dir()
        if not latest_run:
            print("❌ No run directories found in dist/")
            return

    # Start Report Logging
    report_path = DIST_BASE_DIR / f"diff_report.txt"

    with ReportLogger(report_path):
        print("=" * 60)
        print("🧪 DIFF-QUICKSTATEMENTS GENERATION")
        print("=" * 60)
        print()

        print(f"📁 Using run: {latest_run.name}")
        print()

        # Statistiken
        total_added = 0
        total_modified = 0

        # Verarbeite alle Export-Typen
        for export_type, query_item_qid in TEST_EXPORTS.items():
            print("-" * 60)
            print(f"🔄 Processing: {export_type.upper()}")
            print("-" * 60)

            # Finde CSV
            csv_pattern = f"osm_export_{export_type}_*.csv"
            csv_files = list(latest_run.glob(csv_pattern))

            if not csv_files:
                print(f"⚠️  No CSV found matching: {csv_pattern}")
                print()
                continue

            csv_path = csv_files[0]

            # Lade OSM Daten
            osm_df = load_osm_csv(csv_path)
            if osm_df is None:
                print(f"⚠️  Skipping {export_type} - empty CSV")
                print()
                continue

            osm_items = [parse_osm_item(row) for _, row in osm_df.iterrows()]
            print()

            # Hole Wikibase Daten (kann leer sein wenn noch nichts importiert)
            wikibase_items = fetch_wikibase_items(export_type, query_item_qid)
            print()

            # Generiere Diff (auch wenn wikibase_items leer - dann alle CREATE!)
            output_filename = (
                f"quickstatements_DIFF_{export_type}_{latest_run.name}.txt"
            )
            output_path = latest_run / output_filename

            added, modified = generate_diff_quickstatements(
                osm_items, wikibase_items, export_type, output_path
            )

            total_added += added
            total_modified += modified

            print()

        print("=" * 60)
        print("✅ DIFF-QUICKSTATEMENTS COMPLETE")
        print("=" * 60)
        print(f"📊 TOTAL SUMMARY:")
        print(f"   ✅ {total_added} items ADDED across all types")
        print(f"   📝 {total_modified} items MODIFIED across all types")
        print("=" * 60)
        print()
        print(f"📄 Report saved to: {report_path}")
        print("=" * 60)


if __name__ == "__main__":
    main()
