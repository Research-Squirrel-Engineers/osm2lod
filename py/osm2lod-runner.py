#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
osm2lod-runner.py

Generic OSM → CSV → RDF/Turtle (osm2lod)
======================================

- Runs multiple Overpass Turbo exports from ONE script
- Supported exports: ogham, holywells, ci, drillcores
- Selection via SELECTED_EXPORTS list (default: run ALL)
- Only emits RDF triples for values that actually exist (no NaN/"nan"/empty)
- GeoSPARQL WKT geometry (EPSG:4326)
- DCAT Dataset:
    * one dataset per export run
    * each OSM record is also typed as DCAT.Dataset (as requested)
- CIDOC CRM base class configurable per export (applied to record URI)
- Writes metadata_{export}_{ts}.json per export
"""

from __future__ import annotations

import time
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
import pandas as pd
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, OWL, DCTERMS, FOAF


# =================================================
# Namespaces
# =================================================

OSM2LOD = Namespace("https://research-squirrel-engineers.github.io/osm2lod/")
OSM_TAG = Namespace("https://research-squirrel-engineers.github.io/osm2lod/osmtag/")

CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
PROV = Namespace("http://www.w3.org/ns/prov#")

GEOSPARQL = Namespace("http://www.opengis.net/ont/geosparql#")
SF = Namespace("http://www.opengis.net/ont/sf#")

OSM_BASE = "http://openstreetmap.org/"
CRS_EPSG_4326 = "http://www.opengis.net/def/crs/EPSG/0/4326"

# Overpass endpoints (fallback chain)
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]


# =================================================
# Export definitions (ONLY queries differ)
# =================================================

EXPORTS: Dict[str, Dict[str, Any]] = {
    "ogham": {
        "query": """
[out:json][timeout:25];
area["name"="Ireland"]->.boundaryarea;
nwr(area.boundaryarea)["historic"="ogham_stone"];
out geom;
""",
        "entity_base_class": CRM.E22_Human_Made_Object,
    },
    "holywells": {
        "query": """
[out:json][timeout:25];
area["name"="Ireland"]->.boundaryarea;
nwr(area.boundaryarea)["place_of_worship"="holy_well"];
out geom;
""",
        "entity_base_class": CRM.E26_Physical_Feature,
    },
    "ci": {
        "query": """
[out:json][timeout:25];
(
  node(337519639);
  node(369821847);
  node(1221172611);
  node(2293681037);
  node(7778324735);
  node(8814442373);
  node(10879170567);
  node(11107939919);
  node(11109379095);
);
out geom;
""",
        "entity_base_class": CRM.E55_Place,
    },
    "drillcores": {
        "query": """
[out:json][timeout:25];
(
  node(13386703821);
  node(13386786938);
  node(13386723672);
  node(13200955773);
  node(13200943487);
);
out geom;
""",
        "entity_base_class": CRM.E55_Place,
    },
}


# =================================================
# EXPORT SELECTION (VS Code: edit this list)
# =================================================
# [] or None -> run ALL
SELECTED_EXPORTS: List[str] = [
    # "ogham",
    # "holywells",
    # "ci",
    # "drillcores",
]


# =================================================
# Core tag set (written ONLY if value exists)
# =================================================

CORE_TAG_KEYS = [
    "name",
    "alt_name",
    "int_name",
    "loc_name",
    "description",
    "note",
    "source",
    "source:ref",
    "source:url",
    "ref",
    "access",
    "operator",
    "historic",
    "historic:civilization",
    "place_of_worship",
    "religion",
    "denomination",
    "natural",
    "man_made",
    "water",
    "water_source",
    "tourism",
    "archaeological_site",
    "ele",
    "wikidata",
    "wikipedia",
    "wikimedia_commons",
    "image",
    "website",
    "url",
]

# Optional per-export tag extensions (usually empty; add if you want)
EXPORT_EXTRA_TAG_KEYS: Dict[str, List[str]] = {
    "drillcores": ["volcano:status", "volcano:type"],
    # "holywells": ["saint:name", "patron_day", ...],
    # "ci": [...],
    # "ogham": [...],
}


# =================================================
# Helpers
# =================================================

_NA_STRINGS = {"nan", "na", "n/a", "null", "none", "nil", "-"}


def clean_value(v: Any) -> Optional[str]:
    """Return clean string or None (prevents triples like osmtag:wikidata 'nan')."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            v = str(v)

    s = str(v).strip()
    if not s:
        return None
    if s.lower() in _NA_STRINGS:
        return None
    return s


def overpass_fetch(
    query: str, *, pause_s: float = 1.0, retries: int = 4
) -> Dict[str, Any]:
    """
    Fetch with endpoint fallback + retry/backoff.
    Handles intermittent 504s from overpass-api.de.
    """
    last_err: Optional[Exception] = None
    query = query.strip()

    for endpoint in OVERPASS_URLS:
        for attempt in range(1, retries + 1):
            time.sleep(max(0.0, pause_s))
            try:
                r = requests.post(endpoint, data={"data": query}, timeout=240)
                # 429/504 etc -> raise to retry
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_err = e
                # exponential backoff
                backoff = min(30.0, 2.0**attempt)
                time.sleep(backoff)

        # next endpoint
    raise RuntimeError(
        f"Overpass failed after retries on all endpoints. Last error: {last_err}"
    )


def extract_point(el: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    # nodes: lat/lon
    if (
        el.get("type") == "node"
        and el.get("lat") is not None
        and el.get("lon") is not None
    ):
        return float(el["lat"]), float(el["lon"])
    # ways/relations: use center if present
    c = el.get("center")
    if isinstance(c, dict) and c.get("lat") is not None and c.get("lon") is not None:
        return float(c["lat"]), float(c["lon"])
    # geometry list fallback (rare in your current exports, but safe)
    geom = el.get("geometry")
    if isinstance(geom, list) and geom:
        lats = [
            p.get("lat")
            for p in geom
            if isinstance(p, dict) and p.get("lat") is not None
        ]
        lons = [
            p.get("lon")
            for p in geom
            if isinstance(p, dict) and p.get("lon") is not None
        ]
        if lats and lons:
            return float(sum(lats) / len(lats)), float(sum(lons) / len(lons))
    return None


def osm_uri(el_type: str, el_id: int) -> URIRef:
    return URIRef(f"{OSM_BASE}{el_type}/{el_id}")


def entity_uri(el_type: str, el_id: int) -> URIRef:
    return URIRef(f"{OSM2LOD}{el_type}/{el_id}")


def geom_uri(el_type: str, el_id: int) -> URIRef:
    return URIRef(f"{OSM2LOD}{el_type}/{el_id}_geom")


def wkt_point(lon: float, lat: float) -> str:
    return f"<{CRS_EPSG_4326}> POINT({lon} {lat})"


def tag_predicate(key: str) -> URIRef:
    return URIRef(f"{OSM_TAG}{quote(key, safe='')}")


def write_metadata_json(
    dist_dir: Path,
    export_type: str,
    ts: str,
    now_iso: str,
    csv_path: Path,
    ttl_path: Path,
    record_count: int,
    columns: List[str],
    overpass_query: str,
    entity_base_class: URIRef,
    core_keys: List[str],
    extra_keys: List[str],
) -> Path:
    metadata = {
        "@context": {
            "dcat": str(DCAT),
            "prov": str(PROV),
            "dcterms": str(DCTERMS),
            "geosparql": str(GEOSPARQL),
            "osm2lod": str(OSM2LOD),
            "osmtag": str(OSM_TAG),
        },
        "exportType": export_type,
        "datasetUri": str(URIRef(f"{OSM2LOD}dataset/osm-export/{export_type}/{ts}")),
        "dcterms:title": "OSM Overpass Export",
        "dcterms:created": now_iso,
        "dcterms:license": "https://opendatacommons.org/licenses/odbl/",
        "overpassEndpointCandidates": OVERPASS_URLS,
        "overpassQuery": overpass_query.strip(),
        "entityBaseClass": str(entity_base_class),
        "recordUriPattern": f"{OSM_BASE}" + "{type}/{id}",
        "primaryTopicPattern": str(OSM2LOD) + "{type}/{id}",
        "geometry": {
            "type": "GeoSPARQL WKT Point",
            "crs": CRS_EPSG_4326,
            "wktExample": f"<{CRS_EPSG_4326}> POINT(lon lat)",
        },
        "coreTagKeys": core_keys,
        "extraTagKeys": extra_keys,
        "recordCount": int(record_count),
        "columns": columns,
        "dcat:distribution": [
            {
                "dcat:mediaType": "text/csv",
                "dcat:downloadURL": csv_path.name,
                "dcat:byteSize": csv_path.stat().st_size,
            },
            {
                "dcat:mediaType": "text/turtle",
                "dcat:downloadURL": ttl_path.name,
                "dcat:byteSize": ttl_path.stat().st_size,
            },
        ],
        "output": {"csv": csv_path.name, "ttl": ttl_path.name},
    }

    meta_path = dist_dir / f"metadata_{export_type}_{ts}.json"
    meta_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return meta_path


# =================================================
# RDF writer
# =================================================


def export_to_rdf(
    export_type: str,
    elements: List[Dict[str, Any]],
    entity_base_class: URIRef,
    dist_dir: Path,
    overpass_query: str,
    add_export_type_to_each_record: bool = True,
) -> None:
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%d_%H%M%SZ")
    now_iso = now.isoformat()

    rows: List[Dict[str, Any]] = []
    for el in elements:
        pt = extract_point(el)
        if not pt:
            continue
        lat, lon = pt
        row: Dict[str, Any] = {
            "type": el.get("type"),
            "id": el.get("id"),
            "lat": lat,
            "lon": lon,
        }
        for k, v in (el.get("tags") or {}).items():
            row[f"tag:{k}"] = v
        rows.append(row)

    df = pd.DataFrame(rows)

    csv_path = dist_dir / f"osm_export_{export_type}_{ts}.csv"
    ttl_path = dist_dir / f"osm_export_{export_type}_{ts}.ttl"

    df.to_csv(csv_path, index=False, encoding="utf-8")

    g = Graph()
    g.bind("osm2lod", OSM2LOD)
    g.bind("osmtag", OSM_TAG)
    g.bind("crm", CRM)
    g.bind("dcat", DCAT)
    g.bind("prov", PROV)
    g.bind("dcterms", DCTERMS)
    g.bind("foaf", FOAF)
    g.bind("geosparql", GEOSPARQL)
    g.bind("sf", SF)
    g.bind("owl", OWL)

    dataset = URIRef(f"{OSM2LOD}dataset/osm-export/{export_type}/{ts}")
    g.add((dataset, RDF.type, DCAT.Dataset))
    g.add((dataset, DCTERMS.type, Literal(export_type)))
    g.add((dataset, DCTERMS.created, Literal(now_iso, datatype=XSD.dateTime)))
    g.add(
        (
            dataset,
            DCTERMS.rights,
            Literal("© OpenStreetMap contributors (ODbL)", lang="en"),
        )
    )
    g.add((dataset, DCTERMS.provenance, Literal(overpass_query.strip())))

    extra_keys = EXPORT_EXTRA_TAG_KEYS.get(export_type, [])
    tag_keys = sorted(set(CORE_TAG_KEYS + extra_keys))

    for _, row in df.iterrows():
        el_type = str(row["type"])
        el_id = int(row["id"])
        lat = float(row["lat"])
        lon = float(row["lon"])

        rec = osm_uri(el_type, el_id)
        ent = entity_uri(el_type, el_id)

        # IMPORTANT: you said DCAT.Dataset is correct for records → keep it
        g.add((rec, RDF.type, DCAT.Dataset))
        g.add((rec, RDF.type, entity_base_class))
        g.add((rec, DCTERMS.isPartOf, dataset))
        g.add((rec, FOAF.primaryTopic, ent))
        g.add((rec, DCTERMS.identifier, Literal(f"osm:{el_type}/{el_id}")))
        g.add((rec, DCTERMS.created, Literal(now_iso, datatype=XSD.dateTime)))

        if add_export_type_to_each_record:
            g.add((rec, URIRef(f"{OSM2LOD}exportType"), Literal(export_type)))

        name = clean_value(row.get("tag:name"))
        if name:
            g.add((rec, RDFS.label, Literal(name)))

        geom = geom_uri(el_type, el_id)
        g.add((rec, GEOSPARQL.hasGeometry, geom))
        g.add((geom, RDF.type, SF.Point))
        g.add(
            (
                geom,
                GEOSPARQL.asWKT,
                Literal(wkt_point(lon, lat), datatype=GEOSPARQL.wktLiteral),
            )
        )

        # core+extra tag emission (ONLY if real value exists)
        for key in tag_keys:
            col = f"tag:{key}"
            if col not in df.columns:
                continue
            val = clean_value(row.get(col))
            if val:
                g.add((rec, tag_predicate(key), Literal(val)))

        # Wikidata linking only for Q-IDs
        wd = clean_value(row.get("tag:wikidata"))
        if wd and wd.startswith("Q"):
            g.add((rec, OWL.sameAs, URIRef(f"https://www.wikidata.org/entity/{wd}")))

    g.serialize(ttl_path, format="turtle")

    meta_path = write_metadata_json(
        dist_dir=dist_dir,
        export_type=export_type,
        ts=ts,
        now_iso=now_iso,
        csv_path=csv_path,
        ttl_path=ttl_path,
        record_count=len(df),
        columns=list(df.columns),
        overpass_query=overpass_query,
        entity_base_class=entity_base_class,
        core_keys=CORE_TAG_KEYS,
        extra_keys=extra_keys,
    )

    print(f"✔ {export_type}: {len(df)} records")
    print(f"  → {csv_path.name}")
    print(f"  → {ttl_path.name}")
    print(f"  → {meta_path.name}")


# =================================================
# Main
# =================================================


def main() -> None:
    dist_dir = Path("dist")
    dist_dir.mkdir(exist_ok=True)

    exports_to_run = SELECTED_EXPORTS or list(EXPORTS.keys())

    unknown = set(exports_to_run) - set(EXPORTS.keys())
    if unknown:
        raise ValueError(f"Unknown export types: {sorted(unknown)}")

    print("▶ Running exports:", ", ".join(exports_to_run))

    for exp in exports_to_run:
        cfg = EXPORTS[exp]
        data = overpass_fetch(cfg["query"])
        export_to_rdf(
            export_type=exp,
            elements=data.get("elements", []),
            entity_base_class=cfg["entity_base_class"],
            dist_dir=dist_dir,
            overpass_query=cfg["query"],
        )


if __name__ == "__main__":
    main()
