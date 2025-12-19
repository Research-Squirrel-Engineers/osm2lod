#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generic OSM Overpass -> CSV -> RDF/Turtle (osm2lod)

- subject URIs are canonical OSM URIs: http://openstreetmap.org/{type}/{id}
- DCAT Dataset + CatalogRecord (record)
- CIDOC CRM base class configurable (applied to record URI)
- GeoSPARQL geometry (WKT Point, EPSG:4326)
- Tags are emitted ONLY if the value is really present (no NaN/"nan"/empty)
- Core tag set + optional per-export extension
- Export type label is embedded in:
  - filenames
  - dataset URI
  - metadata.json
  - (optionally) each record via osm2lod:exportType
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
# PATHS & TIME
# =================================================

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DIST_DIR = PROJECT_DIR / "dist"
DIST_DIR.mkdir(exist_ok=True)

NOW = datetime.now(timezone.utc)
TS_STR = NOW.strftime("%Y-%m-%d_%H%M%SZ")
ISO_TIME = NOW.isoformat()


# =================================================
# CONFIG (ONLY CHANGE THESE THREE FOR NEW EXPORTS)
# =================================================

EXPORT_TYPE = "holywells"  # "ogham" | "holywells" | "ci" | "drillcores" | ...

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

RAW_OVERPASS_QUERY = """
[out:json][timeout:25];
area["name"="Ireland"]->.boundaryarea;
nwr(area.boundaryarea)["place_of_worship"="holy_well"];
out geom;
""".strip()

CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
ENTITY_BASE_CLASS = CRM.E22_Human_Made_Object


# =================================================
# OTHER SETTINGS (rarely changed)
# =================================================

PAUSE_BEFORE_REQUEST_S = 1.0

OSM2LOD = Namespace("https://research-squirrel-engineers.github.io/osm2lod/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
PROV = Namespace("http://www.w3.org/ns/prov#")
GEOSPARQL = Namespace("http://www.opengis.net/ont/geosparql#")
SF = Namespace("http://www.opengis.net/ont/sf#")

# predicates for OSM tags (requested prefix: osmtag:)
OSM_TAG = Namespace("https://research-squirrel-engineers.github.io/osm2lod/osmtag/")

CRS_EPSG_4326 = "http://www.opengis.net/def/crs/EPSG/0/4326"
OSM_BASE = "http://openstreetmap.org/"

ADD_EXPORT_TYPE_TO_EACH_RECORD = True


# =================================================
# Core tags
# =================================================

CORE_TAG_KEYS: List[str] = [
    # identity & labels
    "name",
    "int_name",
    "alt_name",
    "loc_name",
    # description / documentation
    "description",
    "note",
    "source",
    "source:ref",
    "source:url",
    # access / operator-ish
    "access",
    "operator",
    # refs
    "ref",
    "ref:IE:smr",
    "ref:IE:nm",
    "ref:IE:niah",
    # heritage hints
    "heritage",
    "heritage:operator",
    "heritage:website",
    # external links
    "wikidata",
    "wikipedia",
    "wikimedia_commons",
    "image",
    "website",
    "url",
    # broad thematic keys
    "historic",
    "place_of_worship",
    "religion",
    "denomination",
    "amenity",
    "natural",
    "man_made",
    "water",
    "water_source",
    "tourism",
    "ele",
    "historic:civilization",
    "archaeological_site",
]

# optional extension per export (usually empty)
EXPORT_EXTRA_TAG_KEYS: List[str] = [
    # e.g. for drillcores:
    "volcano:status",
    "volcano:type",
    # e.g. for holywells:
    # "saint:name", "patron_day", "service_times"
]


# =================================================
# Helpers: value sanitation (fixes NaN/"nan")
# =================================================

_NA_STRINGS = {"nan", "na", "n/a", "null", "none", "nil", "-"}


def clean_value(v: Any) -> Optional[str]:
    """
    Normalise values coming from pandas/Overpass.
    Returns a clean string or None if value should be treated as missing.
    This prevents triples like osmtag:wikidata "nan".
    """
    if v is None:
        return None

    # pandas NaN / numpy NaN
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    # bytes -> str
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            v = str(v)

    # basic scalar -> str
    if isinstance(v, (int, float)):
        # floats already handled by isna; keep numeric as string
        return str(v)

    s = str(v).strip()
    if not s:
        return None

    if s.lower() in _NA_STRINGS:
        return None

    return s


# =================================================
# Overpass
# =================================================


def overpass_fetch_raw(query: str, pause_s: float = 1.0) -> Dict[str, Any]:
    time.sleep(max(0.0, pause_s))
    r = requests.post(OVERPASS_URL, data={"data": query}, timeout=180)
    r.raise_for_status()
    return r.json()


# =================================================
# JSON -> pandas
# =================================================


def _point_from_element(el: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    if el.get("type") == "node" and "lat" in el and "lon" in el:
        return float(el["lat"]), float(el["lon"])

    c = el.get("center")
    if isinstance(c, dict) and "lat" in c and "lon" in c:
        return float(c["lat"]), float(c["lon"])

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

    return None, None


def overpass_elements_to_df(elements: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for el in elements:
        lat, lon = _point_from_element(el)
        row: Dict[str, Any] = {
            "type": el.get("type"),
            "id": el.get("id"),
            "lat": lat,
            "lon": lon,
        }

        tags = el.get("tags") or {}
        if isinstance(tags, dict):
            for k, v in tags.items():
                row[f"tag:{k}"] = v

        rows.append(row)

    df = pd.DataFrame(rows)

    # Mandatory coordinates only
    df = df[df["lat"].notna() & df["lon"].notna()].copy()

    base_cols = ["type", "id", "lat", "lon"]
    tag_cols = sorted([c for c in df.columns if c.startswith("tag:")])
    cols = base_cols + tag_cols
    return df[cols]


# =================================================
# RDF helpers
# =================================================


def _osm_record_uri(el_type: str, el_id: int) -> URIRef:
    return URIRef(f"{OSM_BASE}{el_type}/{el_id}")


def _entity_uri(el_type: str, el_id: int) -> URIRef:
    return URIRef(f"{OSM2LOD}{el_type}/{el_id}")


def _geom_uri(el_type: str, el_id: int) -> URIRef:
    return URIRef(f"{OSM2LOD}{el_type}/{el_id}_geom")


def _wkt_point(lon: float, lat: float) -> str:
    return f"<{CRS_EPSG_4326}> POINT({lon} {lat})"


def _tag_predicate(tag_key: str) -> URIRef:
    return URIRef(f"{OSM_TAG}{quote(tag_key, safe='')}")


# =================================================
# Optional mapping (safe no-op if tags not present)
# =================================================


def apply_main_tag_mapping(g: Graph, record_uri: URIRef, tags: Dict[str, str]) -> None:
    """
    Keep this minimal. It never emits anything unless input tags contain real values.
    """

    def concept(key: str, value: str) -> URIRef:
        return URIRef(f"{OSM2LOD}concept/{quote(key, safe='')}/{quote(value, safe='')}")

    if tags.get("historic") == "ogham_stone":
        g.add((record_uri, RDF.type, URIRef(f"{OSM2LOD}OghamStone")))
        g.add((record_uri, DCTERMS.subject, concept("historic", "ogham_stone")))

    if tags.get("place_of_worship") == "holy_well":
        g.add((record_uri, RDF.type, URIRef(f"{OSM2LOD}HolyWell")))
        g.add((record_uri, DCTERMS.subject, concept("place_of_worship", "holy_well")))

    # ref:* -> dcterms:identifier
    for k, v in tags.items():
        if k.startswith("ref:"):
            g.add((record_uri, DCTERMS.identifier, Literal(v)))

    # Wikidata -> owl:sameAs (only for Q-IDs)
    wd = tags.get("wikidata")
    if wd and wd.startswith("Q"):
        g.add((record_uri, OWL.sameAs, URIRef(f"https://www.wikidata.org/entity/{wd}")))


# =================================================
# RDF writer
# =================================================


def df_to_rdf(df: pd.DataFrame) -> Graph:
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

    dataset = URIRef(f"{OSM2LOD}dataset/osm-export/{EXPORT_TYPE}/{TS_STR}")
    g.add((dataset, RDF.type, DCAT.Dataset))
    g.add((dataset, DCTERMS.title, Literal("OSM Overpass Export", lang="en")))
    g.add((dataset, DCTERMS.created, Literal(ISO_TIME, datatype=XSD.dateTime)))
    g.add((dataset, DCTERMS.source, URIRef(OVERPASS_URL)))
    g.add((dataset, DCTERMS.type, Literal(EXPORT_TYPE)))
    g.add((dataset, DCTERMS.identifier, Literal(f"osm2lod:{EXPORT_TYPE}:{TS_STR}")))
    g.add(
        (
            dataset,
            DCTERMS.rights,
            Literal("© OpenStreetMap contributors (ODbL)", lang="en"),
        )
    )
    g.add((dataset, DCTERMS.provenance, Literal(RAW_OVERPASS_QUERY)))

    tag_keys = sorted(set(CORE_TAG_KEYS + EXPORT_EXTRA_TAG_KEYS))

    for _, row in df.iterrows():
        el_type = str(row["type"])
        el_id = int(row["id"])
        lat = float(row["lat"])
        lon = float(row["lon"])

        record_uri = _osm_record_uri(el_type, el_id)
        entity_uri = _entity_uri(el_type, el_id)

        g.add((record_uri, RDF.type, DCAT.CatalogRecord))
        g.add((record_uri, RDF.type, ENTITY_BASE_CLASS))

        g.add((record_uri, DCTERMS.created, Literal(ISO_TIME, datatype=XSD.dateTime)))
        g.add((record_uri, DCTERMS.identifier, Literal(f"osm:{el_type}/{el_id}")))
        g.add((record_uri, DCTERMS.isPartOf, dataset))
        g.add((record_uri, FOAF.primaryTopic, entity_uri))

        if ADD_EXPORT_TYPE_TO_EACH_RECORD:
            g.add((record_uri, URIRef(f"{OSM2LOD}exportType"), Literal(EXPORT_TYPE)))

        g.add((entity_uri, RDF.type, URIRef(f"{OSM2LOD}OSMElement")))
        g.add((entity_uri, RDF.type, URIRef(f"{OSM2LOD}{el_type.capitalize()}")))

        # label
        name = clean_value(row.get("tag:name"))
        if name:
            g.add((record_uri, RDFS.label, Literal(name)))

        # GeoSPARQL
        geom = _geom_uri(el_type, el_id)
        g.add((record_uri, GEOSPARQL.hasGeometry, geom))
        g.add((geom, RDF.type, SF.Point))
        g.add(
            (
                geom,
                GEOSPARQL.asWKT,
                Literal(_wkt_point(lon, lat), datatype=GEOSPARQL.wktLiteral),
            )
        )

        # core tags (ONLY if real value exists)
        tags: Dict[str, str] = {}
        for k in tag_keys:
            col = f"tag:{k}"
            if col not in df.columns:
                continue
            v = clean_value(row.get(col))
            if not v:
                continue
            tags[k] = v
            g.add((record_uri, _tag_predicate(k), Literal(v)))

        apply_main_tag_mapping(g, record_uri, tags)

    return g


# =================================================
# metadata.json
# =================================================


def write_metadata_json(
    csv_path: Path, ttl_path: Path, record_count: int, columns: List[str]
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
        "exportType": EXPORT_TYPE,
        "dcterms:title": "OSM Overpass Export",
        "dcterms:created": ISO_TIME,
        "dcterms:license": "https://opendatacommons.org/licenses/odbl/",
        "dcterms:source": OVERPASS_URL,
        "prov:wasGeneratedBy": {
            "prov:type": "SoftwareExecution",
            "prov:startedAtTime": ISO_TIME,
            "prov:used": "RAW Overpass Turbo Query",
        },
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
        "recordCount": record_count,
        "columns": columns,
        "coreTagKeys": CORE_TAG_KEYS,
        "extraTagKeys": EXPORT_EXTRA_TAG_KEYS,
        "entityBaseClass": str(ENTITY_BASE_CLASS),
        "recordUriPattern": f"{OSM_BASE}" + "{type}/{id}",
        "primaryTopicPattern": str(OSM2LOD) + "{type}/{id}",
        "geometry": {
            "type": "GeoSPARQL WKT Point",
            "crs": CRS_EPSG_4326,
            "wktExample": f"<{CRS_EPSG_4326}> POINT(lon lat)",
        },
        "overpassQuery": RAW_OVERPASS_QUERY,
        "output": {"csv": csv_path.name, "ttl": ttl_path.name},
    }

    meta_path = DIST_DIR / f"metadata_{EXPORT_TYPE}_{TS_STR}.json"
    meta_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return meta_path


# =================================================
# Main
# =================================================


def main() -> None:
    data = overpass_fetch_raw(RAW_OVERPASS_QUERY, PAUSE_BEFORE_REQUEST_S)
    elements = data.get("elements", [])

    df = overpass_elements_to_df(elements)

    csv_path = DIST_DIR / f"osm_export_{EXPORT_TYPE}_{TS_STR}.csv"
    ttl_path = DIST_DIR / f"osm_export_{EXPORT_TYPE}_{TS_STR}.ttl"

    df.to_csv(csv_path, index=False, encoding="utf-8")

    g = df_to_rdf(df)
    g.serialize(destination=ttl_path, format="turtle")

    meta_path = write_metadata_json(csv_path, ttl_path, len(df), list(df.columns))

    print(f"✔ EXPORT_TYPE   {EXPORT_TYPE}")
    print(f"✔ CSV           {csv_path}")
    print(f"✔ RDF           {ttl_path}")
    print(f"✔ META          {meta_path}")
    print(f"✔ Rows          {len(df)}")
    print(f"✔ Columns       {len(df.columns)}")
    print(f"✔ BaseClass     {ENTITY_BASE_CLASS}")


if __name__ == "__main__":
    main()
