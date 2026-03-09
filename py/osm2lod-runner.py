#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import shutil
import time
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set, NamedTuple
from urllib.parse import quote, urlparse

import requests
import pandas as pd
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, OWL, DCTERMS, FOAF


# =================================================
# Changelog types
# =================================================


class ChangelogItem(NamedTuple):
    osm_id: str
    osm_type: str
    osm_numeric_id: int
    name: str
    old_version: Optional[int] = None
    new_version: Optional[int] = None
    old_timestamp: Optional[str] = None
    new_timestamp: Optional[str] = None
    field_changes: Optional[Dict[str, Tuple[Any, Any]]] = (
        None  # field_name -> (old_value, new_value)
    )


class ChangelogReport(NamedTuple):
    export_type: str
    old_date: str
    new_date: str
    added: List[ChangelogItem]
    deleted: List[ChangelogItem]
    modified: List[ChangelogItem]


# =================================================
# Namespaces
# =================================================

OSM2LOD = Namespace("http://research-squirrel-engineers.github.io/osm2lod/")
CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
PROV = Namespace("http://www.w3.org/ns/prov#")
GEOSPARQL = Namespace("http://www.opengis.net/ont/geosparql#")
SF = Namespace("http://www.opengis.net/ont/sf#")

WD = Namespace("http://wikidata.org/entity/")

OSM_BASE = "http://openstreetmap.org/"
CRS_EPSG_4326 = "http://www.opengis.net/def/crs/EPSG/0/4326"

OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]


# =================================================
# URL expansion (QS only)
# =================================================

ENABLE_URL_EXPANSION = True
SHORT_URL_DOMAINS = {"skfb.ly", "flic.kr"}
URL_EXPAND_TIMEOUT = 15

_URL_EXPAND_CACHE: Dict[str, str] = {}


def expand_url(url: str, timeout: int = URL_EXPAND_TIMEOUT) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; url-expander/1.0)"}
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        if r.status_code < 400 and r.url:
            return r.url
    except requests.RequestException:
        pass

    r = requests.get(
        url, allow_redirects=True, timeout=timeout, headers=headers, stream=True
    )
    return r.url or url


def maybe_expand_url(url: str) -> str:
    url = (url or "").strip()
    if not ENABLE_URL_EXPANSION:
        return url
    if not url.lower().startswith(("http://", "https://")):
        return url

    if url in _URL_EXPAND_CACHE:
        return _URL_EXPAND_CACHE[url]

    try:
        host = (urlparse(url).netloc or "").lower().split(":")[0]
        if host not in SHORT_URL_DOMAINS:
            _URL_EXPAND_CACHE[url] = url
            return url

        expanded = expand_url(url)
        _URL_EXPAND_CACHE[url] = expanded or url
        return _URL_EXPAND_CACHE[url]
    except Exception:
        _URL_EXPAND_CACHE[url] = url
        return url


# =================================================
# Export definitions
# =================================================

EXPORTS: Dict[str, Dict[str, Any]] = {
    "ogham": {
        "query": """
[out:json][timeout:25];
area["name"="Ireland"]->.boundaryarea;
nwr(area.boundaryarea)["historic"="ogham_stone"];
out meta geom;
""",
        "entity_base_class": CRM.E22_Human_Made_Object,
    },
    "holywells": {
        "query": """
[out:json][timeout:25];
area["name"="Ireland"]->.boundaryarea;
nwr(area.boundaryarea)["place_of_worship"="holy_well"];
out meta geom;
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
out meta geom;
""",
        "entity_base_class": CRM.E55_Place,
    },
    "drillcores": {  # https://overpass-turbo.eu/s/2hD7
        "query": """
[out:json][timeout:25];
(
  way(23450728);
  way(56428302);
  way(72767715);
  way(144726755);
  way(144726746);
  way(144719688);
  node(13200955773);
  node(13200943487);
);
out meta geom;
""",
        "entity_base_class": CRM.E55_Place,
    },
    "benchmarks": {
        "query": """
[out:json][timeout:180];
(
  area["name:en"="Ireland"]->.ie;
  area["name:en"="Scotland"]->.sc;
  
  node(area.ie)["man_made"="survey_point"]["benchmark"="yes"]["wikimedia_commons"];
  node(area.sc)["man_made"="survey_point"]["benchmark"="yes"]["wikimedia_commons"];
);
out meta geom;
""",
        "entity_base_class": CRM.E22_Human_Made_Object,
    },
}

SELECTED_EXPORTS: List[str] = [
    # "ogham",
    # "holywells",
    # "ci",
    # "drillcores",
]

DIST_BASE_DIR = Path("dist")
CLEAR_DIST_ON_START = True

DIST_CLEAN_PREFIXES = (
    "osm_export_",
    "metadata_",
    "quickstatements_",
)

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

EXPORT_EXTRA_TAG_KEYS: Dict[str, List[str]] = {
    "drillcores": ["volcano:status", "volcano:type"],
    "benchmarks": ["benchmark", "survey:date", "survey_point:structure"],
}

URL_TAG_KEYS = {"source:url", "website", "url", "image", "wikimedia_commons"}

P10_QUERY_ITEM = {
    "ogham": "Q24",
    "holywells": "Q25",
    "ci": "Q26",
    "drillcores": "Q27",
    "benchmarks": "Q28",
}

P1_FIXED = {"ogham": ["Q12"], "holywells": ["Q14"], "ci": ["Q21", "Q22"]}

P4_TYPE_ITEM = {"node": "Q5", "way": "Q6", "relation": "Q7"}

_NA_STRINGS = {"nan", "na", "n/a", "null", "none", "nil", "-"}
_DOI_RE = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.IGNORECASE)
_BCP47_RE = re.compile(r"^[a-zA-Z]{2,3}([\-][a-zA-Z0-9]{2,8})*$")

# =================================================
# Export-type specific RDF classes (osm2lod)
# =================================================

EXPORT_RDFTYPE: Dict[str, URIRef] = {
    "ogham": URIRef(f"{OSM2LOD}OghamStone"),
    "holywells": URIRef(f"{OSM2LOD}HolyWell"),
    "ci": URIRef(f"{OSM2LOD}CI_Findspot"),
    "maar": URIRef(f"{OSM2LOD}Maar"),
    "coreprofile": URIRef(f"{OSM2LOD}CoreProfile"),
    "benchmarks": URIRef(f"{OSM2LOD}Benchmark"),
}


# =================================================
# Helpers
# =================================================


def clean_value(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    s = str(v).strip()
    if not s:
        return None
    if s.lower() in _NA_STRINGS:
        return None
    return s


def get_or_create_run_dir(base_dir: Path) -> Path:
    """
    Erstellt einen datumsspezifischen Unterordner für den aktuellen Durchlauf.
    Format: YYYY-MM-DD
    Falls der Ordner bereits existiert, wird er komplett gelöscht und neu erstellt.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_dir = base_dir / today

    # Falls der Ordner existiert, lösche ihn komplett
    if run_dir.exists():
        shutil.rmtree(run_dir)
        print(f"🗑️  Existing run directory deleted: {run_dir}")

    # Erstelle den Ordner neu
    run_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Run directory created: {run_dir}")

    return run_dir


def clear_dist(dist_dir: Path) -> None:
    """
    Legacy-Funktion: Löscht alte Export-Dateien im dist-Verzeichnis.
    Wird nicht mehr benötigt, da wir datumsspezifische Unterordner verwenden.
    """
    if not dist_dir.exists():
        return
    for p in dist_dir.iterdir():
        if not p.is_file():
            continue
        if p.name.startswith(DIST_CLEAN_PREFIXES):
            try:
                p.unlink()
            except Exception:
                pass


def overpass_fetch(
    query: str, *, pause_s: float = 1.0, retries: int = 4
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    query = query.strip()

    for endpoint in OVERPASS_URLS:
        for attempt in range(1, retries + 1):
            time.sleep(max(0.0, pause_s))
            try:
                r = requests.post(
                    endpoint, data={"data": query}, timeout=400
                )  # Erhöht von 240 auf 400
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_err = e
                backoff = min(30.0, 2.0**attempt)
                time.sleep(backoff)

    raise RuntimeError(
        f"Overpass failed after retries on all endpoints. Last error: {last_err}"
    )


def extract_point(el: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    if (
        el.get("type") == "node"
        and el.get("lat") is not None
        and el.get("lon") is not None
    ):
        return float(el["lat"]), float(el["lon"])
    c = el.get("center")
    if isinstance(c, dict) and c.get("lat") is not None and c.get("lon") is not None:
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
    return None


def osm_canonical_uri(el_type: str, el_id: int) -> URIRef:
    """Canonical OSM URI (external reference)."""
    return URIRef(f"{OSM_BASE}{el_type}/{el_id}")


def record_uri(el_type: str, el_id: int) -> URIRef:
    """Internal RDF subject in osm2lod namespace: osm2lod:node_123, osm2lod:way_..., osm2lod:relation_..."""
    return URIRef(f"{OSM2LOD}{el_type}_{el_id}")


def geom_uri(el_id: int) -> URIRef:
    return URIRef(f"{OSM2LOD}{el_id}_geom")


def wkt_point(lon: float, lat: float) -> str:
    return f"<{CRS_EPSG_4326}> POINT({lon} {lat})"


def osmtag_predicate(key: str) -> URIRef:
    safe = key.replace(":", "__")
    return URIRef(f"{OSM2LOD}osmtag__{safe}")


def parse_wikipedia_literal(value: str) -> Literal:
    m = re.match(r"^([a-z]{2,3}):(.+)$", value.strip())
    if m:
        return Literal(m.group(2).strip(), lang=m.group(1))
    return Literal(value)


def extract_doi(value: str) -> Optional[str]:
    if not value:
        return None
    m = _DOI_RE.search(value.strip())
    if not m:
        return None
    return m.group(1).rstrip(" .;,)")


def is_valid_lang_tag(lang: str) -> bool:
    lang = (lang or "").strip()
    if not lang:
        return False
    if ":" in lang or "__" in lang:
        return False
    return bool(_BCP47_RE.match(lang))


def qs_escape(s: str) -> str:
    return s.replace('"', r"\"")


def wikipedia_to_url(v: str) -> Optional[str]:
    v = (v or "").strip()
    if not v:
        return None
    if v.lower().startswith(("http://", "https://")):
        return v
    m = re.match(r"^([a-z]{2,3}):(.+)$", v)
    if not m:
        return None
    lang = m.group(1)
    title = m.group(2).strip()
    title_enc = quote(title.replace(" ", "_"), safe=":/()_-.,'&%")
    return f"https://{lang}.wikipedia.org/wiki/{title_enc}"


def osm_element_url(el_type: str, el_id: int) -> str:
    return f"https://www.openstreetmap.org/{el_type}/{el_id}"


def is_maar_row(row: pd.Series) -> bool:
    vt = clean_value(row.get("tag:volcano:type"))
    if vt and vt.strip().lower() == "maar":
        return True
    nat = clean_value(row.get("tag:natural"))
    if nat and nat.strip().lower() == "volcano":
        return True
    mm = clean_value(row.get("tag:man_made"))
    if mm and mm.strip().lower() == "volcano":
        return True
    return False


# =================================================
# metadata.json writer
# =================================================


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
    dataset_uri: URIRef,
) -> Path:
    metadata = {
        "@context": {
            "dcat": str(DCAT),
            "prov": str(PROV),
            "dcterms": str(DCTERMS),
            "geosparql": str(GEOSPARQL),
            "osm2lod": str(OSM2LOD),
            "wd": str(WD),
        },
        "exportType": export_type,
        "datasetUri": str(dataset_uri),
        "dcterms:title": "OSM Overpass Export",
        "dcterms:created": now_iso,
        "dcterms:license": "https://opendatacommons.org/licenses/odbl/",
        "overpassEndpointCandidates": OVERPASS_URLS,
        "overpassQuery": overpass_query.strip(),
        "entityBaseClass": str(entity_base_class),
        "recordUriPattern": f"{OSM2LOD}" + "{type}_{id}",
        "canonicalOsmUriPattern": f"{OSM_BASE}" + "{type}/{id}",
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
# QuickStatements writer (unchanged)
# =================================================


def export_to_quickstatements(
    export_type: str,
    df: pd.DataFrame,
    dist_dir: Path,
    ts: str,
    now_edtf: str,
) -> Path:
    qs_path = dist_dir / f"quickstatements_{export_type}_{ts}.txt"

    p10_item = P10_QUERY_ITEM[export_type]
    fixed_p1 = P1_FIXED.get(export_type)

    lines: List[str] = []

    def add_last(prop: str, value: str) -> None:
        lines.append(f"LAST|{prop}|{value}")

    ref_cols = [c for c in df.columns if c.startswith("tag:ref:")]
    has_source_ref = "tag:source_ref" in df.columns
    has_source_colon_ref = "tag:source:ref" in df.columns

    for _, row in df.iterrows():
        el_type = clean_value(row.get("type"))
        if not el_type:
            continue
        el_id_s = clean_value(row.get("id"))
        if not el_id_s:
            continue

        try:
            el_id = int(float(el_id_s))
        except Exception:
            continue

        lat_s = clean_value(row.get("lat"))
        lon_s = clean_value(row.get("lon"))
        if lat_s is None or lon_s is None:
            continue
        lat = float(lat_s)
        lon = float(lon_s)

        ver_int: Optional[int] = None
        ver_s = clean_value(row.get("version"))
        if ver_s is not None:
            try:
                ver_int = int(float(ver_s))
            except Exception:
                ver_int = None

        changeset_int: Optional[int] = None
        cs_s = clean_value(row.get("changeset"))
        if cs_s is not None:
            try:
                changeset_int = int(float(cs_s))
            except Exception:
                changeset_int = None

        ts_el = clean_value(row.get("timestamp"))

        name_en = clean_value(row.get("tag:name:en"))
        name = clean_value(row.get("tag:name"))
        label = name_en or name or f"OSM {el_type} {el_id}"

        desc = f"OSM import snapshot ({export_type}) – {el_type} {el_id}"

        if export_type == "drillcores":
            p1_list = ["Q20"] if is_maar_row(row) else ["Q17"]
        else:
            p1_list = fixed_p1 or []

        p4_item = P4_TYPE_ITEM.get(el_type)
        if not p4_item:
            continue

        wd = clean_value(row.get("tag:wikidata"))
        wiki = clean_value(row.get("tag:wikipedia"))

        p8_urls: List[str] = []
        if export_type == "drillcores":
            u = clean_value(row.get("tag:source:url"))
            if u and u.lower().startswith(("http://", "https://")):
                p8_urls.append(u)
        elif export_type == "holywells":
            u = clean_value(row.get("tag:url:sketchfab"))
            if u and u.lower().startswith(("http://", "https://")):
                p8_urls.append(u)
            img = clean_value(row.get("tag:image"))
            if img and img.lower().startswith(("http://", "https://")):
                p8_urls.append(img)
        elif export_type == "ogham":
            for k in ("tag:url", "tag:website", "tag:image"):
                u = clean_value(row.get(k))
                if u and u.lower().startswith(("http://", "https://")):
                    p8_urls.append(u)

        if ENABLE_URL_EXPANSION and p8_urls:
            p8_urls = [maybe_expand_url(u) for u in p8_urls]

        p9_ids: List[str] = []
        ref = clean_value(row.get("tag:ref"))
        if ref:
            p9_ids.append(f"ref={ref}")

        for col in ref_cols:
            v = clean_value(row.get(col))
            if not v:
                continue
            key = col.replace("tag:", "")
            p9_ids.append(f"{key}={v}")

        if has_source_ref:
            v = clean_value(row.get("tag:source_ref"))
            if v:
                p9_ids.append(f"source_ref={v}")

        if has_source_colon_ref:
            v = clean_value(row.get("tag:source:ref"))
            if v:
                p9_ids.append(f"source:ref={v}")

        lines.append("CREATE")
        add_last("Len", f'"{qs_escape(label)}"')
        add_last("Den", f'"{qs_escape(desc)}"')

        for q in p1_list:
            add_last("P1", q)

        add_last("P3", str(el_id))
        add_last("P4", p4_item)
        add_last("P5", f"@{lat}/{lon}")

        if wd and wd.startswith("Q"):
            add_last("P6", f'"{qs_escape(wd)}"')

        wiki_url = wikipedia_to_url(wiki) if wiki else None
        if wiki_url:
            wiki_url = maybe_expand_url(wiki_url)
            add_last("P7", f'"{qs_escape(wiki_url)}"')

        for u in p8_urls:
            add_last("P8", f'"{qs_escape(u)}"')

        for s in p9_ids:
            add_last("P9", f'"{qs_escape(s)}"')

        add_last("P10", p10_item)
        add_last("P11", f'"{qs_escape(osm_element_url(el_type, el_id))}"')

        add_last("P12", f'"{qs_escape(now_edtf)}"')

        if ver_int is not None:
            add_last("P13", str(ver_int))
        if changeset_int is not None:
            add_last("P16", str(changeset_int))
        if ts_el:
            add_last("P17", f'"{qs_escape(ts_el)}"')

        lines.append("")

    qs_path.write_text("\n".join(lines), encoding="utf-8")
    return qs_path


# =================================================
# RDF writer (CHANGED SUBJECT URI)
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
    now_edtf = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    rows: List[Dict[str, Any]] = []
    for el in elements:
        pt = extract_point(el)
        if not pt:
            continue
        lat, lon = pt

        row: Dict[str, Any] = {
            "type": el.get("type"),
            "id": el.get("id"),
            "version": el.get("version"),
            "timestamp": el.get("timestamp"),
            "changeset": el.get("changeset"),
            "user": el.get("user"),
            "uid": el.get("uid"),
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
    g.bind("crm", CRM)
    g.bind("dcat", DCAT)
    g.bind("prov", PROV)
    g.bind("dcterms", DCTERMS)
    g.bind("foaf", FOAF)
    g.bind("geosparql", GEOSPARQL)
    g.bind("sf", SF)
    g.bind("owl", OWL)
    g.bind("wd", WD)

    dataset = URIRef(f"{OSM2LOD}{export_type}__{ts}")
    g.add((dataset, RDF.type, DCAT.Dataset))
    g.add((dataset, RDF.type, URIRef(f"{OSM2LOD}Dataset")))
    g.add((dataset, DCTERMS.title, Literal("OSM Overpass Export", lang="en")))
    g.add((dataset, DCTERMS.type, Literal(export_type)))
    g.add((dataset, DCTERMS.created, Literal(now_iso, datatype=XSD.dateTime)))
    g.add((dataset, DCTERMS.identifier, Literal(f"osm2lod:{export_type}:{ts}")))
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

    name_lang_cols: List[Tuple[str, str]] = []
    for col in df.columns:
        if not col.startswith("tag:name:"):
            continue
        lang = col.split("tag:name:", 1)[1].strip()
        if is_valid_lang_tag(lang):
            name_lang_cols.append((col, lang))

    export_specific_rdf_type = EXPORT_RDFTYPE.get(export_type)

    for _, row in df.iterrows():
        el_type = str(row["type"])
        el_id = int(row["id"])
        lat = float(row["lat"])
        lon = float(row["lon"])

        # NEW: internal subject URI
        rec = record_uri(el_type, el_id)

        # canonical external OSM URI reference
        osm_ref = osm_canonical_uri(el_type, el_id)

        g.add((rec, RDF.type, DCAT.Dataset))
        g.add((rec, RDF.type, entity_base_class))
        g.add((rec, RDF.type, URIRef(f"{OSM2LOD}OSMEntity")))

        # -------------------------------------------------
        # export-specific rdf:type (incl. drillcores split)
        # -------------------------------------------------
        if export_type == "drillcores":
            mm = clean_value(row.get("tag:man_made"))
            if mm and mm.strip().lower() == "bore_hole":
                g.add((rec, RDF.type, EXPORT_RDFTYPE["coreprofile"]))
            else:
                # default for all other records in drillcores export
                g.add((rec, RDF.type, EXPORT_RDFTYPE["maar"]))
        else:
            if export_specific_rdf_type is not None:
                g.add((rec, RDF.type, export_specific_rdf_type))

        g.add((rec, DCTERMS.isPartOf, dataset))

        # keep a stable link back to OSM
        g.add((rec, FOAF.primaryTopic, osm_ref))
        # optional: also assert equivalence
        # g.add((rec, OWL.sameAs, osm_ref))

        g.add((rec, DCTERMS.identifier, Literal(f"osm:{el_type}/{el_id}")))
        g.add((rec, DCTERMS.created, Literal(now_iso, datatype=XSD.dateTime)))

        if add_export_type_to_each_record:
            g.add((rec, URIRef(f"{OSM2LOD}exportType"), Literal(export_type)))

        label_written = False
        name_en = clean_value(row.get("tag:name:en"))
        name = clean_value(row.get("tag:name"))

        if name_en:
            g.add((rec, RDFS.label, Literal(name_en, lang="en")))
            label_written = True
        elif name:
            g.add((rec, RDFS.label, Literal(name, lang="en")))
            label_written = True

        for col, lang in name_lang_cols:
            val = clean_value(row.get(col))
            if val:
                g.add((rec, RDFS.label, Literal(val, lang=lang)))
                label_written = True

        if not label_written:
            g.add((rec, RDFS.label, Literal(f"OSM {el_type} {el_id}", lang="en")))

        geom = geom_uri(el_id)
        g.add((rec, GEOSPARQL.hasGeometry, geom))
        g.add((geom, RDF.type, SF.Point))
        g.add(
            (
                geom,
                GEOSPARQL.asWKT,
                Literal(wkt_point(lon, lat), datatype=GEOSPARQL.wktLiteral),
            )
        )

        doi_candidates: List[str] = []
        for key in tag_keys:
            col = f"tag:{key}"
            if col not in df.columns:
                continue
            raw = clean_value(row.get(col))
            if not raw:
                continue

            if key == "wikipedia":
                g.add((rec, osmtag_predicate(key), parse_wikipedia_literal(raw)))
                continue

            # Spezielle Behandlung für wikimedia_commons
            if key == "wikimedia_commons":
                # Erstelle Commons URI aus "File:..." oder direkter URL
                if raw.lower().startswith(("http://", "https://")):
                    commons_uri = URIRef(raw)
                elif raw.startswith("File:"):
                    # Konvertiere "File:XYZ.jpg" zu "https://commons.wikimedia.org/wiki/File:XYZ.jpg"
                    encoded_filename = quote(raw, safe=":")
                    commons_uri = URIRef(
                        f"https://commons.wikimedia.org/wiki/{encoded_filename}"
                    )
                else:
                    # Fallback: nehme an es ist ein Dateiname ohne "File:" Präfix
                    encoded_filename = quote(f"File:{raw}", safe=":")
                    commons_uri = URIRef(
                        f"https://commons.wikimedia.org/wiki/{encoded_filename}"
                    )
                g.add((rec, osmtag_predicate(key), commons_uri))
                continue

            if key in URL_TAG_KEYS and raw.lower().startswith(("http://", "https://")):
                g.add((rec, osmtag_predicate(key), URIRef(raw)))
            else:
                g.add((rec, osmtag_predicate(key), Literal(raw)))

            if key in ("source:ref", "source", "ref"):
                doi = extract_doi(raw)
                if doi:
                    doi_candidates.append(doi)

        wd = clean_value(row.get("tag:wikidata"))
        if wd and wd.startswith("Q"):
            g.add((rec, OWL.sameAs, WD[wd]))

        if doi_candidates:
            doi = doi_candidates[0]
            doi_uri = URIRef(f"https://doi.org/{doi}")
            g.add((rec, DCTERMS.identifier, doi_uri))
            g.add((rec, DCTERMS.source, doi_uri))

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
        dataset_uri=dataset,
    )

    qs_path = export_to_quickstatements(
        export_type=export_type,
        df=df,
        dist_dir=dist_dir,
        ts=ts,
        now_edtf=now_edtf,
    )

    print(f"✔ {export_type}: {len(df)} records")
    print(f"  → {csv_path.name}")
    print(f"  → {ttl_path.name}")
    print(f"  → {meta_path.name}")
    print(f"  → {qs_path.name}")
    if ENABLE_URL_EXPANSION:
        print(f"  → URL expansion cache size: {len(_URL_EXPAND_CACHE)}")


# =================================================
# Changelog generation
# =================================================


def find_previous_run_dir(base_dir: Path, current_date: str) -> Optional[Path]:
    """
    Findet den vorherigen Run-Ordner (der Ordner mit dem jüngsten Datum vor current_date).
    """
    if not base_dir.exists():
        return None

    run_dirs = []
    for p in base_dir.iterdir():
        if p.is_dir() and p.name != current_date:
            # Prüfe ob es ein Datums-Ordner ist (Format: YYYY-MM-DD)
            if re.match(r"^\d{4}-\d{2}-\d{2}$", p.name):
                run_dirs.append(p)

    if not run_dirs:
        return None

    # Sortiere nach Datum (Name ist im Format YYYY-MM-DD, sortiert lexikographisch korrekt)
    run_dirs.sort(key=lambda x: x.name)

    # Finde den jüngsten vor current_date
    for p in reversed(run_dirs):
        if p.name < current_date:
            return p

    return None


def get_all_run_dirs(base_dir: Path) -> List[Path]:
    """
    Findet alle Run-Ordner im base_dir und gibt sie chronologisch sortiert zurück.
    """
    if not base_dir.exists():
        return []

    run_dirs = []
    for p in base_dir.iterdir():
        if p.is_dir():
            # Prüfe ob es ein Datums-Ordner ist (Format: YYYY-MM-DD)
            if re.match(r"^\d{4}-\d{2}-\d{2}$", p.name):
                run_dirs.append(p)

    # Sortiere chronologisch (älteste zuerst)
    run_dirs.sort(key=lambda x: x.name)
    return run_dirs


def compare_csv_exports(
    old_csv: Path, new_csv: Path, export_type: str
) -> ChangelogReport:
    """
    Vergleicht zwei CSV-Exporte und findet Änderungen.
    """
    old_df = pd.read_csv(old_csv)
    new_df = pd.read_csv(new_csv)

    # Erstelle OSM IDs
    old_df["osm_id"] = old_df["type"] + "/" + old_df["id"].astype(str)
    new_df["osm_id"] = new_df["type"] + "/" + new_df["id"].astype(str)

    old_ids = set(old_df["osm_id"])
    new_ids = set(new_df["osm_id"])

    # Finde Änderungen
    added_ids = new_ids - old_ids
    deleted_ids = old_ids - new_ids
    common_ids = old_ids & new_ids

    added: List[ChangelogItem] = []
    deleted: List[ChangelogItem] = []
    modified: List[ChangelogItem] = []

    # Added items
    for osm_id in sorted(added_ids):
        row = new_df[new_df["osm_id"] == osm_id].iloc[0]
        name = clean_value(row.get("tag:name")) or "Unnamed"
        added.append(
            ChangelogItem(
                osm_id=osm_id,
                osm_type=row["type"],
                osm_numeric_id=int(row["id"]),
                name=name,
                new_version=(
                    int(row["version"]) if pd.notna(row.get("version")) else None
                ),
                new_timestamp=row.get("timestamp"),
            )
        )

    # Deleted items
    for osm_id in sorted(deleted_ids):
        row = old_df[old_df["osm_id"] == osm_id].iloc[0]
        name = clean_value(row.get("tag:name")) or "Unnamed"
        deleted.append(
            ChangelogItem(
                osm_id=osm_id,
                osm_type=row["type"],
                osm_numeric_id=int(row["id"]),
                name=name,
                old_version=(
                    int(row["version"]) if pd.notna(row.get("version")) else None
                ),
                old_timestamp=row.get("timestamp"),
            )
        )

    # Modified items (version oder timestamp geändert)
    for osm_id in sorted(common_ids):
        old_row = old_df[old_df["osm_id"] == osm_id].iloc[0]
        new_row = new_df[new_df["osm_id"] == osm_id].iloc[0]

        old_ver = int(old_row["version"]) if pd.notna(old_row.get("version")) else None
        new_ver = int(new_row["version"]) if pd.notna(new_row.get("version")) else None

        if old_ver != new_ver or old_row.get("timestamp") != new_row.get("timestamp"):
            name = clean_value(new_row.get("tag:name")) or "Unnamed"

            # Finde alle geänderten Felder
            field_changes: Dict[str, Tuple[Any, Any]] = {}

            # Vergleiche alle Spalten
            for col in new_df.columns:
                if col in ("osm_id", "type", "id"):  # Überspringe Meta-Felder
                    continue

                old_val = clean_value(old_row.get(col))
                new_val = clean_value(new_row.get(col))

                # Nur echte Änderungen tracken (nicht None -> None)
                if old_val != new_val and not (old_val is None and new_val is None):
                    # Formatiere Feldnamen schöner (entferne "tag:" prefix)
                    display_name = (
                        col.replace("tag:", "") if col.startswith("tag:") else col
                    )
                    field_changes[display_name] = (old_val, new_val)

            modified.append(
                ChangelogItem(
                    osm_id=osm_id,
                    osm_type=new_row["type"],
                    osm_numeric_id=int(new_row["id"]),
                    name=name,
                    old_version=old_ver,
                    new_version=new_ver,
                    old_timestamp=old_row.get("timestamp"),
                    new_timestamp=new_row.get("timestamp"),
                    field_changes=field_changes if field_changes else None,
                )
            )

    old_date = old_csv.parent.name
    new_date = new_csv.parent.name

    return ChangelogReport(
        export_type=export_type,
        old_date=old_date,
        new_date=new_date,
        added=added,
        deleted=deleted,
        modified=modified,
    )


def generate_changelog_html(reports: List[ChangelogReport], output_path: Path) -> None:
    """
    Generiert eine HTML-Datei mit allen Changelog-Reports.
    """
    html_parts = [
        "<!DOCTYPE html>",
        "<html lang='en'>",
        "<head>",
        "  <meta charset='UTF-8'>",
        "  <meta name='viewport' content='width=device-width, initial-scale=1.0'>",
        "  <title>OSM2LOD Changelog</title>",
        "  <style>",
        "    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 20px; background: #f5f5f5; }",
        "    .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "    h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }",
        "    h2 { color: #34495e; margin-top: 30px; border-left: 4px solid #3498db; padding-left: 15px; }",
        "    .date-range { background: #ecf0f1; padding: 10px 15px; border-radius: 5px; margin: 15px 0; font-size: 14px; }",
        "    .stats { display: flex; gap: 20px; margin: 20px 0; flex-wrap: wrap; }",
        "    .stat-box { flex: 1; min-width: 150px; padding: 15px; border-radius: 5px; text-align: center; }",
        "    .stat-box.added { background: #d4edda; border: 2px solid #28a745; }",
        "    .stat-box.modified { background: #fff3cd; border: 2px solid #ffc107; }",
        "    .stat-box.deleted { background: #f8d7da; border: 2px solid #dc3545; }",
        "    .stat-number { font-size: 32px; font-weight: bold; margin: 5px 0; }",
        "    .stat-label { font-size: 14px; color: #666; text-transform: uppercase; }",
        "    table { width: 100%; border-collapse: collapse; margin: 20px 0; }",
        "    th { background: #34495e; color: white; padding: 12px; text-align: left; font-weight: 600; }",
        "    td { padding: 10px; border-bottom: 1px solid #ddd; }",
        "    tr:hover { background: #f8f9fa; }",
        "    .osm-link { color: #3498db; text-decoration: none; font-family: monospace; }",
        "    .osm-link:hover { text-decoration: underline; }",
        "    .version-info { font-size: 12px; color: #7f8c8d; }",
        "    .section { margin: 30px 0; }",
        "    .section-title { color: #2c3e50; font-size: 18px; font-weight: 600; margin: 20px 0 10px 0; }",
        "    .empty-state { text-align: center; color: #95a5a6; padding: 40px; font-style: italic; }",
        "    .wikibase-action { background: #e8f4f8; padding: 15px; border-left: 4px solid #3498db; margin: 10px 0; }",
        "    .wikibase-action h3 { margin: 0 0 10px 0; color: #2980b9; font-size: 16px; }",
        "    .wikibase-action ul { margin: 5px 0; padding-left: 20px; }",
        "    .wikibase-action li { margin: 5px 0; }",
        "    details { margin: 10px 0; }",
        "    summary { cursor: pointer; padding: 8px; background: #f8f9fa; border-radius: 4px; font-weight: 500; user-select: none; }",
        "    summary:hover { background: #e9ecef; }",
        "    .field-changes { margin: 10px 0; padding: 10px; background: #f8f9fa; border-radius: 4px; }",
        "    .field-change { margin: 8px 0; padding: 8px; background: white; border-left: 3px solid #ffc107; }",
        "    .field-name { font-weight: 600; color: #495057; margin-bottom: 4px; font-size: 13px; }",
        "    .field-diff { display: flex; gap: 10px; align-items: center; font-family: monospace; font-size: 12px; }",
        "    .field-old { color: #dc3545; background: #f8d7da; padding: 4px 8px; border-radius: 3px; flex: 1; word-break: break-all; }",
        "    .field-new { color: #28a745; background: #d4edda; padding: 4px 8px; border-radius: 3px; flex: 1; word-break: break-all; }",
        "    .field-arrow { color: #6c757d; font-weight: bold; }",
        "    .field-null { color: #6c757d; font-style: italic; }",
        "  </style>",
        "</head>",
        "<body>",
        "  <div class='container'>",
        "    <h1>🔄 OSM2LOD Complete Changelog History</h1>",
        f"    <p>Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>",
    ]

    if not reports:
        html_parts.append(
            "    <div class='empty-state'>No changelog reports available.</div>"
        )

    # Erstelle Timeline-Übersicht wenn mehrere Reports vorhanden
    if len(reports) > 1:
        # Sammle alle einzigartigen Zeitvergleiche
        comparisons = set()
        for report in reports:
            comparisons.add((report.old_date, report.new_date))

        html_parts.extend(
            [
                "    <div class='timeline'>",
                f"      <div class='timeline-header'>📅 Timeline Overview ({len(comparisons)} comparisons)</div>",
            ]
        )

        for old_date, new_date in sorted(comparisons):
            html_parts.append(
                f"      <div class='timeline-item'>"
                f"<span class='timeline-date'>{old_date} → {new_date}</span></div>"
            )

        html_parts.append("    </div>")

    for report in reports:
        total_changes = len(report.added) + len(report.deleted) + len(report.modified)

        html_parts.extend(
            [
                f"    <h2>📦 {report.export_type.upper()}</h2>",
                f"    <div class='date-range'>",
                f"      <strong>Comparing:</strong> {report.old_date} → {report.new_date}",
                f"    </div>",
                f"    <div class='stats'>",
                f"      <div class='stat-box added'>",
                f"        <div class='stat-number'>{len(report.added)}</div>",
                f"        <div class='stat-label'>Added</div>",
                f"      </div>",
                f"      <div class='stat-box modified'>",
                f"        <div class='stat-number'>{len(report.modified)}</div>",
                f"        <div class='stat-label'>Modified</div>",
                f"      </div>",
                f"      <div class='stat-box deleted'>",
                f"        <div class='stat-number'>{len(report.deleted)}</div>",
                f"        <div class='stat-label'>Deleted</div>",
                f"      </div>",
                f"    </div>",
            ]
        )

        # Wikibase Actions Summary
        if total_changes > 0:
            html_parts.extend(
                [
                    "    <div class='wikibase-action'>",
                    "      <h3>📝 Wikibase Update Actions</h3>",
                    "      <ul>",
                ]
            )
            if report.added:
                html_parts.append(
                    f"        <li><strong>Create {len(report.added)} new items</strong> in Wikibase using QuickStatements</li>"
                )
            if report.modified:
                html_parts.append(
                    f"        <li><strong>Update {len(report.modified)} existing items</strong> with new data from OSM</li>"
                )
            if report.deleted:
                html_parts.append(
                    f"        <li><strong>Mark {len(report.deleted)} items as deleted/deprecated</strong> (no longer in OSM)</li>"
                )
            html_parts.extend(
                [
                    "      </ul>",
                    "    </div>",
                ]
            )

        # Added items
        if report.added:
            html_parts.extend(
                [
                    "    <div class='section'>",
                    "      <div class='section-title'>✅ Added Items</div>",
                    "      <table>",
                    "        <thead><tr><th>OSM ID</th><th>Name</th><th>Version</th></tr></thead>",
                    "        <tbody>",
                ]
            )
            for item in report.added:
                osm_url = f"https://www.openstreetmap.org/{item.osm_type}/{item.osm_numeric_id}"
                html_parts.append(
                    f"          <tr>"
                    f"<td><a href='{osm_url}' class='osm-link' target='_blank'>{item.osm_id}</a></td>"
                    f"<td>{item.name}</td>"
                    f"<td class='version-info'>v{item.new_version or '?'}</td>"
                    f"</tr>"
                )
            html_parts.extend(
                [
                    "        </tbody>",
                    "      </table>",
                    "    </div>",
                ]
            )

        # Modified items
        if report.modified:
            html_parts.extend(
                [
                    "    <div class='section'>",
                    "      <div class='section-title'>📝 Modified Items</div>",
                    "      <table>",
                    "        <thead><tr><th>OSM ID</th><th>Name</th><th>Version Change</th><th>Changes</th></tr></thead>",
                    "        <tbody>",
                ]
            )
            for item in report.modified:
                osm_url = f"https://www.openstreetmap.org/{item.osm_type}/{item.osm_numeric_id}"
                version_change = (
                    f"v{item.old_version or '?'} → v{item.new_version or '?'}"
                )

                # Zähle die Feldänderungen
                num_changes = len(item.field_changes) if item.field_changes else 0

                # Baue die Zeile
                row_html = f"          <tr><td><a href='{osm_url}' class='osm-link' target='_blank'>{item.osm_id}</a></td><td>{item.name}</td><td class='version-info'>{version_change}</td><td>"

                if item.field_changes and num_changes > 0:
                    # Erstelle Details/Summary für Änderungen
                    row_html += f"<details><summary>{num_changes} field(s) changed</summary><div class='field-changes'>"

                    for field_name, (old_val, new_val) in sorted(
                        item.field_changes.items()
                    ):
                        # Formatiere die Werte
                        old_display = (
                            old_val
                            if old_val is not None
                            else "<span class='field-null'>empty</span>"
                        )
                        new_display = (
                            new_val
                            if new_val is not None
                            else "<span class='field-null'>empty</span>"
                        )

                        # Kürze sehr lange Werte
                        if isinstance(old_display, str) and len(old_display) > 100:
                            old_display = old_display[:97] + "..."
                        if isinstance(new_display, str) and len(new_display) > 100:
                            new_display = new_display[:97] + "..."

                        row_html += f"""
                        <div class='field-change'>
                          <div class='field-name'>{field_name}</div>
                          <div class='field-diff'>
                            <div class='field-old'>{old_display}</div>
                            <div class='field-arrow'>→</div>
                            <div class='field-new'>{new_display}</div>
                          </div>
                        </div>"""

                    row_html += "</div></details>"
                else:
                    row_html += "<span class='version-info'>Timestamp only</span>"

                row_html += "</td></tr>"
                html_parts.append(row_html)

            html_parts.extend(
                [
                    "        </tbody>",
                    "      </table>",
                    "    </div>",
                ]
            )

        # Deleted items
        if report.deleted:
            html_parts.extend(
                [
                    "    <div class='section'>",
                    "      <div class='section-title'>🗑️ Deleted Items</div>",
                    "      <table>",
                    "        <thead><tr><th>OSM ID</th><th>Name</th><th>Last Version</th></tr></thead>",
                    "        <tbody>",
                ]
            )
            for item in report.deleted:
                osm_url = f"https://www.openstreetmap.org/{item.osm_type}/{item.osm_numeric_id}"
                html_parts.append(
                    f"          <tr>"
                    f"<td><a href='{osm_url}' class='osm-link' target='_blank'>{item.osm_id}</a></td>"
                    f"<td>{item.name}</td>"
                    f"<td class='version-info'>v{item.old_version or '?'}</td>"
                    f"</tr>"
                )
            html_parts.extend(
                [
                    "        </tbody>",
                    "      </table>",
                    "    </div>",
                ]
            )

        if total_changes == 0:
            html_parts.append("    <div class='empty-state'>No changes detected.</div>")

    html_parts.extend(
        [
            "  </div>",
            "</body>",
            "</html>",
        ]
    )

    output_path.write_text("\n".join(html_parts), encoding="utf-8")


def generate_changelog_for_run(
    base_dir: Path, current_run_dir: Path, export_types: List[str]
) -> Optional[Path]:
    """
    Generiert einen Changelog mit der kompletten Historie aller aufeinanderfolgenden Runs.
    """
    all_run_dirs = get_all_run_dirs(base_dir)

    if len(all_run_dirs) < 2:
        print("ℹ️  Less than 2 runs found, skipping changelog generation.")
        return None

    print(f"📊 Generating complete changelog history ({len(all_run_dirs)} runs)")
    print(f"   Date range: {all_run_dirs[0].name} → {all_run_dirs[-1].name}")

    # Sammle alle Reports für alle aufeinanderfolgenden Vergleiche
    all_reports: List[ChangelogReport] = []

    # Vergleiche jeden Run mit seinem Vorgänger
    for i in range(1, len(all_run_dirs)):
        old_run_dir = all_run_dirs[i - 1]
        new_run_dir = all_run_dirs[i]

        print(f"\n   Comparing: {old_run_dir.name} → {new_run_dir.name}")

        for export_type in export_types:
            # Finde CSV-Dateien für diesen Export-Typ
            old_csvs = list(old_run_dir.glob(f"osm_export_{export_type}_*.csv"))
            new_csvs = list(new_run_dir.glob(f"osm_export_{export_type}_*.csv"))

            if not old_csvs or not new_csvs:
                continue

            # Verwende die neueste Datei (sollte nur eine sein)
            old_csv = sorted(old_csvs)[-1]
            new_csv = sorted(new_csvs)[-1]

            try:
                report = compare_csv_exports(old_csv, new_csv, export_type)
                all_reports.append(report)

                total = len(report.added) + len(report.deleted) + len(report.modified)
                print(
                    f"     ✔ {export_type}: {total} changes ({len(report.added)} added, {len(report.modified)} modified, {len(report.deleted)} deleted)"
                )
            except Exception as e:
                print(f"     ✘ {export_type}: Error - {e}")

    if not all_reports:
        print("\nℹ️  No changelog reports generated.")
        return None

    # Generiere HTML im dist-Ordner mit allen Reports
    changelog_path = base_dir / "changelog.html"
    generate_changelog_html(all_reports, changelog_path)

    print(f"\n📄 Complete changelog history saved: {changelog_path}")
    print(f"   Total comparisons: {len(all_reports)}")
    return changelog_path


def main() -> None:
    DIST_BASE_DIR.mkdir(exist_ok=True)

    # Erstelle oder überschreibe den datumsspezifischen Unterordner
    run_dir = get_or_create_run_dir(DIST_BASE_DIR)

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
            dist_dir=run_dir,  # Verwende den datumsspezifischen Unterordner
            overpass_query=cfg["query"],
        )

    print()
    print("=" * 60)

    # Generiere Changelog
    changelog_path = generate_changelog_for_run(
        base_dir=DIST_BASE_DIR, current_run_dir=run_dir, export_types=exports_to_run
    )

    if changelog_path:
        print("=" * 60)
        print(f"✅ Run completed with changelog")
    else:
        print("=" * 60)
        print(f"✅ Run completed (first run, no changelog)")


if __name__ == "__main__":
    main()
