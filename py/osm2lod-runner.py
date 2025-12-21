#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import time
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests
import pandas as pd
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, OWL, DCTERMS, FOAF


# =================================================
# Namespaces
# =================================================

OSM2LOD = Namespace("https://research-squirrel-engineers.github.io/osm2lod/")
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
    "drillcores": {
        "query": """
[out:json][timeout:25];
(
  node(13386703821);
  node(13386786938);
  way(23450728);
  way(56428302);
  node(13200939169);
  node(13200972050);
  node(13386723672);
  node(13200955773);
  node(13200943487);
  node(13388651352);
  node(13388641643);
);
out meta geom;
""",
        "entity_base_class": CRM.E55_Place,
    },
}

SELECTED_EXPORTS: List[str] = [
    # "ogham",
    # "holywells",
    # "ci",
    # "drillcores",
]

DIST_DIR = Path("dist")
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
}

URL_TAG_KEYS = {"source:url", "website", "url", "image", "wikimedia_commons"}

P10_QUERY_ITEM = {"ogham": "Q24", "holywells": "Q25", "ci": "Q26", "drillcores": "Q27"}

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


def clear_dist(dist_dir: Path) -> None:
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
                r = requests.post(endpoint, data={"data": query}, timeout=240)
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


def main() -> None:
    DIST_DIR.mkdir(exist_ok=True)

    if CLEAR_DIST_ON_START:
        clear_dist(DIST_DIR)

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
            dist_dir=DIST_DIR,
            overpass_query=cfg["query"],
        )


if __name__ == "__main__":
    main()
