#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import shutil
import sys
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
(
  area(3600062273)->.roi;
  area(3600156393)->.ni;
);
(
  nwr(area.roi)["historic"="ogham_stone"];
  nwr(area.ni)["historic"="ogham_stone"];
);
out meta geom;
""",
        "entity_base_class": CRM.E22_Human_Made_Object,
    },
    "holywells": {
        "query": """
[out:json][timeout:25];
(
  area(3600062273)->.roi;
  area(3600156393)->.ni;
);
(
  nwr(area.roi)["place_of_worship"="holy_well"];
  nwr(area.ni)["place_of_worship"="holy_well"];
);
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
  node(1918839400);
  relation(9343404);
  node(13650180080);
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
  area(3600062273)->.roi;
  area(3600156393)->.ni;
  area(3600058446)->.scotland;
);
(
  nwr(area.roi)["man_made"="survey_point"]["benchmark"="yes"]["wikimedia_commons"];
  nwr(area.ni)["man_made"="survey_point"]["benchmark"="yes"]["wikimedia_commons"];
  nwr(area.scotland)["man_made"="survey_point"]["benchmark"="yes"]["wikimedia_commons"];
);
out meta geom;
""",
        "entity_base_class": CRM.E22_Human_Made_Object,
    },
    "sisal": {
        "query": """
[out:json][timeout:25];
(
  node(194177073);
  node(274302247);
  node(278574320);
  node(286783901);
  node(298946850);
  node(308354506);
  node(320120939);
  node(340386100);
  node(354842709);
  node(370538351);
  node(418784261);
  node(440011392);
  node(475710521);
  node(495996579);
  node(496887249);
  node(503858812);
  node(516610283);
  node(554858725);
  node(556748045);
  node(558629070);
  node(644792344);
  node(686817231);
  node(696792182);
  node(707254947);
  node(715290197);
  node(745919883);
  node(776040430);
  node(903676829);
  node(923447187);
  node(942519597);
  node(977664869);
  node(981440188);
  node(992092588);
  node(1017924795);
  node(1074302540);
  node(1123061406);
  node(1123061543);
  node(1123061583);
  node(1123061603);
  node(1123061615);
  node(1123061620);
  node(1174578048);
  node(1192434410);
  node(1201366333);
  node(1312824946);
  node(1325351976);
  node(1389940709);
  node(1488744824);
  node(1572294886);
  node(1825661457);
  node(1961032052);
  node(2163196165);
  node(2174721117);
  node(2264386190);
  node(2267151473);
  node(2285744211);
  node(2375460063);
  node(2382055156);
  node(2423303117);
  node(2483192759);
  node(2486226122);
  node(2495014138);
  node(2509040859);
  node(2510360072);
  node(2634652214);
  node(2677612603);
  node(2729783661);
  node(2824859401);
  node(2875516784);
  node(2970889616);
  node(2972577607);
  node(3096058666);
  node(3269827185);
  node(3292745497);
  node(3294134845);
  node(3420452460);
  node(3427512355);
  node(3440813505);
  node(3544073316);
  node(3570423898);
  node(3570423899);
  node(3657259352);
  node(3658668205);
  node(3783268558);
  node(3835819480);
  node(3887334792);
  node(3905429443);
  node(4018446140);
  node(4086768866);
  node(4100771303);
  node(4153268605);
  node(4153268606);
  node(4153384278);
  node(4209573222);
  node(4285191320);
  node(4306709100);
  node(4358968290);
  node(4376800615);
  node(4388840122);
  node(4480576512);
  node(4503846516);
  node(4523782074);
  node(4557134896);
  node(4705261283);
  node(4771754221);
  node(4848050737);
  node(4874702122);
  node(4917192240);
  node(4935677369);
  node(4998330664);
  node(5048285229);
  node(5058896537);
  node(5097880046);
  node(5204252292);
  node(5228393368);
  node(5382854703);
  node(5438822822);
  node(5459800715);
  node(5501815824);
  node(5503469510);
  node(5677995348);
  node(5894264165);
  node(5905508413);
  node(5909253343);
  node(5949505815);
  node(6050984397);
  node(6176987555);
  node(6632690571);
  node(6640740634);
  node(6957595702);
  node(6975770077);
  node(6986158611);
  node(6989696617);
  node(7008567688);
  node(7016608685);
  node(7161046616);
  node(7192265387);
  node(7206231475);
  node(7315412515);
  node(7318685910);
  node(7327265062);
  node(7412451779);
  node(7415334685);
  node(7713740998);
  node(7713840588);
  node(7713923814);
  node(7743122549);
  node(7746558018);
  node(7807817048);
  node(7885338123);
  node(8177727382);
  node(8349784316);
  node(8354378120);
  node(8358838507);
  node(8422630011);
  node(8755803759);
  node(8944365518);
  node(8972797679);
  node(9006583904);
  node(9007272454);
  node(9054470052);
  node(9098145258);
  node(9186866184);
  node(9247925660);
  node(9249065418);
  node(9693174633);
  node(9753722102);
  node(9801979591);
  node(9858176658);
  node(9887908804);
  node(9908902505);
  node(9913438479);
  node(9927050233);
  node(9958994170);
  node(10084842668);
  node(10110882090);
  node(10135300283);
  node(10135300285);
  node(10135316701);
  node(10302261247);
  node(10676966439);
  node(10768872262);
  node(10773895664);
  node(10903529963);
  node(10905682924);
  node(10926006942);
  node(10969878099);
  node(11135158274);
  node(11268539252);
  node(11298621603);
  node(11300105382);
  node(11423443988);
  node(11443521790);
  node(11478460514);
  node(11520700149);
  node(11687476904);
  node(11737465821);
  node(11876346537);
  node(11876859066);
  node(11954189744);
  node(12080122765);
  node(12150637414);
  node(12184077155);
  node(12497973563);
  node(12521313377);
  node(12595360316);
  node(12879668276);
  node(13014990612);
  node(13028255810);
  node(13073296633);
  node(13093008192);
  node(13130638123);
  node(13152004017);
  node(13169167192);
  node(13307603920);
  node(13324454762);
  node(13404301868);
  node(13556252331);
  node(13566719139);
  node(13581390702);
  node(13586189099);
);
out meta geom;
""",
        "entity_base_class": CRM.E26_Physical_Feature,
    },
    "romansites": {
        "query": """
[out:json][timeout:25];
(
  way(104871217);
  way(168318389);
  node(6358516450);
  node(3483897061);
  way(157929084);
  way(32545380);
  node(1796082995);
  node(7878861331);
  way(699387398);
  way(346290310);
  node(333175552);
  way(435021824);
  node(1930543644);
  node(6654634631);
  way(1054468269);
);
out meta geom;
""",
        "entity_base_class": CRM.E26_Physical_Feature,
    },
    "hogbacks": {
        "query": """
[out:json][timeout:25];
node["historic"="hogback"];
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
    "benchmarks": "Q890",
    "sisal": "Q894",
    "romansites": "Q895",
    "hogbacks": "Q897",
}

P1_FIXED = {
    "ogham": ["Q12"],
    "holywells": ["Q14"],
    "ci": ["Q21", "Q22"],
    "sisal": ["Q892"],
    "romansites": ["Q893"],
    "hogbacks": ["Q896"],
}

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
    "sisal": URIRef(f"{OSM2LOD}SisalSite"),
    "romansites": URIRef(f"{OSM2LOD}RomanSite"),
    "hogbacks": URIRef(f"{OSM2LOD}HogbackStone"),
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

    # QuickStatements - auskommentiert, wird jetzt über DIFF-QS generiert
    # qs_path = export_to_quickstatements(
    #     export_type=export_type,
    #     df=df,
    #     dist_dir=dist_dir,
    #     ts=ts,
    #     now_edtf=now_edtf,
    # )

    print(f"✔ {export_type}: {len(df)} records")
    print(f"  → {csv_path.name}")
    print(f"  → {ttl_path.name}")
    print(f"  → {meta_path.name}")
    # print(f"  → {qs_path.name}")  # auskommentiert
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

            # Skip wenn neue CSV nicht existiert
            if not new_csvs:
                continue

            new_csv = sorted(new_csvs)[-1]

            # Wenn alte CSV nicht existiert → alle sind "added"
            if not old_csvs:
                try:
                    new_df = pd.read_csv(new_csv)
                    if len(new_df) == 0:
                        continue  # Skip leere CSVs

                    added: List[ChangelogItem] = []
                    new_df["osm_id"] = new_df["type"] + "/" + new_df["id"].astype(str)

                    for osm_id in sorted(new_df["osm_id"]):
                        row = new_df[new_df["osm_id"] == osm_id].iloc[0]
                        name = clean_value(row.get("tag:name")) or "Unnamed"
                        added.append(
                            ChangelogItem(
                                osm_id=osm_id,
                                osm_type=row["type"],
                                osm_numeric_id=int(row["id"]),
                                name=name,
                                new_version=(
                                    int(row["version"])
                                    if pd.notna(row.get("version"))
                                    else None
                                ),
                                new_timestamp=row.get("timestamp"),
                            )
                        )

                    report = ChangelogReport(
                        export_type=export_type,
                        old_date=old_run_dir.name,
                        new_date=new_run_dir.name,
                        added=added,
                        deleted=[],
                        modified=[],
                    )
                    all_reports.append(report)
                    print(
                        f"     ✔ {export_type}: {len(added)} changes ({len(added)} added, 0 modified, 0 deleted)"
                    )
                except Exception as e:
                    print(f"     ✘ {export_type}: Error - {e}")
                continue

            # Beide CSVs existieren → normaler Vergleich
            old_csv = sorted(old_csvs)[-1]

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


# =================================================
# Wikibase Diff Generator
# =================================================

WIKIBASE_SPARQL_ENDPOINT = "https://osm2wiki.wikibase.cloud/query/sparql"
WIKIBASE_ENTITY_PREFIX = "https://osm2wiki.wikibase.cloud/entity/"


def normalize_wikipedia(value: str) -> str:
    """Normalisiert Wikipedia-Werte für Vergleich (OSM-Format vs URL)."""
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


def fetch_wikibase_items(export_type: str, query_item_qid: str) -> List[Dict[str, Any]]:
    """Holt alle Items des Export-Typs aus der Wikibase via SPARQL."""

    sparql_query = f"""
PREFIX osmwd: <{WIKIBASE_ENTITY_PREFIX}>
PREFIX osmwdt: <https://osm2wiki.wikibase.cloud/prop/direct/>

SELECT ?item ?itemLabel ?itemDescription 
       ?osmid ?osmtype ?geo ?version ?osmchangeset ?osmtimestamp
       ?wikidataid ?wikipedia ?osmurl
WHERE {{ 
  ?item osmwdt:P3 ?osmid .
  ?item osmwdt:P4 ?osmtype .
  ?item osmwdt:P5 ?geo .
  ?item osmwdt:P11 ?osmurl .
  ?item osmwdt:P13 ?version .
  ?item osmwdt:P16 ?osmchangeset .
  
  OPTIONAL {{ ?item osmwdt:P6 ?wikidataid . }}
  OPTIONAL {{ ?item osmwdt:P7 ?wikipedia . }}
  OPTIONAL {{ ?item osmwdt:P17 ?osmtimestamp . }}
  
  ?item osmwdt:P10 osmwd:{query_item_qid} .
  
  SERVICE wikibase:label {{ 
    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". 
  }} 
}}
"""

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

        items_dict: Dict[str, Dict[str, Any]] = {}

        for binding in bindings:
            qid = binding["item"]["value"].split("/")[-1]

            if qid not in items_dict:
                osmtype_uri = binding.get("osmtype", {}).get("value", "")
                osmtype_label = osmtype_uri.split("/")[-1]
                type_map = {"Q5": "node", "Q6": "way", "Q7": "relation"}
                osm_type = type_map.get(osmtype_label, osmtype_label)

                # Parse Koordinaten aus WKT Format
                geo_wkt = binding.get("geo", {}).get("value", "")
                coordinates = ""
                if geo_wkt:
                    import re

                    match = re.match(r"Point\(([^ ]+) ([^ ]+)\)", geo_wkt)
                    if match:
                        lon, lat = match.groups()
                        coordinates = f"{lat}/{lon}"
                    else:
                        coordinates = geo_wkt

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
                }

        return list(items_dict.values())

    except Exception as e:
        print(f"⚠️  Error querying Wikibase for {export_type}: {e}")
        return []


def generate_diff_quickstatements_for_run(
    base_dir: Path, run_dir: Path, export_types: List[str]
) -> Optional[Path]:
    """
    Generiert Diff-QuickStatements für alle Export-Typen.
    Schreibt Dateien in run_dir und Report in base_dir.
    """

    print()
    print("=" * 60)
    print("🔄 Generating Diff-QuickStatements vs Wikibase")
    print("=" * 60)

    report_lines = []
    report_lines.append("=" * 60)
    report_lines.append("🔄 DIFF-QUICKSTATEMENTS GENERATION")
    report_lines.append("=" * 60)
    report_lines.append(f"Run directory: {run_dir.name}")
    report_lines.append("")

    total_added = 0
    total_modified = 0
    total_deprecated = 0

    for export_type in export_types:
        print(f"\n📊 {export_type.upper()}")
        report_lines.append(f"{'='*60}")
        report_lines.append(f"{export_type.upper()}")
        report_lines.append(f"{'='*60}")

        # Lade OSM CSV
        csv_pattern = f"osm_export_{export_type}_*.csv"
        csv_files = list(run_dir.glob(csv_pattern))

        if not csv_files:
            msg = f"⚠️  No CSV found: {csv_pattern}"
            print(f"   {msg}")
            report_lines.append(msg)
            report_lines.append("")
            continue

        osm_df = pd.read_csv(csv_files[0])
        print(f"   OSM: {len(osm_df)} items")
        report_lines.append(f"OSM items: {len(osm_df)}")

        # Hole Wikibase Items
        query_item = P10_QUERY_ITEM.get(export_type)
        if not query_item:
            print(f"   ⚠️  No Q-ID configured for {export_type}")
            report_lines.append("⚠️  No Q-ID configured")
            report_lines.append("")
            continue

        wb_items = fetch_wikibase_items(export_type, query_item)
        print(f"   Wikibase: {len(wb_items)} items")
        report_lines.append(f"Wikibase items: {len(wb_items)}")

        if not wb_items:
            print(f"   ⚠️  No items in Wikibase yet")
            report_lines.append("⚠️  No items in Wikibase yet - skipping diff")
            report_lines.append("")
            continue

        # Erstelle Indices
        wb_index = {f"{it['osm_type']}/{it['osm_id']}": it for it in wb_items}
        osm_index = {}
        for _, row in osm_df.iterrows():
            key = f"{row['type']}/{row['id']}"
            osm_index[key] = {
                "osm_id": int(row["id"]),
                "osm_type": row["type"],
                "version": int(row["version"]) if pd.notna(row.get("version")) else 0,
                "changeset": (
                    int(row["changeset"]) if pd.notna(row.get("changeset")) else 0
                ),
                "osm_timestamp": row.get("timestamp", ""),
                "coordinates": f"{row['lat']}/{row['lon']}",
                "wikidata": (
                    str(row.get("tag:wikidata", ""))
                    if pd.notna(row.get("tag:wikidata"))
                    else ""
                ),
                "wikipedia": (
                    str(row.get("tag:wikipedia", ""))
                    if pd.notna(row.get("tag:wikipedia"))
                    else ""
                ),
            }

        # Analyse
        osm_keys = set(osm_index.keys())
        wb_keys = set(wb_index.keys())
        added = len(osm_keys - wb_keys)
        deleted = len(wb_keys - osm_keys)
        common = len(osm_keys & wb_keys)

        # Zähle Modified
        modified = 0
        for key in osm_keys & wb_keys:
            osm_it = osm_index[key]
            wb_it = wb_index[key]

            if osm_it["version"] != wb_it["version"]:
                modified += 1
                continue

            if osm_it["wikidata"] != wb_it["wikidata"]:
                modified += 1
                continue

            osm_wiki_norm = normalize_wikipedia(osm_it["wikipedia"])
            wb_wiki_norm = normalize_wikipedia(wb_it["wikipedia"])
            if osm_wiki_norm != wb_wiki_norm:
                modified += 1
                continue

        print(f"   Diff: +{added} ~{modified} -{deleted}")
        report_lines.append(f"Added: {added}")
        report_lines.append(f"Modified: {modified}")
        report_lines.append(f"Deprecated: {deleted}")
        report_lines.append("")

        total_added += added
        total_modified += modified
        total_deprecated += deleted

        # Generiere QuickStatements-Datei
        # (vollständige Logik hier aus dem test_diff_generator.py kopieren)
        # Für jetzt nur ein Platzhalter
        qs_file = run_dir / f"quickstatements_DIFF_{export_type}_{run_dir.name}.txt"
        qs_file.write_text(
            f"# Diff for {export_type}: +{added} ~{modified} -{deleted}\n",
            encoding="utf-8",
        )
        print(f"   ✅ {qs_file.name}")

    # Summary
    report_lines.append(f"{'='*60}")
    report_lines.append("TOTAL SUMMARY")
    report_lines.append(f"{'='*60}")
    report_lines.append(f"Total Added: {total_added}")
    report_lines.append(f"Total Modified: {total_modified}")
    report_lines.append(f"Total Deprecated: {total_deprecated}")

    # Schreibe Report
    report_path = base_dir / "diff_report.txt"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    print()
    print(f"✅ Diff-QuickStatements generated")
    print(f"   Report: {report_path}")

    return report_path


# =================================================
# OWL Ontology Export
# =================================================


def export_owl_ontology(dist_dir: Path) -> Path:
    """
    Generates a full OWL ontology for the osm2lod vocabulary and writes it
    to dist/osm2lod_ontology.ttl.

    Covers:
    - Ontology metadata (owl:Ontology)
    - All osm2lod domain classes with rdfs:subClassOf alignments to CRM / GeoSPARQL / DCAT
    - All object and datatype properties (osmtag__*, exportType, etc.)
    - Alignment axioms to external ontologies (CRM, GeoSPARQL, DCAT, DCTERMS, FOAF, OWL)
    """
    from rdflib.namespace import SKOS

    g = Graph()

    # ── Namespaces ────────────────────────────────────────────────────────────
    g.bind("osm2lod", OSM2LOD)
    g.bind("owl", OWL)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)
    g.bind("dcterms", DCTERMS)
    g.bind("dcat", DCAT)
    g.bind("crm", CRM)
    g.bind("geosparql", GEOSPARQL)
    g.bind("foaf", FOAF)
    g.bind("skos", SKOS)
    g.bind("prov", PROV)

    now_iso = datetime.now(timezone.utc).isoformat()

    # ── owl:Ontology ──────────────────────────────────────────────────────────
    ONT = URIRef(f"{OSM2LOD}ontology")
    g.add((ONT, RDF.type, OWL.Ontology))
    g.add((ONT, RDFS.label, Literal("osm2lod Ontology", lang="en")))
    g.add(
        (
            ONT,
            DCTERMS.title,
            Literal("osm2lod – OSM to Linked Open Data Vocabulary", lang="en"),
        )
    )
    g.add(
        (
            ONT,
            DCTERMS.description,
            Literal(
                "Vocabulary for representing OpenStreetMap features as Linked Data, "
                "aligned to CIDOC-CRM, GeoSPARQL, DCAT and Dublin Core.",
                lang="en",
            ),
        )
    )
    g.add((ONT, DCTERMS.creator, Literal("Research Squirrel Engineers")))
    g.add((ONT, DCTERMS.created, Literal(now_iso, datatype=XSD.dateTime)))
    g.add(
        (ONT, DCTERMS.license, URIRef("https://creativecommons.org/licenses/by/4.0/"))
    )
    g.add((ONT, OWL.versionInfo, Literal(now_iso)))
    g.add(
        (ONT, URIRef(f"{OWL}imports"), URIRef("http://www.opengis.net/ont/geosparql"))
    )

    # ── Helper lambdas ────────────────────────────────────────────────────────
    def add_class(
        uri: URIRef,
        label: str,
        comment: str,
        parent: URIRef = None,
        equiv: URIRef = None,
        subclass_of: list = None,
    ) -> None:
        g.add((uri, RDF.type, OWL.Class))
        g.add((uri, RDFS.label, Literal(label, lang="en")))
        g.add((uri, RDFS.comment, Literal(comment, lang="en")))
        g.add((uri, RDFS.isDefinedBy, ONT))
        if parent:
            g.add((uri, RDFS.subClassOf, parent))
        if equiv:
            g.add((uri, OWL.equivalentClass, equiv))
        for sc in subclass_of or []:
            g.add((uri, RDFS.subClassOf, sc))

    def add_obj_prop(
        uri: URIRef,
        label: str,
        comment: str,
        domain: URIRef = None,
        range_: URIRef = None,
        subprop: URIRef = None,
        equiv: URIRef = None,
    ) -> None:
        g.add((uri, RDF.type, OWL.ObjectProperty))
        g.add((uri, RDFS.label, Literal(label, lang="en")))
        g.add((uri, RDFS.comment, Literal(comment, lang="en")))
        g.add((uri, RDFS.isDefinedBy, ONT))
        if domain:
            g.add((uri, RDFS.domain, domain))
        if range_:
            g.add((uri, RDFS.range, range_))
        if subprop:
            g.add((uri, RDFS.subPropertyOf, subprop))
        if equiv:
            g.add((uri, OWL.equivalentProperty, equiv))

    def add_data_prop(
        uri: URIRef,
        label: str,
        comment: str,
        domain: URIRef = None,
        range_: URIRef = None,
        subprop: URIRef = None,
    ) -> None:
        g.add((uri, RDF.type, OWL.DatatypeProperty))
        g.add((uri, RDFS.label, Literal(label, lang="en")))
        g.add((uri, RDFS.comment, Literal(comment, lang="en")))
        g.add((uri, RDFS.isDefinedBy, ONT))
        if domain:
            g.add((uri, RDFS.domain, domain))
        if range_:
            g.add((uri, RDFS.range, range_))
        if subprop:
            g.add((uri, RDFS.subPropertyOf, subprop))

    # ── Core abstract classes ─────────────────────────────────────────────────
    OSM_ENTITY = URIRef(f"{OSM2LOD}OSMEntity")
    OSM_DATASET = URIRef(f"{OSM2LOD}Dataset")

    add_class(
        OSM_ENTITY,
        label="OSM Entity",
        comment="Abstract superclass for all OpenStreetMap features represented as Linked Data.",
        subclass_of=[DCAT.Dataset],
    )
    add_class(
        OSM_DATASET,
        label="osm2lod Dataset",
        comment="A DCAT dataset produced by an osm2lod export run.",
        subclass_of=[DCAT.Dataset],
    )

    # ── Domain classes (one per export type + internal subtypes) ─────────────
    CLASS_DEFS = [
        (
            URIRef(f"{OSM2LOD}OghamStone"),
            "Ogham Stone",
            "An early medieval inscribed standing stone bearing Ogham script.",
            OSM_ENTITY,
            [CRM.E22_Human_Made_Object],
        ),
        (
            URIRef(f"{OSM2LOD}HolyWell"),
            "Holy Well",
            "A sacred water site, typically a natural spring venerated in folk or religious tradition.",
            OSM_ENTITY,
            [CRM.E26_Physical_Feature],
        ),
        (
            URIRef(f"{OSM2LOD}CI_Findspot"),
            "CI Findspot",
            "A Palaeolithic cave or rock-shelter site yielding Continuum of Innovation (CI) artefacts.",
            OSM_ENTITY,
            [CRM.E55_Place],
        ),
        (
            URIRef(f"{OSM2LOD}Maar"),
            "Maar",
            "A low-relief volcanic crater formed by a phreatomagmatic eruption.",
            OSM_ENTITY,
            [CRM.E55_Place],
        ),
        (
            URIRef(f"{OSM2LOD}CoreProfile"),
            "Core Profile",
            "A drill-core borehole profile associated with a volcanic maar site.",
            OSM_ENTITY,
            [CRM.E55_Place],
        ),
        (
            URIRef(f"{OSM2LOD}Benchmark"),
            "Benchmark",
            "An Ordnance Survey benchmark / geodetic survey point with a Wikimedia Commons photograph.",
            OSM_ENTITY,
            [CRM.E22_Human_Made_Object],
        ),
        (
            URIRef(f"{OSM2LOD}SisalSite"),
            "SISAL Site",
            "A speleothem cave site included in the SISAL (Speleothem Isotopes Synthesis and Analysis) database.",
            OSM_ENTITY,
            [CRM.E26_Physical_Feature],
        ),
        (
            URIRef(f"{OSM2LOD}RomanSite"),
            "Roman Site",
            "A Roman-period archaeological site.",
            OSM_ENTITY,
            [CRM.E26_Physical_Feature],
        ),
        (
            URIRef(f"{OSM2LOD}HogbackStone"),
            "Hogback Stone",
            "A Viking-Age carved recumbent monument, typically found in northern England and Scotland.",
            OSM_ENTITY,
            [CRM.E22_Human_Made_Object],
        ),
    ]

    for uri, label, comment, parent, subclass_of in CLASS_DEFS:
        add_class(
            uri, label=label, comment=comment, parent=parent, subclass_of=subclass_of
        )

    # ── Object properties ─────────────────────────────────────────────────────

    # geospatial
    add_obj_prop(
        GEOSPARQL.hasGeometry,
        label="has geometry",
        comment="Links an OSM entity to its GeoSPARQL geometry node.",
        domain=OSM_ENTITY,
        range_=URIRef("http://www.opengis.net/ont/geosparql#Geometry"),
    )

    # provenance / OSM reference
    add_obj_prop(
        FOAF.primaryTopic,
        label="primary topic",
        comment="Links an osm2lod record to the canonical OpenStreetMap URI of the original element.",
        domain=OSM_ENTITY,
        range_=URIRef("http://www.w3.org/2002/07/owl#Thing"),
    )

    # dataset membership
    add_obj_prop(
        DCTERMS.isPartOf,
        label="is part of",
        comment="Links an OSM entity record to the osm2lod Dataset it was exported in.",
        domain=OSM_ENTITY,
        range_=OSM_DATASET,
    )

    # Wikidata alignment
    add_obj_prop(
        OWL.sameAs,
        label="same as (Wikidata)",
        comment="owl:sameAs link to the corresponding Wikidata entity (Q-item).",
        domain=OSM_ENTITY,
        range_=URIRef("http://www.wikidata.org/entity/Q35120"),  # wd:Q35120 = entity
    )

    # ── Datatype properties ───────────────────────────────────────────────────

    # exportType
    add_data_prop(
        URIRef(f"{OSM2LOD}exportType"),
        label="export type",
        comment="Identifies the osm2lod export category this entity belongs to "
        "(e.g. 'ogham', 'holywells', 'sisal').",
        domain=OSM_ENTITY,
        range_=XSD.string,
    )

    # OSM tag properties — one DatatypeProperty per CORE_TAG_KEY + extra keys
    all_tag_keys: List[str] = sorted(
        set(
            CORE_TAG_KEYS + [k for keys in EXPORT_EXTRA_TAG_KEYS.values() for k in keys]
        )
    )

    TAG_PROP_LABELS: Dict[str, str] = {
        "name": "name",
        "alt_name": "alternative name",
        "int_name": "international name",
        "loc_name": "local name",
        "description": "description",
        "note": "note",
        "source": "source",
        "source:ref": "source reference",
        "source:url": "source URL",
        "ref": "reference identifier",
        "access": "access restriction",
        "operator": "operator",
        "historic": "historic feature type",
        "historic:civilization": "historic civilisation",
        "place_of_worship": "place of worship type",
        "religion": "religion",
        "denomination": "denomination",
        "natural": "natural feature type",
        "man_made": "man-made feature type",
        "water": "water feature type",
        "water_source": "water source",
        "tourism": "tourism type",
        "archaeological_site": "archaeological site type",
        "ele": "elevation",
        "wikidata": "Wikidata QID (tag)",
        "wikipedia": "Wikipedia article (tag)",
        "wikimedia_commons": "Wikimedia Commons file (tag)",
        "image": "image URL",
        "website": "website URL",
        "url": "URL",
        "volcano:status": "volcano status",
        "volcano:type": "volcano type",
        "benchmark": "benchmark type",
        "survey:date": "survey date",
        "survey_point:structure": "survey point structure",
    }

    TAG_PROP_RANGES: Dict[str, URIRef] = {
        "source:url": XSD.anyURI,
        "website": XSD.anyURI,
        "url": XSD.anyURI,
        "image": XSD.anyURI,
        "wikimedia_commons": XSD.anyURI,
        "ele": XSD.decimal,
        "survey:date": XSD.date,
    }

    for key in all_tag_keys:
        safe = key.replace(":", "__")
        prop = URIRef(f"{OSM2LOD}osmtag__{safe}")
        label = TAG_PROP_LABELS.get(key, key.replace(":", " ").replace("_", " "))
        comment = f"OSM tag '{key}' mapped as an osm2lod datatype property."
        range_ = TAG_PROP_RANGES.get(key, XSD.string)

        # URL-valued tags become object properties
        if range_ == XSD.anyURI:
            add_obj_prop(
                prop,
                label=f"OSM tag: {label}",
                comment=comment,
                domain=OSM_ENTITY,
                range_=URIRef("http://www.w3.org/2002/07/owl#Thing"),
            )
        else:
            add_data_prop(
                prop,
                label=f"OSM tag: {label}",
                comment=comment,
                domain=OSM_ENTITY,
                range_=range_,
            )

    # ── Geometry datatype property ────────────────────────────────────────────
    add_data_prop(
        GEOSPARQL.asWKT,
        label="as WKT",
        comment="GeoSPARQL WKT literal representing the geometry of an OSM entity "
        "(CRS: EPSG:4326, format: POINT(lon lat)).",
        domain=URIRef("http://www.opengis.net/ont/geosparql#Geometry"),
        range_=GEOSPARQL.wktLiteral,
    )

    # ── Alignment axioms (rdfs:subClassOf to external ontologies) ─────────────
    # These are already expressed via subclass_of in CLASS_DEFS above.
    # Add explicit cross-ontology notes as rdfs:seeAlso
    alignments = [
        (URIRef(f"{OSM2LOD}OghamStone"), CRM.E22_Human_Made_Object),
        (URIRef(f"{OSM2LOD}HolyWell"), CRM.E26_Physical_Feature),
        (URIRef(f"{OSM2LOD}CI_Findspot"), CRM.E55_Place),
        (URIRef(f"{OSM2LOD}Maar"), CRM.E55_Place),
        (URIRef(f"{OSM2LOD}CoreProfile"), CRM.E55_Place),
        (URIRef(f"{OSM2LOD}Benchmark"), CRM.E22_Human_Made_Object),
        (URIRef(f"{OSM2LOD}SisalSite"), CRM.E26_Physical_Feature),
        (URIRef(f"{OSM2LOD}RomanSite"), CRM.E26_Physical_Feature),
        (URIRef(f"{OSM2LOD}HogbackStone"), CRM.E22_Human_Made_Object),
        (OSM_ENTITY, DCAT.Dataset),
        (OSM_DATASET, DCAT.Dataset),
    ]
    for cls, ext in alignments:
        g.add((cls, RDFS.seeAlso, ext))

    # ── Serialize ─────────────────────────────────────────────────────────────
    owl_path = dist_dir / "osm2lod_ontology.ttl"
    g.serialize(owl_path, format="turtle")

    triple_count = len(g)
    print(f"🦉 OWL ontology written: {owl_path}")
    print(f"   Classes   : {sum(1 for s in g.subjects(RDF.type, OWL.Class))}")
    print(f"   Obj props : {sum(1 for s in g.subjects(RDF.type, OWL.ObjectProperty))}")
    print(
        f"   Data props: {sum(1 for s in g.subjects(RDF.type, OWL.DatatypeProperty))}"
    )
    print(f"   Triples   : {triple_count}")

    return owl_path


def main() -> None:
    DIST_BASE_DIR.mkdir(exist_ok=True)

    # Erstelle oder überschreibe den datumsspezifischen Unterordner
    run_dir = get_or_create_run_dir(DIST_BASE_DIR)

    # Start Report Logging (kompletter Terminal-Output)
    report_path = DIST_BASE_DIR / "report.txt"

    with ReportLogger(report_path):
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

        # Generiere OWL Ontologie (einmalig in dist/)
        export_owl_ontology(DIST_BASE_DIR)

        print()
        print("=" * 60)

        # Generiere Changelog
        changelog_path = generate_changelog_for_run(
            base_dir=DIST_BASE_DIR, current_run_dir=run_dir, export_types=exports_to_run
        )

        # Generiere Diff-QuickStatements (via externes Skript)
        print()
        print("=" * 60)
        print("🔄 Generating Diff-QuickStatements vs Wikibase")
        print("=" * 60)

        diff_script = Path(__file__).parent / "generate_diff_quickstatements.py"
        if diff_script.exists():
            import subprocess

            result = subprocess.run(
                [sys.executable, str(diff_script), str(run_dir.name)],
                capture_output=False,
            )
            if result.returncode == 0:
                print("✅ Diff-QuickStatements generated")
            else:
                print("⚠️  Diff generation failed")
        else:
            print(f"⚠️  Diff script not found: {diff_script}")

        if changelog_path:
            print("=" * 60)
            print(f"✅ Run completed with changelog and diff")
        else:
            print("=" * 60)
            print(f"✅ Run completed (first run, no changelog)")

        print()
        print(f"📄 Full report saved to: {report_path}")
        print("=" * 60)


if __name__ == "__main__":
    main()
