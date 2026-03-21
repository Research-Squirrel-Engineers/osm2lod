# SISAL-OSM Mapper

**Verknüpft SISAL-Höhlenstandorte mit OpenStreetMap-Daten und analysiert die Match-Qualität**

Entwickelt für das **EPICA-SISAL-FAIRification** Projekt von Florian Thiery.

---

## 🎯 Was macht das Tool?

Dieses Tool:

1. **Lädt SISAL-Höhlendaten** (305 Speleothem-Standorte weltweit)
2. **Sucht in OpenStreetMap** nach Höhlen im Umkreis jedes Standorts (Standard: 5 km)
3. **Berechnet Match-Qualität** anhand von:
   - 📏 **Distanz** (Haversine-Formel in km)
   - 📝 **Namen-Ähnlichkeit** (SequenceMatcher, 0.0-1.0)
   - 🎯 **Composite Score** (70% Name + 30% Nähe)
4. **Erstellt detaillierte Berichte** mit:
   - Match-Status je Site (gefunden / nicht gefunden / warum)
   - Statistiken (Distanzen, Ähnlichkeiten)
   - Overpass-Turbo-URLs für manuelle Verifikation
5. **Exportiert CSV-Dateien** für Excel/Analyse

---

## 📋 Installation (Windows)

### 1. Python installieren (falls nicht vorhanden)

- Download: [python.org/downloads](https://www.python.org/downloads/)
- **Wichtig:** "Add Python to PATH" aktivieren!

### 2. Pakete installieren

```cmd
pip install pandas requests
```

### 3. Dateien vorbereiten

Ordnerstruktur:
```
C:\Users\YourName\sisal-osm\
├── sisal_sites_all.csv          ← SISAL-Daten hier
├── sisal_osm_mapper.py          ← Python-Script
├── sisal_osm_mapper.ipynb       ← Jupyter Notebook
└── output\                      ← Wird automatisch erstellt
    ├── sisal_osm_matches.csv
    └── top_20_matches.csv
```

---

## 🚀 Schnellstart

### **Option 1: Python-Script (einfach)**

```cmd
cd C:\Users\YourName\sisal-osm
python sisal_osm_mapper.py
```

**Ausgabe:**
- Console zeigt Fortschritt + Zusammenfassung
- `output/sisal_osm_matches.csv` mit allen Ergebnissen
- `output/top_20_matches.csv` mit besten Matches

**Wichtig:** Script ist im TEST_MODE (erste 10 Sites). Zum Ändern:
1. Öffne `sisal_osm_mapper.py` in Editor
2. Zeile ~450: `TEST_MODE = True` → `TEST_MODE = False`
3. Speichern und erneut ausführen

### **Option 2: Jupyter Notebook (empfohlen für Analyse)**

```cmd
# Jupyter installieren
pip install jupyter

# Notebook starten
cd C:\Users\YourName\sisal-osm
jupyter notebook sisal_osm_mapper.ipynb
```

**Vorteile:**
- Schritt-für-Schritt Ausführung
- Interaktive Visualisierungen
- Änderungen ohne Script-Edit (MAX_SITES in Zelle 6)

---

## 📊 Output-Dateien verstehen

### `sisal_osm_matches.csv`

**Spalten:**

| Spalte | Bedeutung | Beispiel |
|--------|-----------|----------|
| `site_id` | SISAL Site-ID | 117 |
| `site_name` | SISAL-Name | Bunker cave |
| `latitude` / `longitude` | Koordinaten | 51.3675, 7.6647 |
| `has_osm_match` | Match gefunden? | TRUE / FALSE |
| `total_osm_elements` | Anzahl OSM-Elemente | 3 |
| `cave_entrances_count` | Davon Höhleneingänge | 1 |
| `sinkholes_count` | Davon Dolinen | 0 |
| `tourism_caves_count` | Davon Touristenhöhlen | 0 |
| **`match_reason`** | **Warum Match / kein Match** | **"Strong name match (sim: 0.92)"** |
| `best_match_name` | Name des besten OSM-Matches | Bunker-Höhle |
| **`best_match_distance_km`** | **Distanz in km** | **0.342** |
| **`best_match_name_similarity`** | **Namen-Ähnlichkeit (0-1)** | **0.92** |
| `best_match_category` | OSM-Kategorie | cave_entrance |
| `best_match_osm_id` | OSM-ID für Verifikation | node/123456789 |
| **`best_match_score`** | **Composite Score (0-1)** | **0.748** |
| **`overpass_url`** | **Link zur manuellen Prüfung** | https://overpass-turbo.eu/... |

### `match_reason` Interpretieren

| Match Reason | Bedeutung | Aktion |
|--------------|-----------|--------|
| `"No OSM elements found"` | Nichts im Umkreis | Radius vergrößern oder OSM ergänzen |
| `"Strong name match (sim: 0.9)"` | Sehr gute Namens-Übereinstimmung | ✅ Sehr wahrscheinlich korrekt |
| `"Moderate name match (sim: 0.6)"` | Mäßige Übereinstimmung | ⚠️ Manuell prüfen |
| `"Close proximity (0.5 km)"` | Sehr nah, aber anderer Name | ⚠️ Evt. falsche Bezeichnung in OSM |
| `"Within search radius (4.2 km)"` | Nur Entfernung passt | ⚠️ Wahrscheinlich andere Höhle |

### Score-System

**`best_match_score` Berechnung:**
```
score = (name_similarity × 0.7) + (proximity_score × 0.3)

proximity_score = max(0, 1 - distance_km / search_radius_km)
```

**Beispiel:**
- Name-Ähnlichkeit: 0.85
- Distanz: 1.2 km (bei 5 km Radius)
- Proximity Score: 1 - (1.2/5) = 0.76
- **Finale Score: (0.85 × 0.7) + (0.76 × 0.3) = 0.595 + 0.228 = 0.823** ✅

**Interpretation:**
- **> 0.7:** Exzellenter Match
- **0.5 - 0.7:** Guter Match, manuell prüfen
- **< 0.5:** Schwacher Match, wahrscheinlich falsch

---

## 🔍 Manuelle Verifikation

### Overpass-Turbo-URL verwenden

1. Öffne `sisal_osm_matches.csv` in Excel
2. Kopiere URL aus Spalte `overpass_url`
3. Einfügen in Browser → öffnet interaktive Karte
4. Prüfe:
   - Liegt die SISAL-Site (blauer Kreis) nahe am OSM-Feature?
   - Stimmt der Name überein?
   - Ist es tatsächlich eine Höhle?

### In OpenStreetMap prüfen

Aus `best_match_osm_id` (z.B. `node/123456789`):
```
https://www.openstreetmap.org/node/123456789
```

→ Zeigt OSM-Eintrag mit allen Tags und Bearbeitungshistorie

---

## ⚙️ Konfiguration anpassen

### Suchradius ändern

**Python-Script:** Zeile 24
```python
DEFAULT_SEARCH_RADIUS = 10000  # 10 km statt 5 km
```

**Jupyter Notebook:** Zelle 1
```python
SEARCH_RADIUS = 10000  # 10 km
```

### Anzahl Sites ändern

**Python-Script:** Zeile ~450
```python
TEST_MODE = False  # Alle 305 Sites verarbeiten
```

**Jupyter Notebook:** Zelle 6
```python
MAX_SITES = None  # Alle Sites
# oder
MAX_SITES = 50    # Nur erste 50
```

### API-Delay anpassen

Bei Overpass-Fehlern (`429 Too Many Requests`):

**Python-Script:** Zeile 26
```python
API_DELAY = 2.5  # 2.5 Sekunden statt 1.5
```

**Jupyter Notebook:** Zelle 1
```python
API_DELAY = 2.5
```

---

## 📈 Typische Analyse-Workflows

### Workflow 1: Initiale Übersicht

```cmd
# 1. Erste 10 Sites testen
python sisal_osm_mapper.py  # TEST_MODE = True

# 2. Ergebnisse prüfen
# output/sisal_osm_matches.csv öffnen

# 3. Bei guten Ergebnissen: alle Sites
# TEST_MODE = False setzen und wiederholen
```

### Workflow 2: Radius-Optimierung

```python
# In Jupyter Notebook verschiedene Radien testen:

# Test 1: 2 km Radius
SEARCH_RADIUS = 2000
MAX_SITES = 20
# ... Zellen ausführen ...

# Test 2: 10 km Radius
SEARCH_RADIUS = 10000
MAX_SITES = 20
# ... Zellen ausführen ...

# Vergleiche match_rate in Zelle 7
```

### Workflow 3: Qualitätskontrolle

```python
# In Excel: Filter anwenden

# 1. Nur starke Matches
#    Filter: best_match_score > 0.7

# 2. Nur Höhleneingänge
#    Filter: best_match_category = "cave_entrance"

# 3. Nah gelegene Matches
#    Filter: best_match_distance_km < 1.0

# 4. Schwache Matches prüfen
#    Filter: 0.3 < best_match_score < 0.5
#    → overpass_url aufrufen und manuell verifizieren
```

---

## 🐛 Troubleshooting

### Problem: `FileNotFoundError: sisal_sites_all.csv`

**Lösung:**
```cmd
# Prüfe ob Datei existiert
dir sisal_sites_all.csv

# Falls nicht: Pfad in Script anpassen
# sisal_osm_mapper.py Zeile 22:
DEFAULT_CSV_PATH = Path(r"C:\Users\YourName\Downloads\sisal_sites_all.csv")
```

### Problem: `ModuleNotFoundError: No module named 'pandas'`

**Lösung:**
```cmd
pip install pandas requests
```

### Problem: Overpass API Fehler (`429 Too Many Requests`)

**Lösung:** API-Delay erhöhen
```python
API_DELAY = 2.5  # oder höher
```

### Problem: Keine Matches gefunden

**Mögliche Ursachen:**

1. **Radius zu klein**
   - Erhöhe `SEARCH_RADIUS` auf 10000 (10 km)
   
2. **Höhle nicht in OSM**
   - Prüfe manuell via Overpass-Turbo-URL
   - Evt. unter anderem Namen eingetragen

3. **Falsche OSM-Tags**
   - OSM nutzt evt. `leisure=cave` statt `natural=cave_entrance`
   - Script erweitern (siehe "OSM-Query erweitern")

### Problem: CSV zeigt Umlaute falsch (Excel)

**Lösung:**
1. Excel öffnen
2. "Daten" → "Aus Text/CSV"
3. Datei auswählen
4. Codierung: **UTF-8** auswählen
5. Import

**Oder:** LibreOffice Calc nutzt automatisch UTF-8

---

## 🔧 Erweiterte Anpassungen

### OSM-Query erweitern

In `build_overpass_query()` (Python/Notebook):

```python
# Zusätzliche Tags hinzufügen:
query = f"""
[out:json][timeout:25];
(
  // Bestehende Tags...
  
  // NEU: Leisure caves
  node["leisure"="cave"](around:{radius},{lat},{lon});
  way["leisure"="cave"](around:{radius},{lat},{lon});
);
out body;
>;
out skel qt;
"""
```

### Name-Normalisierung verbessern

In `string_similarity()`:

```python
def string_similarity(s1: str, s2: str) -> float:
    # Erweiterte Normalisierung
    s1_norm = s1.lower()
    s2_norm = s2.lower()
    
    # Entferne Suffixe
    for suffix in [' cave', ' höhle', ' grotte', ' caverna']:
        s1_norm = s1_norm.replace(suffix, '')
        s2_norm = s2_norm.replace(suffix, '')
    
    # Entferne Sonderzeichen
    import unicodedata
    s1_norm = unicodedata.normalize('NFKD', s1_norm).encode('ASCII', 'ignore').decode()
    s2_norm = unicodedata.normalize('NFKD', s2_norm).encode('ASCII', 'ignore').decode()
    
    return SequenceMatcher(None, s1_norm, s2_norm).ratio()
```

---

## 📚 Nächste Schritte

### Für dein EPICA-SISAL-Projekt:

1. **Statistik für deRSE26-Poster:**
   ```python
   # Match-Rate nach Region
   results_df.groupby('continent')['has_osm_match'].mean()
   
   # Top-Länder mit OSM-Höhlen
   results_df[results_df['has_osm_match']].groupby('country').size()
   ```

2. **RDF/Turtle-Export** (für Linked Data):
   - Nutze `sisal_osm_rdf.py` (separate Datei)
   - Importiere Matches in deine geolod-Ontologie
   - Verknüpfe mit `owl:sameAs`

3. **QGIS-Visualisierung:**
   ```python
   # GeoJSON exportieren
   import geopandas as gpd
   
   gdf = gpd.GeoDataFrame(
       results_df,
       geometry=gpd.points_from_xy(
           results_df.longitude, 
           results_df.latitude
       ),
       crs='EPSG:4326'
   )
   gdf.to_file('output/sisal_osm_matches.geojson', driver='GeoJSON')
   ```

4. **Campanian Ignimbrite Integration:**
   - Analog: Vulkanische Events in OSM suchen
   - Temporal linking via CIDOC-CRM

---

## 📖 Referenzen

- **SISAL Database:** https://doi.org/10.17864/1947.408
- **Overpass API:** https://overpass-api.de
- **Overpass Turbo:** https://overpass-turbo.eu
- **OSM Cave Tagging:** https://wiki.openstreetmap.org/wiki/Tag:natural=cave_entrance

---

## 📝 Lizenz

MIT License

---

## 🙋 Support

Bei Fragen:
1. README nochmal lesen (Troubleshooting-Sektion)
2. Overpass-Turbo-URL manuell testen
3. Issue auf GitHub erstellen

**Viel Erfolg mit deiner SISAL-OSM-Analyse!** 🎉
