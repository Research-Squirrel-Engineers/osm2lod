# 🧪 Diff-QuickStatements Test Script

## 📋 Was macht das Skript?

Testet die Generierung von **Diff-QuickStatements** durch Vergleich von:
- **OSM-Daten** (aus dem neuesten `dist/YYYY-MM-DD/` Ordner)
- **Wikibase-Daten** (via SPARQL Query gegen `osm2wiki.wikibase.cloud`)

Schreibt Ergebnis in `test/` Ordner.

---

## 🚀 Ausführung

### Windows (PowerShell):
```powershell
cd C:\git\osm2lod
python py\test_diff_generator.py
```

### Linux/Mac:
```bash
cd ~/osm2lod
python3 py/test_diff_generator.py
```

---

## 📂 Voraussetzungen

1. **Mindestens ein Run-Ordner** in `dist/`:
   ```
   dist/
   └── 2026-03-10/
       └── osm_export_holywells_*.csv
   ```

2. **Items in Wikibase** für den Export-Typ (holywells mit Q10=Q25)

3. **Internet-Verbindung** (für SPARQL Query)

---

## 📊 Output

Das Skript erstellt:

```
test/
└── quickstatements_DIFF_holywells_2026-03-10.txt
```

### Datei-Struktur:

```quickstatements
# ===============================================
# DIFF QuickStatements: holywells
# Generated: 2026-03-10T20:40:58Z
# Summary:
#   - 3 ADDED (CREATE)
#   - 8 MODIFIED (UPDATE)
#   - 0 DELETED (ignored)
# ===============================================

# ----------------
# ADDED ITEMS (3)
# ----------------

CREATE
LAST|Len|"New Holy Well"
LAST|Den|"OSM import snapshot (holywells) – node/12345678"
...

# ---------------------
# CHECKING FOR UPDATES
# ---------------------

# node/296103528 (Q1001): version 5→6
Q1001|P12|"2026-03-10T20:40:58Z"
Q1001|P13|6
Q1001|P16|159620588
Q1001|P17|"2026-03-10T12:34:56Z"

# node/473149586 (Q1002): coordinates, tags (5 total)
Q1002|P12|"2026-03-10T20:40:58Z"
Q1002|P5|@51.7129/-9.9441
Q1002|-P9
Q1002|P9|"tag1=value1"
Q1002|P9|"tag2=value2"
...
```

---

## ⚙️ Konfiguration

Im Skript anpassbar:

```python
# Welcher Export-Typ?
TEST_EXPORT_TYPE = "holywells"
TEST_QUERY_ITEM = "Q25"

# Wikibase Endpoint
WIKIBASE_SPARQL_ENDPOINT = "https://osm2wiki.wikibase.cloud/query/sparql"
```

---

## 🔍 Was wird verglichen?

### **Statische Properties (nie geändert):**
- P1 (instance of)
- P4 (OSM type)
- P10 (query item)
- P11 (OSM URL)

### **Immer aktualisiert:**
- P12 (Snapshot timestamp)

### **Conditional (nur wenn geändert):**
- P13 (version)
- P16 (changeset)
- P17 (OSM timestamp)
- P5 (coordinates)
- P6 (wikidata QID)
- P7 (wikipedia URL)
- P9 (tags - multi-value)
- Label (Len)

---

## 📈 Console Output

```
============================================================
🧪 DIFF-QUICKSTATEMENTS TEST
============================================================

📁 Using latest run: 2026-03-10

📂 Loading OSM CSV: osm_export_holywells_2026-03-10_204102Z.csv
✅ Loaded 715 OSM items

🔍 Querying Wikibase for holywells items (Q10=Q25)...
✅ Found 847 bindings in Wikibase
✅ Processed 712 unique items from Wikibase

📊 Diff Analysis:
   ✅ Added: 3
   📝 Common (potential updates): 712
   🗑️  Deleted: 0

✅ Diff QuickStatements written to: test\quickstatements_DIFF_holywells_2026-03-10.txt
   📝 8 items with actual changes

============================================================
✅ TEST COMPLETE
============================================================
```

---

## 🐛 Troubleshooting

### Problem: "No run directories found"
**Lösung:** Führe erst `osm2lod-runner.py` aus um Daten zu generieren

### Problem: "No items found in Wikibase"
**Lösung:** 
- Prüfe ob Items mit `P10=Q25` (holywells) existieren
- Prüfe SPARQL Endpoint URL
- Teste SPARQL Query manuell in https://osm2wiki.wikibase.cloud/query/

### Problem: "SPARQL timeout"
**Lösung:** Wikibase hat viele Items - Query optimieren oder timeout erhöhen

---

## 🔧 Nächste Schritte

Wenn der Test funktioniert:
1. ✅ Prüfe `test/quickstatements_DIFF_*.txt` Output
2. ✅ Verifiziere dass ADDED/MODIFIED korrekt erkannt werden
3. ✅ Teste Import in Wikibase (z.B. 1-2 UPDATE Statements)
4. ➡️ Integration in `osm2lod-runner.py` (automatisch bei jedem Run)

---

## 📝 Notizen

- **Tags (P9):** Aktuell werden bei Änderung ALLE Tags gelöscht (`-P9`) und neu gesetzt
- **Deleted Items:** Werden ignoriert (bleiben in Wikibase)
- **OSM Key:** `type/id` z.B. `node/296103528`
