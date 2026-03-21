#!/usr/bin/env python3
"""
SISAL-OSM Mapper - Ultra Simple & Reliable
===========================================
Works with manually downloaded OSM data (no API problems!)

QUICK START:
1. Download OSM caves: https://overpass-turbo.eu/
2. Run this query (wait 2-3 min):

   [out:json][timeout:180];
   (node["natural"="cave_entrance"]; way["natural"="cave_entrance"];);
   out center;

3. Export → GeoJSON → Save as "osm_caves.geojson"
4. Run: python sisal_osm_mapper.py

Done! No timeouts, no rate limits, 100% reliable.
"""

import pandas as pd
import re
import json
import math
from pathlib import Path
from difflib import SequenceMatcher

# ============================================================================
# FILES
# ============================================================================

SISAL_CSV = Path("sisal_sites_all.csv")
OSM_FILE = Path("osm_caves.geojson")  # Or osm_caves.json
OUTPUT = Path("output")

# ============================================================================
# HELPERS
# ============================================================================


def distance_km(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lat2 = map(math.radians, [lat1, lat2])
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def similarity(s1, s2):
    if not s1 or not s2:
        return 0.0
    s1 = s1.lower().replace(" cave", "").replace(" höhle", "").strip()
    s2 = s2.lower().replace(" cave", "").replace(" höhle", "").strip()
    return SequenceMatcher(None, s1, s2).ratio()


# ============================================================================
# LOAD
# ============================================================================


def load_sisal():
    df = pd.read_csv(SISAL_CSV)

    def parse(wkt):
        m = re.search(r"POINT\(([+-]?\d+\.?\d*)\s+([+-]?\d+\.?\d*)\)", str(wkt))
        return (float(m.group(2)), float(m.group(1))) if m else (None, None)

    df[["latitude", "longitude"]] = df["wkt"].apply(lambda x: pd.Series(parse(x)))
    return df


def load_osm():
    if not OSM_FILE.exists():
        print(f"\n❌ {OSM_FILE} not found!")
        print("\n" + "=" * 70)
        print("DOWNLOAD OSM DATA:")
        print("=" * 70)
        print("1. Go to: https://overpass-turbo.eu/")
        print("\n2. Paste query:")
        print("   [out:json][timeout:180];")
        print('   (node["natural"="cave_entrance"]; way["natural"="cave_entrance"];);')
        print("   out center;")
        print("\n3. Click 'Run' → wait 2-3 min")
        print("4. Export → GeoJSON → save as 'osm_caves.geojson'")
        print(f"5. Move to: {Path.cwd()}")
        print("6. Run script again")
        print("=" * 70 + "\n")
        return None

    with open(OSM_FILE) as f:
        data = json.load(f)

    caves = []
    for feat in data.get("features", []):
        geom = feat.get("geometry", {})
        if geom.get("type") != "Point":
            continue

        lon, lat = geom["coordinates"]
        props = feat.get("properties", {})
        caves.append(
            {
                "name": props.get("name", "Unnamed"),
                "id": props.get("id", props.get("@id", "unknown")),
                "lat": lat,
                "lon": lon,
            }
        )

    return caves


# ============================================================================
# MATCH
# ============================================================================


def match(sisal, osm, radius=5):
    print(f"Matching {len(sisal)} SISAL → {len(osm)} OSM (radius {radius}km)...\n")
    results = []

    for i, row in sisal.iterrows():
        if pd.notna(row["latitude"]) and pd.notna(row["longitude"]):
            lat, lon, name = row["latitude"], row["longitude"], row["site_name"]

            candidates = []
            for cave in osm:
                d = distance_km(lat, lon, cave["lat"], cave["lon"])
                if d <= radius:
                    sim = similarity(name, cave["name"])
                    score = sim * 0.7 + (1 - d / radius) * 0.3
                    candidates.append(
                        {
                            "name": cave["name"],
                            "id": cave["id"],
                            "dist": round(d, 3),
                            "sim": round(sim, 3),
                            "score": round(score, 3),
                            "lat": cave["lat"],
                            "lon": cave["lon"],
                        }
                    )

            r = {
                "site_id": row["site_id"],
                "site_name": name,
                "lat": lat,
                "lon": lon,
                "matched": len(candidates) > 0,
                "count": len(candidates),
            }

            if candidates:
                best = max(candidates, key=lambda x: x["score"])
                r.update(
                    {
                        "osm_name": best["name"],
                        "osm_id": best["id"],
                        "distance_km": best["dist"],
                        "name_sim": best["sim"],
                        "score": best["score"],
                        "osm_lat": best["lat"],
                        "osm_lon": best["lon"],
                    }
                )

            results.append(r)

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(sisal)}...")

    return pd.DataFrame(results)


# ============================================================================
# MAIN
# ============================================================================


def main():
    print("SISAL-OSM Mapper (Simple & Reliable)")
    print("=" * 70 + "\n")

    sisal = load_sisal()
    print(f"✓ SISAL: {len(sisal)} sites")

    osm = load_osm()
    if not osm:
        return
    print(f"✓ OSM: {len(osm)} caves\n")

    results = match(sisal, osm)

    # Stats
    m = results["matched"].sum()
    print(f"\n{'='*70}")
    print(
        f"Total: {len(results)} | Matched: {m} ({m/len(results)*100:.1f}%) | Unmatched: {len(results)-m}"
    )
    print("=" * 70)

    if m > 0:
        df = results[results["matched"]]
        print(
            f"Distance: {df['distance_km'].min():.2f}–{df['distance_km'].max():.2f}km (avg {df['distance_km'].mean():.2f})"
        )
        print(
            f"Similarity: {df['name_sim'].min():.2f}–{df['name_sim'].max():.2f} (avg {df['name_sim'].mean():.2f})"
        )

    # Save
    OUTPUT.mkdir(exist_ok=True)
    out = OUTPUT / "sisal_osm_matches.csv"
    results.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\n✓ Saved: {out}")

    # Unmatched
    unm = results[~results["matched"]]
    if not unm.empty:
        print(f"\nUNMATCHED ({len(unm)}):")
        for _, r in unm.iterrows():
            print(f"  {r['site_name']:<35} ({r['lat']:.2f}, {r['lon']:.2f})")

    print("\nDONE! ✓")


if __name__ == "__main__":
    main()
