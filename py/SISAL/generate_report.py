#!/usr/bin/env python3
"""
Generate detailed match report and Overpass Turbo query
"""

import pandas as pd
from pathlib import Path
from urllib.parse import urlencode

# Paths
SCRIPT_DIR = Path(__file__).parent
CSV_FILE = SCRIPT_DIR / "output" / "sisal_osm_matches.csv"
REPORT_FILE = SCRIPT_DIR / "output" / "detailed_match_report.txt"
UNMATCHED_FILE = SCRIPT_DIR / "output" / "unmatched_sites_report.txt"
QUERY_FILE = SCRIPT_DIR / "output" / "overpass_turbo_query.txt"
URL_FILE = SCRIPT_DIR / "output" / "overpass_turbo_url.txt"

print("=" * 70)
print("DETAILED MATCH REPORT GENERATOR")
print("=" * 70)

# Load results
df = pd.read_csv(CSV_FILE)
matched = df[df["matched"] == True].sort_values("score", ascending=False)
unmatched = df[df["matched"] == False].sort_values("site_name")

# ============================================================================
# PART 1: DETAILED REPORT
# ============================================================================

with open(REPORT_FILE, "w", encoding="utf-8") as f:
    f.write("=" * 70 + "\n")
    f.write("SISAL-OSM DETAILED MATCH REPORT\n")
    f.write("=" * 70 + "\n\n")

    # Summary
    f.write(f"Total SISAL sites: {len(df)}\n")
    f.write(f"Matched: {len(matched)} ({len(matched)/len(df)*100:.1f}%)\n")
    f.write(f"Unmatched: {len(unmatched)} ({len(unmatched)/len(df)*100:.1f}%)\n")
    f.write("\n" + "=" * 70 + "\n\n")

    # ========================================================================
    # MATCHED SITES (sorted by score, best first)
    # ========================================================================

    f.write(f"MATCHED SITES ({len(matched)})\n")
    f.write("=" * 70 + "\n")
    f.write("Sorted by match score (best matches first)\n\n")

    for idx, (_, row) in enumerate(matched.iterrows(), 1):
        f.write(f"{idx:3d}. {row['site_name']}\n")
        f.write(f"     SISAL ID: {row['site_id']}\n")
        f.write(f"     SISAL Coords: {row['lat']:.4f}, {row['lon']:.4f}\n")
        f.write(f"\n")
        f.write(f"     ✓ MATCHED TO:\n")
        f.write(f"       OSM Name: {row['osm_name']}\n")
        f.write(f"       OSM ID: {row['osm_id']}\n")
        f.write(f"       OSM Coords: {row['osm_lat']:.4f}, {row['osm_lon']:.4f}\n")
        f.write(f"\n")
        f.write(f"     MATCH QUALITY:\n")
        f.write(f"       Distance: {row['distance_km']:.3f} km\n")
        f.write(
            f"       Name similarity: {row['name_sim']:.3f} (0=different, 1=identical)\n"
        )
        f.write(f"       Overall score: {row['score']:.3f}\n")
        f.write(f"\n")

        # Match reason/quality classification
        if row["name_sim"] > 0.8:
            reason = "Strong name match (>0.8 similarity)"
        elif row["name_sim"] > 0.5:
            reason = "Moderate name match (0.5-0.8 similarity)"
        elif row["distance_km"] < 1.0:
            reason = "Close proximity (<1 km, despite low name similarity)"
        else:
            reason = f"Within search radius ({row['distance_km']:.2f} km)"

        f.write(f"     MATCH REASON: {reason}\n")
        f.write(f"\n")
        f.write("-" * 70 + "\n\n")

    # ========================================================================
    # UNMATCHED SITES
    # ========================================================================

    f.write("\n" + "=" * 70 + "\n\n")
    f.write(f"UNMATCHED SITES ({len(unmatched)})\n")
    f.write("=" * 70 + "\n")
    f.write("No OSM cave features found within 5 km radius\n\n")

    for idx, (_, row) in enumerate(unmatched.iterrows(), 1):
        f.write(f"{idx:3d}. {row['site_name']}\n")
        f.write(f"     SISAL ID: {row['site_id']}\n")
        f.write(f"     Coords: {row['lat']:.4f}, {row['lon']:.4f}\n")
        f.write(f"     ✗ REASON: No OSM elements within 5 km radius\n")
        f.write(f"\n")
        f.write(f"     POSSIBLE CAUSES:\n")
        f.write(f"       - Cave not yet mapped in OpenStreetMap\n")
        f.write(f"       - Cave mapped with different tags\n")
        f.write(f"       - SISAL coordinates imprecise\n")
        f.write(f"       - Cave in remote/unmapped area\n")
        f.write(f"\n")
        f.write("-" * 70 + "\n\n")

    # ========================================================================
    # STATISTICS
    # ========================================================================

    f.write("\n" + "=" * 70 + "\n\n")
    f.write("DETAILED STATISTICS\n")
    f.write("=" * 70 + "\n\n")

    if len(matched) > 0:
        f.write("DISTANCE STATISTICS (km):\n")
        f.write(f"  Minimum:  {matched['distance_km'].min():.3f}\n")
        f.write(f"  Maximum:  {matched['distance_km'].max():.3f}\n")
        f.write(f"  Mean:     {matched['distance_km'].mean():.3f}\n")
        f.write(f"  Median:   {matched['distance_km'].median():.3f}\n")
        f.write(f"  Std Dev:  {matched['distance_km'].std():.3f}\n")
        f.write("\n")

        f.write("NAME SIMILARITY STATISTICS:\n")
        f.write(f"  Minimum:  {matched['name_sim'].min():.3f}\n")
        f.write(f"  Maximum:  {matched['name_sim'].max():.3f}\n")
        f.write(f"  Mean:     {matched['name_sim'].mean():.3f}\n")
        f.write(f"  Median:   {matched['name_sim'].median():.3f}\n")
        f.write(f"  Std Dev:  {matched['name_sim'].std():.3f}\n")
        f.write("\n")

        f.write("MATCH SCORE STATISTICS:\n")
        f.write(f"  Minimum:  {matched['score'].min():.3f}\n")
        f.write(f"  Maximum:  {matched['score'].max():.3f}\n")
        f.write(f"  Mean:     {matched['score'].mean():.3f}\n")
        f.write(f"  Median:   {matched['score'].median():.3f}\n")
        f.write(f"  Std Dev:  {matched['score'].std():.3f}\n")
        f.write("\n")

        # Quality categories
        strong = len(matched[matched["name_sim"] > 0.8])
        moderate = len(
            matched[(matched["name_sim"] > 0.5) & (matched["name_sim"] <= 0.8)]
        )
        proximity = len(
            matched[(matched["name_sim"] <= 0.5) & (matched["distance_km"] < 1.0)]
        )
        weak = len(
            matched[(matched["name_sim"] <= 0.5) & (matched["distance_km"] >= 1.0)]
        )

        f.write("MATCH QUALITY CATEGORIES:\n")
        f.write(
            f"  Strong name match (sim > 0.8):        {strong:3d} ({strong/len(matched)*100:5.1f}%)\n"
        )
        f.write(
            f"  Moderate name match (0.5 < sim ≤ 0.8): {moderate:3d} ({moderate/len(matched)*100:5.1f}%)\n"
        )
        f.write(
            f"  Close proximity (dist < 1km):         {proximity:3d} ({proximity/len(matched)*100:5.1f}%)\n"
        )
        f.write(
            f"  Weak match (low sim, dist ≥ 1km):     {weak:3d} ({weak/len(matched)*100:5.1f}%)\n"
        )

print(f"✓ Detailed report saved to: {REPORT_FILE}")

# ============================================================================
# SEPARATE UNMATCHED SITES REPORT
# ============================================================================

with open(UNMATCHED_FILE, "w", encoding="utf-8") as f:
    f.write("=" * 70 + "\n")
    f.write("UNMATCHED SISAL SITES - DETAILED REPORT\n")
    f.write("=" * 70 + "\n")
    f.write(f"\nSites without OSM cave match within 5 km radius\n")
    f.write(
        f"Total unmatched: {len(unmatched)} of {len(df)} ({len(unmatched)/len(df)*100:.1f}%)\n\n"
    )
    f.write("=" * 70 + "\n\n")

    for idx, (_, row) in enumerate(unmatched.iterrows(), 1):
        f.write(f"{idx:3d}. {row['site_name']}\n")
        f.write("=" * 70 + "\n")
        f.write(f"SISAL Information:\n")
        f.write(f"  Site ID: {row['site_id']}\n")
        f.write(f"  Site Name: {row['site_name']}\n")
        f.write(f"  Latitude: {row['lat']:.6f}\n")
        f.write(f"  Longitude: {row['lon']:.6f}\n")
        f.write(f"\n")
        f.write(f"Match Status:\n")
        f.write(f"  ✗ NO MATCH FOUND\n")
        f.write(f"  Reason: No OSM cave features within 5 km search radius\n")
        f.write(f"\n")
        f.write(f"Possible Causes:\n")
        f.write(f"  • Cave not yet mapped in OpenStreetMap\n")
        f.write(f"  • Cave mapped with different/incorrect tags\n")
        f.write(f"  • SISAL coordinates may be imprecise\n")
        f.write(f"  • Cave in remote/poorly-mapped area\n")
        f.write(f"  • Cave entrance may be mapped as different feature type\n")
        f.write(f"\n")
        f.write(f"Links for Manual Verification:\n")
        f.write(
            f"  OpenStreetMap: https://www.openstreetmap.org/#map=14/{row['lat']:.4f}/{row['lon']:.4f}\n"
        )
        f.write(
            f"  Google Maps: https://www.google.com/maps/@{row['lat']:.4f},{row['lon']:.4f},14z\n"
        )

        # Overpass query for manual check (wider radius)
        overpass_check = f"https://overpass-turbo.eu/?Q=[out:json];(node[\"natural\"=\"cave_entrance\"](around:10000,{row['lat']},{row['lon']}););out;&R"
        f.write(f"  Overpass (10km radius): {overpass_check}\n")

        f.write(f"\n")
        f.write("-" * 70 + "\n\n")

    # Summary statistics
    f.write("\n" + "=" * 70 + "\n")
    f.write("GEOGRAPHIC DISTRIBUTION OF UNMATCHED SITES\n")
    f.write("=" * 70 + "\n\n")

    # Count by region (rough classification)
    regions = {"Europe": 0, "Asia": 0, "Americas": 0, "Africa": 0, "Oceania": 0}

    for _, row in unmatched.iterrows():
        lat, lon = row["lat"], row["lon"]
        if 35 <= lat <= 72 and -15 <= lon <= 40:
            regions["Europe"] += 1
        elif -10 <= lat <= 50 and 40 <= lon <= 180:
            regions["Asia"] += 1
        elif -55 <= lat <= 72 and -170 <= lon <= -30:
            regions["Americas"] += 1
        elif -35 <= lat <= 37 and -20 <= lon <= 55:
            regions["Africa"] += 1
        else:
            regions["Oceania"] += 1

    for region, count in regions.items():
        if count > 0:
            f.write(
                f"{region:<15} {count:3d} sites ({count/len(unmatched)*100:5.1f}%)\n"
            )

    f.write("\n" + "=" * 70 + "\n")
    f.write("RECOMMENDATIONS FOR UNMATCHED SITES\n")
    f.write("=" * 70 + "\n\n")
    f.write("1. Manual Verification:\n")
    f.write("   - Use provided OpenStreetMap/Google Maps links\n")
    f.write("   - Check if cave exists but has different name\n")
    f.write("   - Verify SISAL coordinates are correct\n\n")
    f.write("2. Contribute to OpenStreetMap:\n")
    f.write("   - If cave exists but is not mapped: add it!\n")
    f.write("   - Use iD editor: https://www.openstreetmap.org/edit\n")
    f.write("   - Tag as: natural=cave_entrance + name=...\n\n")
    f.write("3. Data Quality Check:\n")
    f.write("   - Cross-reference SISAL coordinates with literature\n")
    f.write("   - Some sites may need coordinate correction\n\n")
    f.write("4. Expand Search Radius:\n")
    f.write("   - Try 10km or 20km radius for remote areas\n")
    f.write("   - Modify script: radius=10 in sisal_osm_mapper.py\n\n")

print(f"✓ Unmatched sites report saved to: {UNMATCHED_FILE}")

# ============================================================================
# PART 2: OVERPASS TURBO QUERY
# ============================================================================

if len(matched) > 0:
    # Extract OSM IDs
    osm_ids = []
    for _, row in matched.iterrows():
        osm_id = str(row["osm_id"]).strip()
        if osm_id and osm_id != "nan":
            osm_ids.append(osm_id)

    # Build query
    query = "[out:json][timeout:25];\n(\n"

    # Add individual node statements
    for osm_id in osm_ids:
        query += f"  node({osm_id});\n"

    query += ");\nout meta geom;"

    # Save query
    with open(QUERY_FILE, "w", encoding="utf-8") as f:
        f.write(query)

    print(f"✓ Overpass query saved to: {QUERY_FILE}")

    # Generate URL
    url = "https://overpass-turbo.eu/?" + urlencode({"Q": query, "R": ""})

    with open(URL_FILE, "w", encoding="utf-8") as f:
        f.write(url)

    print(f"✓ Overpass Turbo URL saved to: {URL_FILE}")

    # Print summary
    print("\n" + "=" * 70)
    print("OVERPASS TURBO QUERY")
    print("=" * 70)
    print(f"\nGenerated query for {len(osm_ids)} matched OSM cave nodes")
    print(f"\nQuery preview (first 10 nodes):")
    print("-" * 70)
    print("[out:json][timeout:25];")
    print("(")
    for osm_id in osm_ids[:10]:
        print(f"  node({osm_id});")
    if len(osm_ids) > 10:
        print(f"  ... ({len(osm_ids)-10} more nodes)")
    print(");")
    print("out meta geom;")
    print("-" * 70)

    print(f"\n🗺️  OPEN THIS URL TO SEE ALL {len(osm_ids)} MATCHES ON MAP:")
    print(f"   {url[:80]}...")
    print(f"\n   (Full URL saved in: {URL_FILE})")

print("\n" + "=" * 70)
print("DONE!")
print("=" * 70)
print(f"\nGenerated files:")
print(f"  1. {REPORT_FILE} - Detailed match report (all sites)")
print(f"  2. {UNMATCHED_FILE} - Unmatched sites only")
print(f"  3. {QUERY_FILE} - Overpass query (matched sites)")
print(f"  4. {URL_FILE} - Overpass Turbo URL")
