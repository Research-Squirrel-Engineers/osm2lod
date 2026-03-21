#!/usr/bin/env python3
"""
Download OSM Cave Data - Alternative Methods
=============================================
If Overpass Turbo is overloaded, use this script to download data.
"""

import requests
import json
from pathlib import Path
import time

OUTPUT_FILE = Path("osm_caves.geojson")

print("="*70)
print("OSM Cave Data Downloader")
print("="*70)

# Method 1: Try public Overpass instance
print("\nMethod 1: Trying public Overpass API...")

query = """
[out:json][timeout:180];
(
  node["natural"="cave_entrance"];
  way["natural"="cave_entrance"];
);
out center;
"""

endpoints = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

success = False

for i, endpoint in enumerate(endpoints, 1):
    print(f"\nTrying endpoint {i}/{len(endpoints)}: {endpoint}")
    print("This may take 2-5 minutes...")
    
    try:
        response = requests.post(
            endpoint,
            data={'data': query},
            timeout=300,
            headers={'User-Agent': 'SISAL-OSM-Mapper/1.0'}
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # Convert to GeoJSON format
            geojson = {
                "type": "FeatureCollection",
                "features": []
            }
            
            for element in data.get('elements', []):
                if element.get('type') == 'node':
                    lat, lon = element.get('lat'), element.get('lon')
                elif 'center' in element:
                    lat = element['center']['lat']
                    lon = element['center']['lon']
                else:
                    continue
                
                feature = {
                    "type": "Feature",
                    "properties": {
                        "name": element.get('tags', {}).get('name', 'Unnamed'),
                        "id": element.get('id'),
                        "@id": f"{element.get('type')}/{element.get('id')}"
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    }
                }
                geojson['features'].append(feature)
            
            # Save
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(geojson, f)
            
            print(f"\n✓ SUCCESS! Downloaded {len(geojson['features'])} caves")
            print(f"✓ Saved to: {OUTPUT_FILE.absolute()}")
            success = True
            break
        else:
            print(f"✗ Failed: HTTP {response.status_code}")
    
    except requests.exceptions.Timeout:
        print("✗ Timeout - server too busy")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    if i < len(endpoints):
        print("Waiting 10 seconds before trying next endpoint...")
        time.sleep(10)

if not success:
    print("\n" + "="*70)
    print("ALL ENDPOINTS FAILED - MANUAL ALTERNATIVE")
    print("="*70)
    print("\nOption A: Try again later (evening/night when API is less busy)")
    print("\nOption B: Smaller regional query:")
    print("-" * 70)
    print("""
Go to: https://overpass-turbo.eu/
Paste this SMALLER query (only SISAL regions):

[out:json][timeout:90];
(
  // Europe
  node["natural"="cave_entrance"](35,-15,72,40);
  way["natural"="cave_entrance"](35,-15,72,40);
  
  // Asia
  node["natural"="cave_entrance"](-10,60,50,180);
  way["natural"="cave_entrance"](-10,60,50,180);
  
  // Americas
  node["natural"="cave_entrance"](-55,-170,72,-30);
  way["natural"="cave_entrance"](-55,-170,72,-30);
);
out center;

Then: Export → GeoJSON → Save as osm_caves.geojson
    """)
    print("-" * 70)

print("\n" + "="*70)
print("DONE")
print("="*70)
