# Mapbox dashboard (Hotspots + Routing)

This is a **static** Mapbox GL JS dashboard that visualises:
- `hotspots.geojson` (road edges with `hotspot_rate`), and
- `routes.geojson` (route lines with properties from your `07_route_demo.py`).

## What you need
1) A Mapbox account + **public token**  
Mapbox → **Account** → **Access tokens** → create a token → copy it.

2) Two GeoJSON files in `mapbox_dashboard/data/`
- `hotspots.geojson`
- `routes.geojson`

## How to run locally (Windows)
Open PowerShell in `mapbox_dashboard/` and run:
```powershell
python -m http.server 8000
```
Then open:
http://localhost:8000

> Note: `file://` won’t work because the browser blocks `fetch()` for local files.

## Generate the GeoJSON from your data lake
See `../tools/README.md`.
