## Mapbox dashboard build tools

### 1) Build hotspots.geojson
Run inside your Airflow folder (Windows PowerShell):

docker run --rm `
  -v traffic_data_lake:/data_lake `
  -v "${PWD}\mapbox_dashboard\tools:/tools" `
  -v "${PWD}\mapbox_dashboard\data:/out" `
  traffic-runner:latest `
  python /tools/build_hotspots_geojson.py `
    --data_lake /data_lake `
    --region London `
    --out /out/hotspots.geojson

### 2) Build routes.geojson
This uses **exports/routes_edges.csv** (or the latest run folder) and **routing_nodes.csv** for coordinates.

docker run --rm `
  -v traffic_data_lake:/data_lake `
  -v "${PWD}\mapbox_dashboard\tools:/tools" `
  -v "${PWD}\mapbox_dashboard\data:/out" `
  traffic-runner:latest `
  python /tools/build_routes_geojson.py `
    --data_lake /data_lake `
    --region London `
    --out /out/routes.geojson

### 3) Copy model metrics (optional, for RQ2 panel)

docker run --rm `
  -v traffic_data_lake:/data_lake `
  -v "${PWD}\mapbox_dashboard\data:/out" `
  alpine sh -lc "cp /data_lake/gold/models_walkforward/region=London/walkforward_metrics.csv /out/walkforward_metrics.csv 2>/dev/null || true; ls -la /out"

### 4) Run the dashboard
Edit **mapbox_dashboard/config.js** and paste your Mapbox token.

Then:

cd mapbox_dashboard
python -m http.server 8000

Open http://localhost:8000
