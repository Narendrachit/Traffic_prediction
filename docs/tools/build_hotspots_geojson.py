#!/usr/bin/env python3
"""
Build hotspots.geojson from your data lake.

Inputs:
- gold/dashboard_data/region=<REGION>/edge_hotspot_rate.csv  (from_node,to_node,hotspot_rate,...)
- gold/routing_graph/region=<REGION>/routing_edges.csv       (src,dst,length_km,...)
- gold/routing_graph/region=<REGION>/routing_nodes.csv       (node,lon,lat or similar)

Output:
- hotspots.geojson with LineString geometries + hotspot_rate properties.

This avoids the common issue: routing_edges.csv often has no coordinates;
the coordinates live in routing_nodes.csv.
"""

import argparse
import json
from pathlib import Path

import pandas as pd


def _pick_col(cols_lower, original_cols, *candidates):
    for c in candidates:
        if c in cols_lower:
            return original_cols[cols_lower.index(c)]
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_lake", required=True, help="Root of the data lake, e.g. /data_lake")
    ap.add_argument("--region", required=True, help="Region name, e.g. London")
    ap.add_argument("--out", required=True, help="Output geojson path, e.g. /out/hotspots.geojson")
    ap.add_argument("--min_rate", type=float, default=0.0, help="Optional: drop edges below this hotspot_rate")
    args = ap.parse_args()

    dl = Path(args.data_lake)

    hotspot_csv = dl / "gold" / "dashboard_data" / f"region={args.region}" / "edge_hotspot_rate.csv"
    edges_csv = dl / "gold" / "routing_graph" / f"region={args.region}" / "routing_edges.csv"
    nodes_csv = dl / "gold" / "routing_graph" / f"region={args.region}" / "routing_nodes.csv"

    for p in [hotspot_csv, edges_csv, nodes_csv]:
        if not p.exists():
            raise SystemExit(f"Missing required file: {p}")

    hs = pd.read_csv(hotspot_csv)
    edges = pd.read_csv(edges_csv)
    nodes = pd.read_csv(nodes_csv)

    # --- hotspot columns ---
    hs_cols = [c.lower() for c in hs.columns]
    from_col = _pick_col(hs_cols, list(hs.columns), "from_node", "src", "from", "u")
    to_col   = _pick_col(hs_cols, list(hs.columns), "to_node", "dst", "to", "v")
    rate_col = _pick_col(hs_cols, list(hs.columns), "hotspot_rate", "rate")

    if not all([from_col, to_col, rate_col]):
        raise SystemExit(f"edge_hotspot_rate.csv must contain from/to/hotspot_rate. Found columns: {list(hs.columns)}")

    # --- edge columns ---
    e_cols = [c.lower() for c in edges.columns]
    src_col = _pick_col(e_cols, list(edges.columns), "src", "from_node", "from", "u")
    dst_col = _pick_col(e_cols, list(edges.columns), "dst", "to_node", "to", "v")
    if not all([src_col, dst_col]):
        raise SystemExit(f"routing_edges.csv must contain src/dst. Found columns: {list(edges.columns)}")

    # --- node columns ---
    n_cols = [c.lower() for c in nodes.columns]
    node_id = _pick_col(n_cols, list(nodes.columns), "node", "node_id", "id", "name")
    lat = _pick_col(n_cols, list(nodes.columns), "lat", "latitude", "y")
    lon = _pick_col(n_cols, list(nodes.columns), "lon", "lng", "longitude", "x")
    if not all([node_id, lat, lon]):
        raise SystemExit(
            "routing_nodes.csv must contain node id + lat/lon columns.\n"
            f"Found columns: {list(nodes.columns)}"
        )

    # Normalise node id as str
    nodes[node_id] = nodes[node_id].astype(str)

    # Join: hotspot -> edges (by from/to) -> nodes coords
    m = hs[[from_col, to_col, rate_col]].copy()
    m[from_col] = m[from_col].astype(str)
    m[to_col] = m[to_col].astype(str)
    m = m.merge(
        edges[[src_col, dst_col] + [c for c in edges.columns if c not in (src_col, dst_col)]],
        left_on=[from_col, to_col],
        right_on=[src_col, dst_col],
        how="left",
    )

    # Join nodes twice
    n_small = nodes[[node_id, lat, lon]].copy()
    n_small.columns = ["_node", "_lat", "_lon"]

    m = m.merge(n_small, left_on=src_col, right_on="_node", how="left").rename(
        columns={"_lat": "src_lat", "_lon": "src_lon"}
    ).drop(columns=["_node"], errors="ignore")

    m = m.merge(n_small, left_on=dst_col, right_on="_node", how="left").rename(
        columns={"_lat": "dst_lat", "_lon": "dst_lon"}
    ).drop(columns=["_node"], errors="ignore")

    # Build features
    features = []
    dropped_no_coords = 0
    for _, r in m.iterrows():
        rate = r.get(rate_col)
        if pd.isna(rate) or float(rate) < float(args.min_rate):
            continue
        if pd.isna(r.get("src_lat")) or pd.isna(r.get("src_lon")) or pd.isna(r.get("dst_lat")) or pd.isna(r.get("dst_lon")):
            dropped_no_coords += 1
            continue

        props = {
            "from_node": str(r[from_col]),
            "to_node": str(r[to_col]),
            "hotspot_rate": float(rate),
        }

        # optionally keep a few useful extras if present
        for extra in ["avg_pred", "n_rows", "length_km", "weight_km"]:
            if extra in m.columns and not pd.isna(r.get(extra)):
                props[extra] = float(r.get(extra))

        geom = {
            "type": "LineString",
            "coordinates": [
                [float(r["src_lon"]), float(r["src_lat"])],
                [float(r["dst_lon"]), float(r["dst_lat"])],
            ],
        }

        features.append({"type": "Feature", "geometry": geom, "properties": props})

    out_fc = {"type": "FeatureCollection", "features": features}
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out_fc), encoding="utf-8")

    print(f"Wrote {out_path} | features={len(features)} | dropped_no_coords={dropped_no_coords}")
    print("Tip: Use the dashboard threshold slider (0.10–0.30) to avoid one edge dominating the view.")


if __name__ == "__main__":
    main()
