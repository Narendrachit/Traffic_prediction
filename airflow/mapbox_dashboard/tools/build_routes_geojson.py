#!/usr/bin/env python3
"""
Build routes.geojson for the Mapbox dashboard.

Inputs:
- /data_lake/gold/exports/routes_edges.csv  (preferred) OR a latest run_utc=.../routes_edges.csv
- /data_lake/gold/routing_graph/region=<REGION>/routing_nodes.csv  (node coordinates)

It converts each (from_node,to_node,route) into a LineString feature.

Expected columns in routes_edges.csv (your current file already matches this):
- from_node, to_node, route, src, dst
Optional:
- length_km, weight_km, hotspot_rate, penalty_km

Output:
- routes.geojson (FeatureCollection)
"""
import argparse
import json
import os
from glob import glob

import pandas as pd


def _pick(columns_lower, original_columns, *candidates):
    for cand in candidates:
        if cand in columns_lower:
            return original_columns[columns_lower.index(cand)]
    return None


def _read_latest_routes_edges(data_lake: str, region: str) -> str:
    # 1) Stable export path (recommended)
    exp = os.path.join(data_lake, "gold", "exports", "routes_edges.csv")
    if os.path.exists(exp):
        return exp

    # 2) Single-route variant
    exp2 = os.path.join(data_lake, "gold", "exports", "route_edges.csv")
    if os.path.exists(exp2):
        return exp2

    # 3) Latest run folder
    run_dir = os.path.join(data_lake, "gold", "route_analysis", f"region={region}")
    runs = sorted(glob(os.path.join(run_dir, "run_utc=*")))
    for r in reversed(runs):
        cand = os.path.join(r, "routes_edges.csv")
        if os.path.exists(cand):
            return cand
        cand2 = os.path.join(r, "route_edges.csv")
        if os.path.exists(cand2):
            return cand2

    raise FileNotFoundError(
        "Could not find routes_edges.csv.\n"
        f"Tried: {exp}, {exp2}, and {run_dir}/run_utc=*/routes_edges.csv"
    )


def _read_nodes(data_lake: str, region: str) -> pd.DataFrame:
    nodes_path = os.path.join(data_lake, "gold", "routing_graph", f"region={region}", "routing_nodes.csv")
    if not os.path.exists(nodes_path):
        alt = os.path.join(data_lake, "gold", "routing_graph", f"region={region}", "nodes.csv")
        if os.path.exists(alt):
            nodes_path = alt
        else:
            raise FileNotFoundError(
                "routing_nodes.csv not found.\n"
                f"Expected: {nodes_path}\n"
                f"Also tried: {alt}"
            )
    nodes = pd.read_csv(nodes_path)
    cols = [c.lower() for c in nodes.columns]
    node_id = _pick(cols, nodes.columns, "node", "node_id", "id", "name")
    lat = _pick(cols, nodes.columns, "lat", "latitude", "y")
    lon = _pick(cols, nodes.columns, "lon", "lng", "longitude", "x")
    if not all([node_id, lat, lon]):
        raise SystemExit(
            "routing_nodes.csv missing required columns.\n"
            f"Found columns: {list(nodes.columns)}\n"
            "Need node id + lat/lon columns (e.g., node, lat, lon)."
        )
    nodes = nodes[[node_id, lat, lon]].rename(columns={node_id: "node", lat: "lat", lon: "lon"})
    return nodes


def _build_node_lookup(nodes: pd.DataFrame) -> dict:
    d = {}
    for _, r in nodes.iterrows():
        try:
            d[str(r["node"])] = (float(r["lon"]), float(r["lat"]))
        except Exception:
            continue
    return d


def _reconstruct_path_edges(df_edges: pd.DataFrame, start: str, end: str):
    """
    Reconstruct ordered node sequence using src->dst mapping.
    Falls back to CSV row order if chain reconstruction fails.
    """
    edges = list(zip(df_edges["src"].astype(str), df_edges["dst"].astype(str)))
    mapping = {}
    for s, t in edges:
        mapping.setdefault(s, []).append(t)

    # Try chain reconstruction
    seq = [start]
    used = set()
    cur = start
    max_steps = len(edges) + 5
    for _ in range(max_steps):
        if cur == end:
            break
        cands = mapping.get(cur, [])
        nxt = None
        for t in cands:
            key = (cur, t, len(used))
            # not perfect uniqueness, but good enough
            if (cur, t) not in used:
                nxt = t
                used.add((cur, t))
                break
        if nxt is None:
            break
        seq.append(nxt)
        cur = nxt

    if seq[-1] == end and len(seq) >= 2:
        return seq

    # Fallback: row order
    seq = [edges[0][0]] + [t for _, t in edges]
    return seq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_lake", default="/data_lake")
    ap.add_argument("--region", required=True)
    ap.add_argument("--out", required=True, help="Output GeoJSON path (e.g., /out/routes.geojson)")
    args = ap.parse_args()

    data_lake = args.data_lake
    region = args.region

    routes_path = _read_latest_routes_edges(data_lake, region)
    routes = pd.read_csv(routes_path)

    cols = [c.lower() for c in routes.columns]
    from_col = _pick(cols, routes.columns, "from_node", "from")
    to_col = _pick(cols, routes.columns, "to_node", "to")
    route_col = _pick(cols, routes.columns, "route", "route_type", "strategy")
    src_col = _pick(cols, routes.columns, "src", "u", "start", "from_id")
    dst_col = _pick(cols, routes.columns, "dst", "v", "end", "to_id")

    if not all([from_col, to_col, route_col, src_col, dst_col]):
        raise SystemExit(
            "routes_edges.csv missing required columns.\n"
            f"Columns: {list(routes.columns)}\n"
            "Need: from_node + to_node + route + src + dst (names can vary)."
        )

    # Normalize
    routes = routes.rename(columns={
        from_col: "from_node",
        to_col: "to_node",
        route_col: "route",
        src_col: "src",
        dst_col: "dst",
    })

    nodes = _read_nodes(data_lake, region)
    node_xy = _build_node_lookup(nodes)

    # Optional numeric cols
    for c in ["length_km", "weight_km", "hotspot_rate", "penalty_km"]:
        if c in routes.columns:
            routes[c] = pd.to_numeric(routes[c], errors="coerce")

    features = []
    dropped_no_coords = 0

    group_cols = ["from_node", "to_node", "route"]
    for (frm, to, rtype), g in routes.groupby(group_cols, sort=False):
        frm = str(frm); to = str(to); rtype = str(rtype)
        # Need coords at least for endpoints
        if frm not in node_xy or to not in node_xy:
            dropped_no_coords += 1
            continue

        g2 = g[["src", "dst"] + [c for c in ["length_km","weight_km","hotspot_rate","penalty_km"] if c in g.columns]].copy()
        g2["src"] = g2["src"].astype(str)
        g2["dst"] = g2["dst"].astype(str)

        node_seq = _reconstruct_path_edges(g2, frm, to)

        coords = []
        for n in node_seq:
            xy = node_xy.get(str(n))
            if xy is not None:
                coords.append([xy[0], xy[1]])

        if len(coords) < 2:
            dropped_no_coords += 1
            continue

        props = {
            "route_id": f"{frm}__{to}__{rtype}",
            "route": rtype,
            "from_node": frm,
            "to_node": to,
            "n_edges": int(len(g2)),
        }
        if "length_km" in g2.columns:
            props["length_km"] = float(g2["length_km"].sum(skipna=True))
        if "weight_km" in g2.columns:
            props["weight_km"] = float(g2["weight_km"].sum(skipna=True))
        if "hotspot_rate" in g2.columns:
            props["hotspot_mean"] = float(g2["hotspot_rate"].mean(skipna=True))
            props["hotspot_max"] = float(g2["hotspot_rate"].max(skipna=True))

        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": props,
        })

    out_geo = {"type": "FeatureCollection", "features": features}
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out_geo, f)

    print(f"Wrote {args.out} | routes={len(features)} | dropped_no_coords={dropped_no_coords}")
    print(f"Routes source: {routes_path}")


if __name__ == "__main__":
    main()
