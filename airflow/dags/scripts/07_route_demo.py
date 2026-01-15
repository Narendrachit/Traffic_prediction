# scripts/07_route_demo.py
"""
STEP 07 — CONGESTION-AWARE ROUTE COMPARISON (UPDATED, MSc level)

Features:
✅ Top-5 most risky edges per route (by penalty_km, then hotspot_rate)
✅ Save summary CSV (single or all-to-nodes)
✅ Explainability: why avoid-route changed (which risky edges were avoided)
✅ Optional GeoJSON export (Mapbox-ready) if node lon/lat exist
✅ Bulk mode: compute routes from one source to ALL reachable nodes

Outputs (written under data_lake/gold/route_analysis/region=.../run_utc=...):
- route_summary.csv (single) OR routes_summary.csv (bulk)
- route_edges.csv   (single) OR routes_edges.csv   (bulk, if --export_edges)
- route_routes.geojson (single, if --export_geojson)
- routes_routes.geojson (bulk limited, if --export_geojson)
- run_metadata.json
"""

from __future__ import annotations

import argparse
import csv
import json
import pickle
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import networkx as nx
import pandas as pd


DEFAULT_LAKE = Path("data_lake")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_graph(pkl_path: Path):
    with open(pkl_path, "rb") as f:
        return pickle.load(f)


def edges_from_path(path: List[str]) -> List[Tuple[str, str]]:
    return list(zip(path[:-1], path[1:]))


def edge_record(G, u: str, v: str) -> Dict:
    d = G[u][v]
    length_km = float(d.get("length_km", 0.0) or 0.0)
    hotspot_rate = float(d.get("hotspot_rate", 0.0) or 0.0)
    weight_km = float(d.get("weight_km", length_km) or length_km)
    penalty_km = float(d.get("penalty_km", weight_km - length_km) or (weight_km - length_km))
    return {
        "src": u,
        "dst": v,
        "length_km": length_km,
        "hotspot_rate": hotspot_rate,
        "weight_km": weight_km,
        "penalty_km": penalty_km,
    }


def path_stats(G, path: List[str], hotspot_thr: float) -> Dict:
    es = [edge_record(G, u, v) for u, v in edges_from_path(path)]
    total_len = sum(e["length_km"] for e in es)
    total_w = sum(e["weight_km"] for e in es)
    total_pen = sum(e["penalty_km"] for e in es)
    hot_edges = [e for e in es if e["hotspot_rate"] > hotspot_thr]
    avg_hot = (sum(e["hotspot_rate"] for e in es) / len(es)) if es else 0.0
    max_hot = max((e["hotspot_rate"] for e in es), default=0.0)
    return {
        "edges": es,
        "n_edges": len(es),
        "total_length_km": total_len,
        "total_weight_km": total_w,
        "total_penalty_km": total_pen,
        "hot_edges_count": len(hot_edges),
        "hot_edges_share": (len(hot_edges) / len(es) * 100.0) if es else 0.0,
        "avg_hotspot_rate": avg_hot,
        "max_hotspot_rate": max_hot,
    }


def top_risky_edges(edges: List[Dict], k: int = 5) -> List[Dict]:
    return sorted(edges, key=lambda e: (e["penalty_km"], e["hotspot_rate"], e["length_km"]), reverse=True)[:k]


def explain_change(short_edges: List[Dict], avoid_edges: List[Dict], hotspot_thr: float) -> Dict:
    se = {(e["src"], e["dst"]): e for e in short_edges}
    ae = {(e["src"], e["dst"]): e for e in avoid_edges}

    removed = [se[k] for k in se.keys() if k not in ae]
    removed_risky = [e for e in removed if e["hotspot_rate"] > hotspot_thr or e["penalty_km"] > 0]

    removed_top = top_risky_edges(removed_risky, k=5)

    reason = "No change (same path)" if not removed else (
        "Avoid-route changed to bypass higher-risk edges on the shortest route."
        if removed_risky else
        "Avoid-route changed, but removed edges were not above threshold (graph tie/alternative equal cost)."
    )

    return {
        "reason": reason,
        "removed_edges_count": len(removed),
        "removed_risky_edges_count": len(removed_risky),
        "removed_risky_top5": "; ".join(
            [f"{e['src']}->{e['dst']} (risk={e['hotspot_rate']:.3f}, penalty={e['penalty_km']:.3f})" for e in removed_top]
        ),
    }


def node_has_coords(G) -> bool:
    # check a few nodes
    for n, d in list(G.nodes(data=True))[:20]:
        if d.get("lon") is not None and d.get("lat") is not None:
            return True
    return False


def build_geojson_linestring(G, path: List[str], props: Dict) -> Optional[Dict]:
    coords = []
    for n in path:
        d = G.nodes[n]
        lon, lat = d.get("lon"), d.get("lat")
        if lon is None or lat is None:
            return None
        coords.append([float(lon), float(lat)])
    return {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": coords},
        "properties": props,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", required=True, help="e.g., London (used for default paths)")
    ap.add_argument("--data_lake", default=str(DEFAULT_LAKE), help="data_lake root folder")
    ap.add_argument("--graph_pkl", default=None, help="Optional override path to routing_graph_weighted.pkl")

    ap.add_argument("--from_node", required=True)
    ap.add_argument("--to_node", default=None, help="Single destination")
    ap.add_argument("--all_to_nodes", action="store_true", help="Compute routes from from_node to all nodes")
    ap.add_argument("--max_targets", type=int, default=0, help="Limit number of targets in bulk mode (0=all)")

    ap.add_argument("--hotspot_threshold", type=float, default=0.0)
    ap.add_argument("--export_edges", action="store_true", help="Write per-edge CSVs")
    ap.add_argument("--export_geojson", action="store_true", help="Write GeoJSON route lines if coords exist")
    ap.add_argument("--geojson_limit", type=int, default=200, help="Max routes to export in bulk geojson")

    args = ap.parse_args()

    data_lake = Path(args.data_lake)
    region = args.region
    default_graph = data_lake / "gold" / "routing_graph_weighted" / f"region={region}" / "routing_graph_weighted.pkl"
    graph_pkl = Path(args.graph_pkl) if args.graph_pkl else default_graph

    if not graph_pkl.exists():
        raise FileNotFoundError(
            f"Weighted graph not found: {graph_pkl}\n"
            f"Fix: run Step 06 for region={region}."
        )

    G = load_graph(graph_pkl)

    src = args.from_node
    if src not in G.nodes:
        raise ValueError(f"from_node '{src}' not found in graph")

    run_dir = data_lake / "gold" / "route_analysis" / f"region={region}" / f"run_utc={utc_stamp()}"
    run_dir.mkdir(parents=True, exist_ok=True)

    thr = float(args.hotspot_threshold)

    # pick targets
    if args.all_to_nodes:
        targets = [n for n in G.nodes if n != src]
        if args.max_targets and args.max_targets > 0:
            targets = targets[: args.max_targets]
    else:
        if not args.to_node:
            raise ValueError("--to_node is required unless --all_to_nodes is set")
        if args.to_node not in G.nodes:
            raise ValueError(f"to_node '{args.to_node}' not found in graph")
        targets = [args.to_node]

    bulk_rows = []
    edges_rows = []
    geojson_features = []

    can_geo = args.export_geojson and node_has_coords(G)

    for dst in targets:
        try:
            path_short = nx.shortest_path(G, source=src, target=dst, weight="length_km")
            path_avoid = nx.shortest_path(G, source=src, target=dst, weight="weight_km")
        except Exception:
            # unreachable or other path issues
            continue

        st_short = path_stats(G, path_short, thr)
        st_avoid = path_stats(G, path_avoid, thr)

        exp = explain_change(st_short["edges"], st_avoid["edges"], thr)

        bulk_rows.append({
            "region": region,
            "from_node": src,
            "to_node": dst,
            "same_path": path_short == path_avoid,

            "short_len_km": st_short["total_length_km"],
            "short_weight_km": st_short["total_weight_km"],
            "short_penalty_km": st_short["total_penalty_km"],
            "short_hot_edges": st_short["hot_edges_count"],
            "short_hot_share_pct": st_short["hot_edges_share"],
            "short_avg_hotspot": st_short["avg_hotspot_rate"],
            "short_max_hotspot": st_short["max_hotspot_rate"],

            "avoid_len_km": st_avoid["total_length_km"],
            "avoid_weight_km": st_avoid["total_weight_km"],
            "avoid_penalty_km": st_avoid["total_penalty_km"],
            "avoid_hot_edges": st_avoid["hot_edges_count"],
            "avoid_hot_share_pct": st_avoid["hot_edges_share"],
            "avoid_avg_hotspot": st_avoid["avg_hotspot_rate"],
            "avoid_max_hotspot": st_avoid["max_hotspot_rate"],

            "delta_len_km": st_avoid["total_length_km"] - st_short["total_length_km"],
            "delta_weight_km": st_avoid["total_weight_km"] - st_short["total_weight_km"],
            "delta_hot_edges": st_short["hot_edges_count"] - st_avoid["hot_edges_count"],

            "explain_reason": exp["reason"],
            "explain_removed_edges": exp["removed_edges_count"],
            "explain_removed_risky_edges": exp["removed_risky_edges_count"],
            "explain_removed_risky_top5": exp["removed_risky_top5"],
        })

        if args.export_edges:
            # store per-edge records for both routes
            for route_name, path, stats in [
                ("shortest_distance", path_short, st_short),
                ("avoid_hotspots", path_avoid, st_avoid),
            ]:
                for e in stats["edges"]:
                    edges_rows.append({
                        "region": region,
                        "from_node": src,
                        "to_node": dst,
                        "route": route_name,
                        **e,
                    })

        if can_geo:
            feat_a = build_geojson_linestring(G, path_short, {
                "region": region, "from": src, "to": dst, "route": "shortest_distance",
                "length_km": st_short["total_length_km"], "weight_km": st_short["total_weight_km"],
                "hot_edges": st_short["hot_edges_count"]
            })
            feat_b = build_geojson_linestring(G, path_avoid, {
                "region": region, "from": src, "to": dst, "route": "avoid_hotspots",
                "length_km": st_avoid["total_length_km"], "weight_km": st_avoid["total_weight_km"],
                "hot_edges": st_avoid["hot_edges_count"]
            })
            if feat_a: geojson_features.append(feat_a)
            if feat_b: geojson_features.append(feat_b)

    # Write outputs
    if args.all_to_nodes:
        summary_path = run_dir / "routes_summary.csv"
        pd.DataFrame(bulk_rows).to_csv(summary_path, index=False)
        print("Saved outputs")
        print(" -", summary_path)

        if args.export_edges:
            edges_path = run_dir / "routes_edges.csv"
            pd.DataFrame(edges_rows).to_csv(edges_path, index=False)
            print(" -", edges_path)

        if args.export_geojson:
            if not can_geo:
                print("[GeoJSON] Skipped: graph nodes missing lon/lat. Ensure Step06 used --base_graph_pkl.")
            else:
                limited = geojson_features[: max(0, int(args.geojson_limit)) * 2]  # *2 because 2 routes per dest
                geo_path = run_dir / "routes_routes.geojson"
                geo_path.write_text(json.dumps({"type": "FeatureCollection", "features": limited}, indent=2))
                print(" -", geo_path)

    else:
        summary_path = run_dir / "route_summary.csv"
        pd.DataFrame(bulk_rows).to_csv(summary_path, index=False)
        print("Saved outputs")
        print(" -", summary_path)

        if args.export_edges:
            edges_path = run_dir / "route_edges.csv"
            pd.DataFrame(edges_rows).to_csv(edges_path, index=False)
            print(" -", edges_path)

        if args.export_geojson:
            if not can_geo:
                print("[GeoJSON] Skipped: graph nodes missing lon/lat. Ensure Step06 used --base_graph_pkl.")
            else:
                geo_path = run_dir / "route_routes.geojson"
                geo_path.write_text(json.dumps({"type": "FeatureCollection", "features": geojson_features}, indent=2))
                print(" -", geo_path)

    meta = {
        "region": region,
        "graph_pkl": str(graph_pkl),
        "from_node": src,
        "all_to_nodes": bool(args.all_to_nodes),
        "targets_attempted": len(targets),
        "hotspot_threshold": thr,
        "export_edges": bool(args.export_edges),
        "export_geojson": bool(args.export_geojson),
    }
    (run_dir / "run_metadata.json").write_text(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
