# scripts/06_build_weighted_routing_graph.py
"""
STEP 06 — BUILD WEIGHTED ROUTING GRAPH (UPDATED, MSc level)

Inputs:
- routing_edges.csv from Step 04 (src, dst, length_km)
- edge_hotspot_rate.csv from Step 05 (from_node,to_node,hotspot_rate, ...)
- OPTIONAL: routing_graph.pkl from Step 04 to preserve node lon/lat

Outputs:
- routing_edges_weighted.csv
- routing_graph_weighted.pkl (keeps node coords if base_graph_pkl provided)
- metadata.json
"""

from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path
from typing import List, Optional

import pandas as pd


def _norm(s: str) -> str:
    return "".join(ch.lower() for ch in s if ch.isalnum())


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = list(df.columns)
    norm_map = {_norm(c): c for c in cols}
    for cand in candidates:
        key = _norm(cand)
        if key in norm_map:
            return norm_map[key]
    for c in cols:
        nc = _norm(c)
        for cand in candidates:
            if _norm(cand) in nc or nc in _norm(cand):
                return c
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--routing_edges_csv", required=True)
    ap.add_argument("--edge_hotspot_rate_csv", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--alpha", type=float, default=2.0, help="Penalty strength: weight_km = length_km*(1+alpha*hotspot_rate)")
    ap.add_argument("--base_graph_pkl", default=None, help="Optional: Step04 routing_graph.pkl to preserve node lon/lat")
    ap.add_argument("--clip_min", type=float, default=0.0)
    ap.add_argument("--clip_max", type=float, default=1.0)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    edges = pd.read_csv(args.routing_edges_csv)
    hot = pd.read_csv(args.edge_hotspot_rate_csv)

    # Detect columns
    e_src = _find_col(edges, ["src", "from", "from_node", "u", "source"])
    e_dst = _find_col(edges, ["dst", "to", "to_node", "v", "target"])
    e_len = _find_col(edges, ["length_km", "len_km", "link_length_km", "distance_km"])

    h_src = _find_col(hot, ["src", "from", "from_node", "u", "source"])
    h_dst = _find_col(hot, ["dst", "to", "to_node", "v", "target"])
    h_rate = _find_col(hot, ["hotspot_rate", "rate", "risk", "congestion_rate"])

    if not (e_src and e_dst and e_len):
        raise ValueError(f"routing_edges_csv missing required cols. Found: {list(edges.columns)}")
    if not (h_src and h_dst and h_rate):
        raise ValueError(
            "edge_hotspot_rate_csv must contain (src/from_node) + (dst/to_node) + (hotspot_rate).\n"
            f"Found: {list(hot.columns)}"
        )

    edges = edges.rename(columns={e_src: "src", e_dst: "dst", e_len: "length_km"})
    hot = hot.rename(columns={h_src: "src", h_dst: "dst", h_rate: "hotspot_rate"})

    edges["length_km"] = pd.to_numeric(edges["length_km"], errors="coerce")
    hot["hotspot_rate"] = pd.to_numeric(hot["hotspot_rate"], errors="coerce").fillna(0.0)

    hot["hotspot_rate"] = hot["hotspot_rate"].clip(lower=args.clip_min, upper=args.clip_max)

    merged = edges.merge(hot[["src", "dst", "hotspot_rate"]], on=["src", "dst"], how="left")
    merged["hotspot_rate"] = merged["hotspot_rate"].fillna(0.0)

    merged["weight_km"] = merged["length_km"] * (1.0 + float(args.alpha) * merged["hotspot_rate"])
    merged["penalty_km"] = merged["weight_km"] - merged["length_km"]

    # Build weighted graph (preserve coords if base_graph_pkl present)
    import networkx as nx

    if args.base_graph_pkl:
        with open(args.base_graph_pkl, "rb") as f:
            G = pickle.load(f)
        # Ensure it's a graph
        if not hasattr(G, "nodes") or not hasattr(G, "edges"):
            raise ValueError("base_graph_pkl is not a valid NetworkX graph")
    else:
        G = nx.DiGraph()

    # If graph was empty, add nodes from edges
    if len(G.nodes) == 0:
        for n in pd.unique(pd.concat([merged["src"], merged["dst"]], ignore_index=True)):
            G.add_node(str(n))

    # Update edges
    # First clear and rebuild if graph doesn't contain edges (safer)
    if G.number_of_edges() == 0:
        for r in merged.itertuples(index=False):
            G.add_edge(str(r.src), str(r.dst),
                       length_km=float(r.length_km),
                       hotspot_rate=float(r.hotspot_rate),
                       weight_km=float(r.weight_km),
                       penalty_km=float(r.penalty_km))
    else:
        # Update existing edges; add any missing
        for r in merged.itertuples(index=False):
            u, v = str(r.src), str(r.dst)
            if not G.has_edge(u, v):
                G.add_edge(u, v)
            G[u][v]["length_km"] = float(r.length_km)
            G[u][v]["hotspot_rate"] = float(r.hotspot_rate)
            G[u][v]["weight_km"] = float(r.weight_km)
            G[u][v]["penalty_km"] = float(r.penalty_km)

    out_edges = out_dir / "routing_edges_weighted.csv"
    out_pkl = out_dir / "routing_graph_weighted.pkl"
    out_meta = out_dir / "metadata.json"

    merged.to_csv(out_edges, index=False)
    with open(out_pkl, "wb") as f:
        pickle.dump(G, f)

    meta = {
        "step": "06_build_weighted_routing_graph",
        "routing_edges_csv": str(args.routing_edges_csv),
        "edge_hotspot_rate_csv": str(args.edge_hotspot_rate_csv),
        "base_graph_pkl": str(args.base_graph_pkl) if args.base_graph_pkl else None,
        "out_dir": str(out_dir),
        "alpha": float(args.alpha),
        "edges": int(merged.shape[0]),
        "nodes": int(G.number_of_nodes()),
    }
    out_meta.write_text(json.dumps(meta, indent=2))

    print("DONE Step6 (UPDATED)")
    print("Alpha:", float(args.alpha))
    print(f"Edges: {meta['edges']} | Nodes: {meta['nodes']}")
    print("Saved:", out_edges)
    print("Saved:", out_pkl)
    print("Saved:", out_meta)


if __name__ == "__main__":
    main()
