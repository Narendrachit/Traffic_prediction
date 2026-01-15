# scripts/04_build_routing_graph.py
"""
STEP 04 — BUILD ROUTING GRAPH (UPDATED, MSc level)

Builds a directed routing graph from a parquet dataset (DfT traffic counts).
- Uses only required columns (avoids schema-cast issues like date32 casts)
- Allows explicit column mapping (start/end junction, lat/lon, length)
- Aggregates edge lengths and derives node coordinates (mean/median)
- Exports:
  - routing_edges.csv (src, dst, length_km, n_rows)
  - routing_nodes.csv (node_id, lon, lat, n_rows)
  - routing_graph.pkl (NetworkX graph with node coords + edge lengths)
  - metadata.json
"""

from __future__ import annotations
from scripts._spark_win import get_spark, spark_path

import argparse
import json
import math
import pickle
from pathlib import Path
from typing import List, Optional

import pandas as pd


def _norm(s: str) -> str:
    return "".join(ch.lower() for ch in s if ch.isalnum())


def _find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    norm_map = {_norm(c): c for c in cols}
    for cand in candidates:
        key = _norm(cand)
        if key in norm_map:
            return norm_map[key]
    # also allow fuzzy-ish matches
    for c in cols:
        nc = _norm(c)
        for cand in candidates:
            if _norm(cand) in nc or nc in _norm(cand):
                return c
    return None


def read_parquet_columns(parquet_path: str, columns: List[str]) -> pd.DataFrame:
    """
    Safe parquet reader:
    - Reads only requested columns (prevents Arrow cast issues on unrelated columns)
    - Works for parquet file or directory.
    """
    try:
        import pyarrow.dataset as ds  # type: ignore

        dataset = ds.dataset(parquet_path, format="parquet")
        table = dataset.to_table(columns=columns)
        return table.to_pandas()
    except Exception:
        # fallback to pandas (still uses pyarrow engine if available)
        return pd.read_parquet(parquet_path, columns=columns)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet_path", required=True, help="Parquet file/folder (can be a region=London folder)")
    ap.add_argument("--out_dir", required=True, help="Output directory (gold/routing_graph/region=London)")

    # Explicit mappings (recommended for your dataset)
    ap.add_argument("--from_col", default=None)
    ap.add_argument("--to_col", default=None)
    ap.add_argument("--lon_col", default=None)
    ap.add_argument("--lat_col", default=None)
    ap.add_argument("--len_col", default=None)

    ap.add_argument("--coord_agg", choices=["mean", "median"], default="mean", help="How to aggregate node coords")
    ap.add_argument("--directed", action="store_true", help="Build directed graph (recommended)")
    ap.add_argument("--dropna_coords", action="store_true", help="Drop rows with missing lon/lat")
    ap.add_argument("--min_len_km", type=float, default=0.001, help="Drop edges shorter than this")
    args = ap.parse_args()

    parquet_path = args.parquet_path
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Peek schema columns WITHOUT reading all data
    try:
        import pyarrow.dataset as ds  # type: ignore
        cols = list(ds.dataset(parquet_path, format="parquet").schema.names)
    except Exception:
        # fallback: cheap read of 1 row (may still work)
        df_head = pd.read_parquet(parquet_path).head(1)
        cols = list(df_head.columns)

    # Auto-detect if user didn't supply
    from_col = args.from_col or _find_col(cols, ["from_node", "start_junction_road_name", "start_junction", "from", "src", "u", "source"])
    to_col = args.to_col or _find_col(cols, ["to_node", "end_junction_road_name", "end_junction", "to", "dst", "v", "target"])
    lon_col = args.lon_col or _find_col(cols, ["lon", "longitude", "lng"])
    lat_col = args.lat_col or _find_col(cols, ["lat", "latitude"])
    len_col = args.len_col or _find_col(cols, ["length_km", "link_length_km", "len_km", "distance_km"])

    missing = [("from_col", from_col), ("to_col", to_col), ("lon_col", lon_col), ("lat_col", lat_col), ("len_col", len_col)]
    missing = [k for k, v in missing if v is None]
    if missing:
        raise ValueError(
            "Missing required column mapping(s): "
            + ", ".join(missing)
            + f"\nAvailable columns: {cols}"
        )

    use_cols = [from_col, to_col, lon_col, lat_col, len_col]  # ONLY needed columns (prevents Arrow cast errors)
    df = read_parquet_columns(parquet_path, use_cols)

    # Clean
    df = df.rename(columns={
        from_col: "src_raw",
        to_col: "dst_raw",
        lon_col: "lon_raw",
        lat_col: "lat_raw",
        len_col: "length_raw",
    })

    # Normalize node ids
    df["src"] = df["src_raw"].astype(str).str.strip()
    df["dst"] = df["dst_raw"].astype(str).str.strip()

    # Numeric fields
    df["lon"] = pd.to_numeric(df["lon_raw"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat_raw"], errors="coerce")
    df["length_km"] = pd.to_numeric(df["length_raw"], errors="coerce")

    df = df.dropna(subset=["src", "dst", "length_km"])
    df = df[df["src"].str.len() > 0]
    df = df[df["dst"].str.len() > 0]
    df = df[df["length_km"] >= float(args.min_len_km)]

    if args.dropna_coords:
        df = df.dropna(subset=["lon", "lat"])

    # Aggregate edges
    edges = (
        df.groupby(["src", "dst"], as_index=False)
          .agg(
              length_km=("length_km", "mean"),
              n_rows=("length_km", "size"),
          )
    )

    # Derive node coords by stacking endpoints (best-effort)
    long_nodes = pd.concat(
        [
            df[["src", "lon", "lat"]].rename(columns={"src": "node_id"}),
            df[["dst", "lon", "lat"]].rename(columns={"dst": "node_id"}),
        ],
        axis=0,
        ignore_index=True,
    ).dropna(subset=["node_id"])

    agg = "mean" if args.coord_agg == "mean" else "median"
    if agg == "mean":
        nodes = long_nodes.groupby("node_id", as_index=False).agg(
            lon=("lon", "mean"),
            lat=("lat", "mean"),
            n_rows=("node_id", "size"),
        )
    else:
        nodes = long_nodes.groupby("node_id", as_index=False).agg(
            lon=("lon", "median"),
            lat=("lat", "median"),
            n_rows=("node_id", "size"),
        )

    # Build graph
    import networkx as nx
    G = nx.DiGraph() if args.directed else nx.Graph()

    for r in nodes.itertuples(index=False):
        lon = None if (r.lon is None or (isinstance(r.lon, float) and math.isnan(r.lon))) else float(r.lon)
        lat = None if (r.lat is None or (isinstance(r.lat, float) and math.isnan(r.lat))) else float(r.lat)
        G.add_node(str(r.node_id), lon=lon, lat=lat, n_rows=int(r.n_rows))

    for r in edges.itertuples(index=False):
        G.add_edge(str(r.src), str(r.dst), length_km=float(r.length_km), n_rows=int(r.n_rows))

    # Save outputs
    edges_csv = out_dir / "routing_edges.csv"
    nodes_csv = out_dir / "routing_nodes.csv"
    graph_pkl = out_dir / "routing_graph.pkl"
    meta_json = out_dir / "metadata.json"

    edges.to_csv(edges_csv, index=False)
    nodes.to_csv(nodes_csv, index=False)
    with open(graph_pkl, "wb") as f:
        pickle.dump(G, f)

    meta = {
        "step": "04_build_routing_graph",
        "parquet_path": str(parquet_path),
        "out_dir": str(out_dir),
        "from_col": from_col,
        "to_col": to_col,
        "lon_col": lon_col,
        "lat_col": lat_col,
        "len_col": len_col,
        "coord_agg": args.coord_agg,
        "directed": bool(args.directed),
        "edges": int(edges.shape[0]),
        "nodes": int(nodes.shape[0]),
    }
    meta_json.write_text(json.dumps(meta, indent=2))

    print("DONE Step4 (UPDATED with node coords + safe parquet read)")
    print(f"Edges: {meta['edges']} | Nodes: {meta['nodes']}")
    print("Saved:", edges_csv)
    print("Saved:", nodes_csv)
    print("Saved:", graph_pkl)
    print("Saved:", meta_json)


if __name__ == "__main__":
    main()
