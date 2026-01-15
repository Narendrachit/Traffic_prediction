import pickle
import networkx as nx

GRAPH_PKL = r"data_lake\gold\routing_graph_weighted\region=London\routing_graph_weighted.pkl"
SRC = "A407"
DST = "Bugsby's Way"

G = pickle.load(open(GRAPH_PKL, "rb"))

p_dist = nx.shortest_path(G, SRC, DST, weight="length_km")
p_avoid = nx.shortest_path(G, SRC, DST, weight="weight_km")

def path_cost(path, key):
    s = 0.0
    for u, v in zip(path, path[1:]):
        s += float(G[u][v].get(key, 0.0))
    return s

def path_edges(path):
    out = []
    for u, v in zip(path, path[1:]):
        data = G[u][v]
        out.append({
            "src": u,
            "dst": v,
            "length_km": float(data.get("length_km", 0.0)),
            "hotspot_rate": float(data.get("hotspot_rate", 0.0)),
            "weight_km": float(data.get("weight_km", 0.0)),
        })
    return out

def summarize(path, name):
    edges = path_edges(path)
    n_hot = sum(1 for e in edges if e["hotspot_rate"] > 0)
    print(f"\n{name}")
    print(" -> ".join(path))
    print("Total length_km:", path_cost(path, "length_km"))
    print("Total weight_km:", path_cost(path, "weight_km"))
    print("Hotspot edges used:", n_hot, "out of", len(edges))

    # print hotspot edges if any
    hot_edges = [e for e in edges if e["hotspot_rate"] > 0]
    if hot_edges:
        print("\nHotspot edges on this route:")
        for e in hot_edges:
            print(f"  {e['src']} -> {e['dst']} | hotspot_rate={e['hotspot_rate']:.3f} | len={e['length_km']:.2f} | weight={e['weight_km']:.2f}")
    else:
        print("No hotspot edges on this route ")

summarize(p_dist, "Shortest-distance route")
summarize(p_avoid, "Avoid-hotspots route")

print("\nSame path?", p_dist == p_avoid)
