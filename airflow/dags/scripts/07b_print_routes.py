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

print("Shortest-distance path:")
print(" -> ".join(p_dist))
print("length_km=", path_cost(p_dist, "length_km"), " weight_km=", path_cost(p_dist, "weight_km"))

print("\nAvoid-hotspots path:")
print(" -> ".join(p_avoid))
print("length_km=", path_cost(p_avoid, "length_km"), " weight_km=", path_cost(p_avoid, "weight_km"))

print("\nSame path?", p_dist == p_avoid)
