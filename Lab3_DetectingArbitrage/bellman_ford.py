"""
The Bellman-Ford algorithm
"""
from datetime import datetime


def initialize(graph, start_vertex):
    """
    This function initializes two dictionaries that are required to run BellmanFord algorithm with a
    dynamic programming approach

    :param graph: the input graph
    :param start_vertex: the source of shortest path
    :return: distance and predecessor dictionaries
    """
    dist = {}  # distance to the destination vertex: dictionary keyed by vertex of shortest distance
    # from start_vertex to that vertex
    prev = {}  # predecessor: dictionary keyed by vertex of previous vertex in shortest path from start_vertex
    for node in graph:
        dist[node] = float('Inf')  # distances from the source to all vertices (including the source itself)
        # are set as infinite
        prev[node] = None  # at the beginning we don't know the predecessor of nodes in the shortest path
    dist[start_vertex] = 0  # distance to the start vertex is zero
    return dist, prev


def relax(node, neighbour, graph, d, p, tolerance):
    # If the distance between the node and the neighbour is lower than the one I have now, update
    # my distance to this lower distance
    if d[node] != float('Inf') and d[neighbour] + tolerance > d[node] + graph[node][neighbour][0]:
        d[neighbour] = d[node] + graph[node][neighbour][0]
        p[neighbour] = node


def bellman_ford(graph, start_vertex, tolerance=0):
    """
    This function run the Bellman Ford algorithm to find the shortest path from start_vertex

    :param graph: The graph data structure is stored in a dictionary. The dictionaries' keys are the graph's nodes.
    The corresponding value for each key is a dictionary of nodes connected by a direct edge from this node.
    The value for each connected node is a numeric value that shows the weight of the edge.
    So, value in graph[u][v] gives weight of edge (u, v)
    :param start_vertex: source of the shortest path
    :param tolerance: For relaxation and cycle detection, we use tolerance. Only relaxations resulting in an improvement
    greater than tolerance are considered. For negative cycle detection, if the sum of weights is
    greater than -tolerance it is not reported as a negative cycle. This is useful when circuits are expected
    to be close to zero.

    :return: the distance list, previous list, and one of the edges on a negative cycle
    """
    dist, prev = initialize(graph, start_vertex)
    for i in range(len(graph) - 1):
        for u in graph:
            for v in graph[u]:  # For each neighbour of u
                relax(u, v, graph, dist, prev, tolerance)

    # check for negative-weight cycles
    neg_edge = None
    for u in graph:
        for v in graph[u]:
            if dist[u] is not None and dist[v] + tolerance > dist[u] + graph[u][v][0]:
                # If True, the graph has a negative weight cycle
                neg_edge = (v, u)
                break
        if neg_edge is not None:
            break

    return dist, prev, neg_edge


# def shortest_paths(start_vertex, tolerance=0):
#     """
#     Find the shortest paths (sum of edge weights) from start_vertex to every other vertex.
#     Also detect if there are negative cycles and report one of them.
#     Edges may be negative.
#
#     For relaxation and cycle detection, we use tolerance. Only relaxations resulting in an improvement
#     greater than tolerance are considered. For negative cycle detection, if the sum of weights is
#     greater than -tolerance it is not reported as a negative cycle. This is useful when circuits are expected
#     to be close to zero.
#
#     >>> g = BellmanFord({'a': {'b': 1, 'c':5}, 'b': {'c': 2, 'a': 10}, 'c': {'a': 14, 'd': -3}, 'e': {'a': 100}})
#     >>> dist, prev, neg_edge = g.shortest_paths('a')
#     >>> [(v, dist[v]) for v in sorted(dist)]  # shortest distance from 'a' to each other vertex
#     [('a', 0), ('b', 1), ('c', 3), ('d', 0), ('e', inf)]
#     >>> [(v, prev[v]) for v in sorted(prev)]  # last edge in shortest paths
#     [('a', None), ('b', 'a'), ('c', 'b'), ('d', 'c'), ('e', None)]
#     >>> neg_edge is None
#     True
#     >>> g.add_edge('a', 'e', -200)
#     >>> dist, prev, neg_edge = g.shortest_paths('a')
#     >>> neg_edge  # edge where we noticed a negative cycle
#     ('e', 'a')
#
#     :param start_vertex: start of all paths
#     :param tolerance: only if a path is more than tolerance better will it be relaxed
#     :return: distance, predecessor, negative_cycle
#         distance:       dictionary keyed by vertex of shortest distance from start_vertex to that vertex
#         predecessor:    dictionary keyed by vertex of previous vertex in shortest path from start_vertex
#         negative_cycle: None if no negative cycle, otherwise an edge, (u,v), in one such cycle
#     """

def find_negative_cycle(graph, vertex, prev):
    graph_vertices = list(graph.keys())
    for i in range(len(graph_vertices)):
        vertex = prev[vertex]
    cycle = []
    v = vertex
    while True:
        cycle.append(v)
        if v == vertex and len(cycle) > 1:
            break
        v = prev[v]

    cycle.reverse()
    return cycle


def test_with_neg_cyc():
    # This graph has this arbitrage opportunity:
    # start with USD 100
    # 	    exchange USD for GBP at 0.7988496564946477 --> GBP 79.88496564946476
    # 	    exchange GBP for CAD at 0.6232826272068851 --> CAD 49.79091126433016
    # 	    exchange CAD for AUD at 3.3290805390098406 --> AUD 165.7579537096474
    # 	    exchange AUD for USD at 0.75057 --> USD 124.41294731585005
    graph = {
        'USD': {'AUD': (-0.750, datetime.now()), 'GBP': (0.798, datetime.now()),
                'JPY': (-100.014, datetime.now())},
        'GBP': {'CAD': (0.623, datetime.now()), 'USD': (-0.798, datetime.now())},
        'CAD': {'AUD': (3.329, datetime.now()), 'GBP': (-0.623, datetime.now())},
        'AUD': {'USD': (0.750, datetime.now()), 'CAD': (-3.329, datetime.now())},
        'JPY': {'USD': (100.014, datetime.now())}
    }

    dist, prev, neg_edge = bellman_ford(graph, 'USD')
    # shortest_dis = [(v, dist[v]) for v in sorted(dist)]
    # print("shortest_distance = {}".format(shortest_dis))
    #
    # last_edge = [(v, prev[v]) for v in sorted(prev)]
    # print("last_edge= {}".format(last_edge))
    #
    # print(" Negative cycle? {}".format('No negative' if neg_edge is None else neg_edge))

    cycle = find_negative_cycle(graph, neg_edge[1], prev)
    print(cycle)


if __name__ == '__main__':
    test_with_neg_cyc()
