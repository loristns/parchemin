from collections import deque
from collections.abc import Generator


class Graph[K, V]:
    def __init__(self) -> None:
        self.nodes: dict[K, V] = {}
        self.edges: dict[K, list[K]] = {}

    def add_node(self, key: K, value: V) -> None:
        if key in self.nodes:
            raise ValueError(f"Node {key} already exists")
        self.nodes[key] = value

    def add_edge(self, from_key: K, to_key: K) -> None:
        self.edges.setdefault(from_key, []).append(to_key)

    def get_node_connected_edges(self, key: K) -> list[K]:
        return [edge for edge in self.edges.get(key, []) if edge in self.nodes]

    @property
    def connected_edges(self) -> dict[K, list[K]]:
        return {key: self.get_node_connected_edges(key) for key in self.nodes}

    def topological_sort(self) -> Generator[K]:
        # Kahn's algorithm for topological sorting

        # For each node, count the number of incoming edges
        incoming_count: dict[K, int] = dict.fromkeys(self.nodes, 0)
        for to_keys in self.connected_edges.values():
            for to_key in to_keys:
                incoming_count[to_key] += 1

        # Initialize the queue with nodes that have no incoming edges
        queue = deque[K]()
        for node, count in incoming_count.items():
            if count == 0:
                queue.append(node)

        yielded_count = 0

        # Process the nodes in the queue
        while queue:
            node = queue.popleft()
            yield node
            yielded_count += 1

            # Then, for each node connected to the current node, decrement the incoming
            # count and if the incoming count is now 0, add the node to the queue.
            for to_key in self.get_node_connected_edges(node):
                incoming_count[to_key] -= 1
                if incoming_count[to_key] == 0:
                    queue.append(to_key)

        # If we didn't yield all the nodes: there is a cycle.
        if yielded_count != len(self.nodes):
            blocked = [node for node, count in incoming_count.items() if count > 0]
            raise RecursionError(
                "Graph contains a cycle between the following nodes: "
                + ", ".join(repr(node) for node in blocked)
            )
