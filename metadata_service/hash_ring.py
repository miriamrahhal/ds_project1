"""Consistent hashing ring with virtual nodes."""
import bisect
import hashlib


class HashRing:
    def __init__(self, nodes=(), vnodes: int = 128):
        self.vnodes = vnodes
        self._nodes = set(nodes)
        self._positions = []   # sorted ring positions
        self._owners = []      # node owning the position at the same index
        self._rebuild()

    @staticmethod
    def _position(label: str) -> int:
        return int.from_bytes(hashlib.sha256(label.encode()).digest()[:8], "big")

    def _rebuild(self):
        points = sorted(
            (self._position(f"{node}#{i}"), node)
            for node in self._nodes
            for i in range(self.vnodes)
        )
        self._positions = [p for p, _ in points]
        self._owners = [n for _, n in points]

    @property
    def nodes(self) -> list:
        return sorted(self._nodes)

    def copy(self) -> "HashRing":
        return HashRing(self._nodes, self.vnodes)

    def add_node(self, node_id: str):
        self._nodes.add(node_id)
        self._rebuild()

    def remove_node(self, node_id: str):
        self._nodes.discard(node_id)
        self._rebuild()

    def get_nodes(self, key: str, count: int = 1) -> list:
        """First `count` distinct nodes clockwise from the key's position."""
        if not self._owners:
            return []
        count = min(count, len(self._nodes))
        idx = bisect.bisect_right(self._positions, self._position(key)) % len(self._owners)
        chosen = []
        while len(chosen) < count:
            node = self._owners[idx]
            if node not in chosen:
                chosen.append(node)
            idx = (idx + 1) % len(self._owners)
        return chosen