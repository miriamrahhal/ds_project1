"""Measure how many chunks move when a node is added to / removed from the ring."""
import hashlib
import random

from metadata_service.hash_ring import HashRing

N_CHUNKS = 100_000
REPLICAS = 3


def make_hashes(n):
    rng = random.Random(42)
    return [hashlib.sha256(rng.randbytes(16)).hexdigest() for _ in range(n)]


def measure(before, after, hashes, replicas):
    primary = sets = copies = 0
    for h in hashes:
        b, a = before.get_nodes(h, replicas), after.get_nodes(h, replicas)
        primary += b[0] != a[0]
        sets += set(b) != set(a)
        copies += len(set(a) - set(b))
    pct = lambda x, d=len(hashes): 100 * x / d
    return pct(primary), pct(sets), pct(copies, len(hashes) * replicas)


def report(title, before, after, hashes, ideal):
    primary, sets, copies = measure(before, after, hashes, REPLICAS)
    print(f"{title}")
    print(f"  primary owner changed : {primary:5.1f}%   (ideal ~{ideal:.1f}%)")
    print(f"  replica set changed   : {sets:5.1f}%   (R={REPLICAS})")
    print(f"  replica copies moved  : {copies:5.1f}%   (ideal ~{ideal:.1f}%)")
    return primary


def main():
    hashes = make_hashes(N_CHUNKS)
    base = HashRing(["node1", "node2", "node3", "node4"], vnodes=128)

    # load balance
    load = {n: 0 for n in base.nodes}
    for h in hashes:
        load[base.get_nodes(h, 1)[0]] += 1
    print("Primary load per node:", {n: f"{100 * c / N_CHUNKS:.1f}%" for n, c in load.items()})
    print()

    grown = base.copy()
    grown.add_node("node5")
    p_add = report("Add node5 (4 -> 5 nodes)", base, grown, hashes, 100 / 5)
    print()

    shrunk = base.copy()
    shrunk.remove_node("node4")
    p_rem = report("Remove node4 (4 -> 3 nodes)", base, shrunk, hashes, 100 / 4)
    print()

    print("Effect of virtual nodes (add a 5th node):")
    print("  vnodes | max load / mean | primary moved")
    for v in (1, 8, 32, 128, 512):
        r = HashRing(["node1", "node2", "node3", "node4"], vnodes=v)
        counts = {n: 0 for n in r.nodes}
        for h in hashes:
            counts[r.get_nodes(h, 1)[0]] += 1
        r2 = r.copy()
        r2.add_node("node5")
        moved, _, _ = measure(r, r2, hashes, 1)
        print(f"  {v:6d} | {max(counts.values()) / (N_CHUNKS / 4):15.2f} | {moved:12.1f}%")

    assert abs(p_add - 20) < 5, p_add
    assert abs(p_rem - 25) < 5, p_rem
    print("\nOK: movement is close to the ideal 1/N.")


if __name__ == "__main__":
    main()