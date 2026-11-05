"""Repair jobs: rebuild the chunks of a failed node on the remaining nodes."""
import logging
import threading
from datetime import datetime, timezone

import requests

from metadata_service import erasure, metadata_store as store, placement_api as api
from metadata_service.chunking import sha256_hex

log = logging.getLogger("repair")
jobs: list[dict] = []
repair_lock = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _holders(chunk_hash: str, nodes: list, first_only: bool = False) -> list:
    """Return the nodes that hold this chunk or fragment."""
    found = []
    for node in nodes:
        try:
            if requests.head(api._url(node, chunk_hash), timeout=5).status_code == 200:
                found.append(node)
                if first_only:
                    break
        except requests.RequestException:
            continue
    return found

def _repair_replicated(chunk: dict, ring, job: dict) -> None:
    chunk_hash = chunk["hash"]
    holders = _holders(chunk_hash, ring.nodes)
    wanted = min(api.REPLICAS, len(ring.nodes))
    missing = wanted - len(holders)
    if missing <= 0:
        return

    data = api._read_chunk(chunk_hash, ring, quorum=1)
    job["replication_bytes_read"] += len(data)
    targets = ring.get_nodes(chunk_hash, len(ring.nodes))
    for node in [n for n in targets if n not in holders][:missing]:
        requests.put(api._url(node, chunk_hash), data=data, timeout=30).raise_for_status()
        job["replication_bytes_written"] += len(data)
        job["copies_rebuilt"] += 1
    job["chunks_repaired"] += 1


def _repair_erasure(chunk: dict, ring, job: dict) -> None:
    k, m = chunk["k"], chunk["m"]
    fragment_hashes = chunk["fragment_hashes"]
    order = ring.get_nodes(chunk["hash"], len(ring.nodes))
    holders = {i: _holders(h, order, first_only=True) for i, h in enumerate(fragment_hashes)}
    missing = [i for i, nodes in holders.items() if not nodes]
    if not missing:
        return

    available = {}
    for i, fragment_hash in enumerate(fragment_hashes):
        if i in missing:
            continue
        fragment = api._read_fragment(fragment_hash, holders[i][0], ring)
        if fragment is not None:
            available[i] = fragment
            job["erasure_bytes_read"] += len(fragment)
        if len(available) == k:
            break

    data = erasure.decode(available, k, m, chunk["size_bytes"])
    if sha256_hex(data) != chunk["hash"]:
        raise ValueError(f"rebuilt chunk {chunk['hash'][:12]} has the wrong hash")
    rebuilt = erasure.encode(data, k, m)

    used = {node for nodes in holders.values() for node in nodes}
    for i in missing:
        targets = ring.get_nodes(chunk["hash"], len(ring.nodes))
        free = [n for n in targets if n not in used]
        node = free[0] if free else targets[i % len(targets)]
        used.add(node)
        requests.put(api._url(node, fragment_hashes[i]), data=rebuilt[i], timeout=30).raise_for_status()
        job["erasure_bytes_written"] += len(rebuilt[i])
        job["copies_rebuilt"] += 1
    job["chunks_repaired"] += 1


def run_repair(reason: str) -> dict:
    """Check every chunk and rebuild any missing copy or fragment."""
    job = {
        "id": len(jobs) + 1, "reason": reason, "status": "running",
        "started_at": _now(), "finished_at": None,
        "chunks_checked": 0, "chunks_total": 0, "chunks_repaired": 0, "copies_rebuilt": 0,
        "replication_bytes_read": 0, "replication_bytes_written": 0,
        "erasure_bytes_read": 0, "erasure_bytes_written": 0, "failures": 0,
    }
    jobs.append(job)
    with repair_lock:
        ring = api.ring
        with store.transaction() as cur:
            chunks = store.all_chunks(cur)
        job["chunks_total"] = len(chunks)
        for chunk in chunks:
            try:
                if chunk["mode"] == "erasure":
                    _repair_erasure(chunk, ring, job)
                else:
                    _repair_replicated(chunk, ring, job)
            except Exception as exc:
                job["failures"] += 1
                log.warning("repair of chunk %s failed: %s", chunk["hash"][:12], exc)
            job["chunks_checked"] += 1
    job["status"] = "completed" if not job["failures"] else "completed with failures"
    job["finished_at"] = _now()
    log.info("Repair job %s finished: %s", job["id"], job)
    return job


def handle_node_failure(node_id: str) -> None:
    """Take a failed node out of the ring, then rebuild what it held."""
    with api.ring_lock:
        if node_id not in api.ring.nodes:
            return
        new_ring = api.ring.copy()
        new_ring.remove_node(node_id)
        api.ring = new_ring
    log.warning("Node %s marked as failed. Starting repair.", node_id)
    run_repair(f"node {node_id} failed")


def handle_node_recovery(node_id: str) -> None:
    """Return a node to the ring when its heartbeats resume."""
    with api.ring_lock:
        if node_id in api.ring.nodes:
            return
        new_ring = api.ring.copy()
        new_ring.add_node(node_id)
        api.ring = new_ring
    log.info("Node %s recovered and returned to the ring.", node_id)