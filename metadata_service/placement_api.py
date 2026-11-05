"""Object + placement endpoints: chunking, dedup, ring placement, rebalancing."""
import logging
import os
from collections import Counter
from threading import Lock
from typing import Optional

import requests
from fastapi import APIRouter, HTTPException, Request, Response
from starlette.concurrency import run_in_threadpool

from metadata_service import erasure, metadata_store as store
from metadata_service.chunking import CHUNKERS, sha256_hex
from metadata_service.hash_ring import HashRing

log = logging.getLogger("placement")
router = APIRouter()

REPLICAS = int(os.getenv("REPLICAS", "3"))
WRITE_QUORUM = int(os.getenv("WRITE_QUORUM", "2"))
READ_QUORUM = int(os.getenv("READ_QUORUM", "2"))
if WRITE_QUORUM + READ_QUORUM <= REPLICAS:
    raise ValueError("Quorum settings must satisfy W + R > N")
VNODES = int(os.getenv("VNODES", "128"))
DEFAULT_EC_K = int(os.getenv("EC_K", "4"))
DEFAULT_EC_M = int(os.getenv("EC_M", "2"))
NODE_URL_TEMPLATE = os.getenv("NODE_URL_TEMPLATE", "http://{node_id}:8000")

ring = HashRing(
    [n.strip() for n in os.getenv("INITIAL_NODES", "node1,node2,node3,node4").split(",") if n.strip()],
    vnodes=VNODES,
)
ring_lock = Lock()


# ---------------------------------------------------------------- node I/O
def _url(node_id: str, chunk_hash: str) -> str:
    return f"{NODE_URL_TEMPLATE.format(node_id=node_id)}/chunks/{chunk_hash}"


def _delete_chunk(chunk_hash: str, nodes) -> None:
    for node in nodes:
        try:
            requests.delete(_url(node, chunk_hash), timeout=5)
        except requests.RequestException as exc:
            log.warning("delete %s on %s failed: %s", chunk_hash[:12], node, exc)


def _write_chunk(chunk_hash: str, data: bytes, targets) -> None:
    stored = []
    for node in targets:
        try:
            requests.put(_url(node, chunk_hash), data=data, timeout=30).raise_for_status()
            stored.append(node)
        except requests.RequestException as exc:
            log.warning("put %s on %s failed: %s", chunk_hash[:12], node, exc)
    if len(stored) < min(WRITE_QUORUM, len(targets)):
        _delete_chunk(chunk_hash, stored)
        raise HTTPException(503, f"Write quorum not reached for chunk {chunk_hash[:12]}")


def _read_chunk(chunk_hash: str, from_ring: HashRing, quorum: int = READ_QUORUM) -> bytes:
    """Return a chunk only after `quorum` nodes supply a copy with the correct hash."""
    targets = from_ring.get_nodes(chunk_hash, REPLICAS)
    data = None
    valid_copies = 0
    for node in targets + [n for n in from_ring.nodes if n not in targets]:
        try:
            r = requests.get(_url(node, chunk_hash), timeout=10)
            if r.status_code == 200 and sha256_hex(r.content) == chunk_hash:
                data = r.content
                valid_copies += 1
                if valid_copies >= min(quorum, len(targets)):
                    return data
        except requests.RequestException:
            continue
    raise HTTPException(503, f"Read quorum not reached for chunk {chunk_hash[:12]}")


# -------------------------------------------------------- erasure coding
def _fragment_node(targets: list, index: int) -> str:
    return targets[index % len(targets)]


def _write_fragments(chunk_hash: str, data: bytes, k: int, m: int, from_ring: HashRing) -> list:
    """Encode a chunk and store each fragment on a different node where possible."""
    targets = from_ring.get_nodes(chunk_hash, k + m)
    if len(targets) < k + 1:
        raise HTTPException(503, f"Erasure coding {k}+{m} needs {k + 1} nodes, found {len(targets)}")

    fragments = erasure.encode(data, k, m)
    fragment_hashes = [sha256_hex(f) for f in fragments]
    stored = 0
    for index, (fragment_hash, fragment) in enumerate(zip(fragment_hashes, fragments)):
        node = _fragment_node(targets, index)
        try:
            requests.put(_url(node, fragment_hash), data=fragment, timeout=30).raise_for_status()
            stored += 1
        except requests.RequestException as exc:
            log.warning("put fragment %s on %s failed: %s", fragment_hash[:12], node, exc)
    if stored < k + 1:          # one spare fragment, so the chunk survives another failure
        _delete_fragments(fragment_hashes, from_ring)
        raise HTTPException(503, f"Write quorum not reached for chunk {chunk_hash[:12]}")
    return fragment_hashes

def _delete_fragments(fragment_hashes, from_ring: HashRing) -> None:
    for fragment_hash in fragment_hashes:
        _delete_chunk(fragment_hash, from_ring.nodes)


def _read_fragment(fragment_hash: str, preferred: str, from_ring: HashRing):
    for node in [preferred] + [n for n in from_ring.nodes if n != preferred]:
        try:
            r = requests.get(_url(node, fragment_hash), timeout=10)
            if r.status_code == 200 and sha256_hex(r.content) == fragment_hash:
                return r.content
        except requests.RequestException:
            continue
    return None


def _read_erasure_chunk(chunk: dict, from_ring: HashRing) -> bytes:
    """Collect any k valid fragments and rebuild the chunk."""
    k, m = chunk["k"], chunk["m"]
    targets = from_ring.get_nodes(chunk["hash"], k + m)
    fragments = {}
    for index, fragment_hash in enumerate(chunk["fragment_hashes"]):
        fragment = _read_fragment(fragment_hash, _fragment_node(targets, index), from_ring)
        if fragment is not None:
            fragments[index] = fragment
        if len(fragments) == k:
            data = erasure.decode(fragments, k, m, chunk["size_bytes"])
            if sha256_hex(data) == chunk["hash"]:
                return data
            break
    raise HTTPException(503, f"Not enough fragments to rebuild chunk {chunk['hash'][:12]}")


def _read_stored_chunk(chunk: dict, from_ring: HashRing) -> bytes:
    if chunk["mode"] == "erasure":
        return _read_erasure_chunk(chunk, from_ring)
    return _read_chunk(chunk["hash"], from_ring)


# ----------------------------------------------------------------- buckets
@router.post("/buckets/{bucket}", status_code=201)
def create_bucket(bucket: str, mode: str = "replication",
                  k: int = DEFAULT_EC_K, m: int = DEFAULT_EC_M):
    if mode not in ("replication", "erasure"):
        raise HTTPException(400, "mode must be replication or erasure")
    if mode == "erasure" and (k < 1 or m < 1):
        raise HTTPException(400, "k and m must be at least 1")
    with store.transaction() as cur:
        created = store.create_bucket(
            cur, bucket, mode, k if mode == "erasure" else None, m if mode == "erasure" else None
        )
        if not created:
            raise HTTPException(409, "Bucket already exists")
        return store.get_bucket(cur, bucket)


@router.get("/buckets")
def list_buckets():
    with store.transaction() as cur:
        return store.list_buckets(cur)


# ----------------------------------------------------------------- objects
def _store_object(bucket: str, key: str, data: bytes, chunker: str) -> dict:
    if chunker not in CHUNKERS:
        raise HTTPException(400, f"chunker must be one of {sorted(CHUNKERS)}")
    current = ring
    if not current.nodes:
        raise HTTPException(503, "No storage nodes in the ring")

    chunks = list(CHUNKERS[chunker](data))
    hashes = [sha256_hex(c) for c in chunks]
    by_hash = dict(zip(hashes, chunks))
    counts = Counter(hashes)
    sizes = {h: len(c) for h, c in by_hash.items()}

    uploaded = []
    with store.transaction() as cur:
        store.lock_object(cur, bucket, key)
        store.create_bucket(cur, bucket, "replication", None, None)   # default for a new bucket
        settings = store.get_bucket(cur, bucket)
        new = store.add_chunk_refs(cur, sizes, counts)   # only unseen chunks need uploading
        try:
            for h in sorted(new):
                if settings["mode"] == "erasure":
                    fragment_hashes = _write_fragments(h, by_hash[h], settings["k"], settings["m"], current)
                    uploaded.append((h, fragment_hashes))
                    store.set_chunk_fragments(cur, h, settings["k"], settings["m"], fragment_hashes)
                else:
                    _write_chunk(h, by_hash[h], current.get_nodes(h, REPLICAS))
                    uploaded.append((h, None))
            version = store.next_version(cur, bucket, key)
            created = store.insert_object(cur, bucket, key, version, len(data), chunker, hashes)
        except Exception:
            for h, fragment_hashes in uploaded:           # new chunks, so safe to remove
                if fragment_hashes:
                    _delete_fragments(fragment_hashes, current)
                else:
                    _delete_chunk(h, current.get_nodes(h, REPLICAS))
            raise

    return {
        "bucket": bucket, "key": key, "version": version, "size_bytes": len(data),
        "chunker": chunker, "mode": settings["mode"], "created_at": created,
        "chunks_total": len(hashes), "chunks_uploaded": len(new),
        "chunks_deduplicated": len(hashes) - len(new),
    }


def _delete_object(bucket: str, key: str, version: Optional[int]) -> dict:
    with store.transaction() as cur:
        store.lock_object(cur, bucket, key)
        row = store.delete_object(cur, bucket, key, version)
        if row is None:
            raise HTTPException(404, "Object not found")
        chunks = store.get_chunks(cur, row["chunk_hashes"])
        orphaned = store.release_chunk_refs(cur, Counter(row["chunk_hashes"]))
        # still inside the transaction, so a racing writer waits on the chunk row lock
        for h in orphaned:
            if chunks[h]["mode"] == "erasure":
                _delete_fragments(chunks[h]["fragment_hashes"], ring)
            else:
                _delete_chunk(h, ring.nodes)
    return {"deleted_version": row["version"], "chunks_removed_from_nodes": len(orphaned)}


@router.put("/objects/{bucket}/{key:path}")
async def put_object(bucket: str, key: str, request: Request, chunker: str = "fixed"):
    data = await request.body()
    return await run_in_threadpool(_store_object, bucket, key, data, chunker)


@router.get("/objects/{bucket}/{key:path}")
def get_object(bucket: str, key: str, version: Optional[int] = None):
    with store.transaction() as cur:
        row = store.get_object(cur, bucket, key, version)
        if row is None:
            raise HTTPException(404, "Object not found")
        chunks = store.get_chunks(cur, row["chunk_hashes"])
    current = ring
    body = b"".join(_read_stored_chunk(chunks[h], current) for h in row["chunk_hashes"])
    return Response(content=body, media_type="application/octet-stream",
                    headers={"X-Object-Version": str(row["version"])})


@router.delete("/objects/{bucket}/{key:path}")
def delete_object(bucket: str, key: str, version: Optional[int] = None):
    return _delete_object(bucket, key, version)


@router.get("/metadata/{bucket}/{key:path}")
def object_metadata(bucket: str, key: str, version: Optional[int] = None):
    with store.transaction() as cur:
        row = store.get_object(cur, bucket, key, version)
    if row is None:
        raise HTTPException(404, "Object not found")
    return dict(row)


@router.get("/stats")
def dedup_stats():
    with store.transaction() as cur:
        return store.stats(cur)


# -------------------------------------------------------------------- ring
@router.get("/ring")
def ring_info():
    return {
        "nodes": ring.nodes, "vnodes": ring.vnodes, "replicas": REPLICAS,
        "write_quorum": WRITE_QUORUM, "read_quorum": READ_QUORUM,
    }


@router.get("/placement/{chunk_hash}")
def placement(chunk_hash: str):
    return {"chunk": chunk_hash, "nodes": ring.get_nodes(chunk_hash, REPLICAS)}


def _change_ring(node_id: str, add: bool, dry_run: bool) -> dict:
    """Add/remove a node, move the affected chunks, and report how many moved."""
    global ring
    with ring_lock:
        old = ring
        new = old.copy()
        if add:
            if node_id in old.nodes:
                raise HTTPException(409, "Node already in ring")
            new.add_node(node_id)
        else:
            if node_id not in old.nodes:
                raise HTTPException(404, "Node not in ring")
            new.remove_node(node_id)

        with store.transaction() as cur:
            all_chunks = store.all_chunks(cur)

        moved = copies = failures = 0
        for chunk in all_chunks:
            h = chunk["hash"]
            if chunk["mode"] == "erasure":
                count = chunk["k"] + chunk["m"]
                before, after = old.get_nodes(h, count), new.get_nodes(h, count)
                changes = [
                    (fragment_hash, _fragment_node(before, i), _fragment_node(after, i))
                    for i, fragment_hash in enumerate(chunk["fragment_hashes"])
                    if _fragment_node(before, i) != _fragment_node(after, i)
                ]
                if not changes:
                    continue
                moved += 1
                copies += len(changes)
                if dry_run:
                    continue
                for fragment_hash, source, target in changes:
                    try:
                        data = _read_fragment(fragment_hash, source, old)
                        if data is None:
                            raise HTTPException(503, "Fragment unavailable")
                        requests.put(_url(target, fragment_hash), data=data, timeout=30).raise_for_status()
                        _delete_chunk(fragment_hash, [source])
                    except (HTTPException, requests.RequestException) as exc:
                        failures += 1
                        log.warning("migration of fragment %s failed: %s", fragment_hash[:12], exc)
                continue
            before, after = old.get_nodes(h, REPLICAS), new.get_nodes(h, REPLICAS)
            if set(before) == set(after):
                continue
            to_add = [n for n in after if n not in before]
            to_remove = [n for n in before if n not in after]
            moved += 1
            copies += len(to_add)
            if dry_run:
                continue
            try:
                data = _read_chunk(h, old, quorum=1) if to_add else None
                for node in to_add:
                    requests.put(_url(node, h), data=data, timeout=30).raise_for_status()
                _delete_chunk(h, to_remove)
            except (HTTPException, requests.RequestException) as exc:
                failures += 1
                log.warning("migration of %s failed: %s", h[:12], exc)

        if not dry_run:
            ring = new
        total = len(all_chunks)
        return {
            "action": "add" if add else "remove", "node": node_id, "dry_run": dry_run,
            "total_chunks": total, "chunks_moved": moved,
            "chunks_moved_pct": round(100 * moved / total, 2) if total else 0.0,
            "replica_copies_moved": copies, "migration_failures": failures,
            "nodes": new.nodes if not dry_run else old.nodes,
        }


@router.post("/ring/nodes/{node_id}")
def add_ring_node(node_id: str, dry_run: bool = False):
    return _change_ring(node_id, add=True, dry_run=dry_run)


@router.delete("/ring/nodes/{node_id}")
def remove_ring_node(node_id: str, dry_run: bool = False):
    return _change_ring(node_id, add=False, dry_run=dry_run)