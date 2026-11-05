"""Background scrubber: re-reads stored data, detects bit rot and repairs it."""
import logging
import os
import threading
import time
from datetime import datetime, timezone

import requests

from metadata_service import metadata_store as store, placement_api as api, repair
from metadata_service.chunking import sha256_hex

SCRUB_INTERVAL = float(os.getenv("SCRUB_INTERVAL", "300"))

log = logging.getLogger("scrubber")
jobs: list[dict] = []
scrub_lock = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _remove_corrupted_copies(stored_hash: str, nodes: list, job: dict) -> int:
    """Read every copy, verify its hash and delete any copy that is corrupted."""
    corrupted = 0
    for node in repair._holders(stored_hash, nodes):
        try:
            r = requests.get(api._url(node, stored_hash), timeout=30)
        except requests.RequestException as exc:
            log.warning("scrub read of %s on %s failed: %s", stored_hash[:12], node, exc)
            continue
        job["bytes_scanned"] += len(r.content) if r.status_code == 200 else 0
        if r.status_code == 500 or (r.status_code == 200 and sha256_hex(r.content) != stored_hash):
            log.warning("Bit rot detected: %s on %s", stored_hash[:12], node)
            job["corrupted_copies_found"] += 1
            job["corrupted"].append({"hash": stored_hash[:12], "node": node})
            corrupted += 1
            requests.delete(api._url(node, stored_hash), timeout=5)
    return corrupted


def run_scrub() -> dict:
    """Check every chunk or fragment on every node and restore corrupted data."""
    job = {
        "id": len(jobs) + 1, "status": "running", "started_at": _now(), "finished_at": None,
        "chunks_checked": 0, "chunks_total": 0, "bytes_scanned": 0,
        "corrupted_copies_found": 0, "corrupted": [], "chunks_repaired": 0, "copies_rebuilt": 0,
        "replication_bytes_read": 0, "replication_bytes_written": 0,
        "erasure_bytes_read": 0, "erasure_bytes_written": 0, "failures": 0,
    }
    jobs.append(job)
    with scrub_lock:
        ring = api.ring
        with store.transaction() as cur:
            chunks = store.all_chunks(cur)
        job["chunks_total"] = len(chunks)
        for chunk in chunks:
            try:
                if chunk["mode"] == "erasure":
                    corrupted = sum(
                        _remove_corrupted_copies(h, ring.nodes, job) for h in chunk["fragment_hashes"]
                    )
                    if corrupted:
                        repair._repair_erasure(chunk, ring, job)
                else:
                    if _remove_corrupted_copies(chunk["hash"], ring.nodes, job):
                        repair._repair_replicated(chunk, ring, job)
            except Exception as exc:
                job["failures"] += 1
                log.warning("scrub of chunk %s failed: %s", chunk["hash"][:12], exc)
            job["chunks_checked"] += 1
    job["status"] = "completed" if not job["failures"] else "completed with failures"
    job["finished_at"] = _now()
    log.info("Scrub job %s finished: found %s corrupted, rebuilt %s",
             job["id"], job["corrupted_copies_found"], job["copies_rebuilt"])
    return job


def scrub_loop() -> None:
    while True:
        time.sleep(SCRUB_INTERVAL)
        try:
            run_scrub()
        except Exception:
            log.exception("Scrubber error")