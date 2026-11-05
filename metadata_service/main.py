"""Metadata service: tracks node health, stores metadata and starts repair jobs."""
import logging
import os
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from metadata_service import metadata_store, repair, scrubber
from metadata_service.placement_api import router as placement_router

HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "15"))
MONITOR_INTERVAL = float(os.getenv("MONITOR_INTERVAL", "5"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("metadata")

nodes: dict[str, dict] = {}
failed_nodes: set[str] = set()


def node_status(node: dict) -> str:
    return "alive" if time.time() - node["last_heartbeat"] <= HEARTBEAT_TIMEOUT else "failed"


def monitor_nodes() -> None:
    """Detect nodes that stop or resume sending heartbeats."""
    last_check = time.time()
    while True:
        time.sleep(MONITOR_INTERVAL)
        now = time.time()
        paused_for = now - last_check
        last_check = now
        if paused_for > HEARTBEAT_TIMEOUT:
            # The computer slept or this service was paused, so missing heartbeats prove nothing.
            logger.warning("Monitor was paused for %.0fs. Giving nodes time to report.", paused_for)
            for node in list(nodes.values()):
                node["last_heartbeat"] = now
            continue
        try:
            for node_id in list(nodes):
                status = node_status(nodes[node_id])    # read fresh: a repair can take a while
                if status == "failed" and node_id not in failed_nodes:
                    failed_nodes.add(node_id)
                    threading.Thread(
                        target=repair.handle_node_failure, args=(node_id,), daemon=True
                    ).start()
                elif status == "alive" and node_id in failed_nodes:
                    failed_nodes.discard(node_id)
                    repair.handle_node_recovery(node_id)
        except Exception:
            logger.exception("Node monitor error")


@asynccontextmanager
async def lifespan(_: FastAPI):
    metadata_store.init_schema()
    threading.Thread(target=monitor_nodes, name="node-monitor", daemon=True).start()
    threading.Thread(target=scrubber.scrub_loop, name="scrubber", daemon=True).start()
    yield


app = FastAPI(title="Metadata Service", lifespan=lifespan)
app.include_router(placement_router)


class Heartbeat(BaseModel):
    node_id: str
    url: str
    free_space_bytes: int
    chunk_count: int


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/heartbeat")
def heartbeat(beat: Heartbeat) -> dict:
    nodes[beat.node_id] = {**beat.model_dump(), "last_heartbeat": time.time()}
    logger.info(
        "Heartbeat from %s: free_space_bytes=%s chunk_count=%s",
        beat.node_id,
        beat.free_space_bytes,
        beat.chunk_count,
    )
    return {"received": True}


@app.get("/nodes")
def list_nodes() -> list[dict]:
    return [
        {
            **node,
            "status": node_status(node),
            "seconds_since_heartbeat": round(time.time() - node["last_heartbeat"], 1),
        }
        for node in sorted(nodes.values(), key=lambda item: item["node_id"])
    ]


@app.get("/repair/jobs")
def repair_jobs() -> list[dict]:
    return repair.jobs


@app.post("/scrub/run")
def scrub_now() -> dict:
    return scrubber.run_scrub()


@app.get("/scrub/jobs")
def scrub_jobs() -> list[dict]:
    return scrubber.jobs