"""Metadata service (Step 1): receives heartbeats and tracks storage node health."""
import logging
import os
import time

from fastapi import FastAPI
from pydantic import BaseModel

HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "15"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("metadata")

app = FastAPI(title="Metadata Service")
nodes: dict[str, dict] = {}


class Heartbeat(BaseModel):
    node_id: str
    url: str
    free_space_bytes: int
    chunk_count: int


def node_status(node: dict) -> str:
    return "alive" if time.time() - node["last_heartbeat"] <= HEARTBEAT_TIMEOUT else "failed"


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