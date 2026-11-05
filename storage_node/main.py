"""Storage node: stores chunks on local disk, addressed by their SHA-256 hash."""
import asyncio
import hashlib
import logging
import os
import re
import shutil
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response

NODE_ID = os.getenv("NODE_ID", "node-local")
NODE_URL = os.getenv("NODE_URL", "http://localhost:8001")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
METADATA_URL = os.getenv("METADATA_URL", "http://localhost:8000")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "5"))

CHUNKS_DIR = DATA_DIR / "chunks"
HASH_PATTERN = re.compile(r"^[a-f0-9]{64}$")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(NODE_ID)


def validate_hash(chunk_hash: str) -> None:
    """Accept only a SHA-256 hex string. This also blocks path traversal."""
    if not HASH_PATTERN.fullmatch(chunk_hash):
        raise HTTPException(status_code=400, detail="Chunk ID must be a lowercase SHA-256 hash")


def chunk_path(chunk_hash: str) -> Path:
    # The first two characters are a subfolder, so one folder never holds every chunk.
    return CHUNKS_DIR / chunk_hash[:2] / chunk_hash


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def count_chunks() -> int:
    if not CHUNKS_DIR.exists():
        return 0
    return sum(1 for path in CHUNKS_DIR.glob("*/*") if path.is_file() and not path.name.endswith(".tmp"))


def free_space() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return shutil.disk_usage(DATA_DIR).free


async def send_heartbeats() -> None:
    """Report this node's state to the metadata service every few seconds."""
    async with httpx.AsyncClient(timeout=3) as client:
        while True:
            payload = {
                "node_id": NODE_ID,
                "url": NODE_URL,
                "free_space_bytes": free_space(),
                "chunk_count": count_chunks(),
            }
            try:
                response = await client.post(f"{METADATA_URL}/heartbeat", json=payload)
                response.raise_for_status()
                logger.info(
                    "Heartbeat sent: free_space_bytes=%s chunk_count=%s",
                    payload["free_space_bytes"],
                    payload["chunk_count"],
                )
            except httpx.HTTPError as error:
                # The node keeps serving chunks if the metadata service is down.
                logger.warning("Heartbeat failed: %s", error)
            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(_: FastAPI):
    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
    task = None
    if HEARTBEAT_INTERVAL > 0:
        task = asyncio.create_task(send_heartbeats())
    yield
    if task:
        task.cancel()


app = FastAPI(title=f"Storage Node {NODE_ID}", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "node_id": NODE_ID, "chunk_count": count_chunks()}


@app.put("/chunks/{chunk_hash}", status_code=201)
async def put_chunk(chunk_hash: str, request: Request, response: Response) -> dict:
    validate_hash(chunk_hash)
    data = await request.body()

    actual_hash = sha256(data)
    if actual_hash != chunk_hash:
        raise HTTPException(
            status_code=400,
            detail=f"Hash mismatch: URL has {chunk_hash}, content is {actual_hash}",
        )

    path = chunk_path(chunk_hash)
    if path.exists():
        response.status_code = 200
        return {"hash": chunk_hash, "size": len(data), "node_id": NODE_ID, "already_stored": True}

    path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temporary file first, so a crash never leaves a partial chunk.
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as temporary_file:
        temporary_file.write(data)
        temporary_file.flush()
        os.fsync(temporary_file.fileno())
        temporary_name = temporary_file.name
    os.replace(temporary_name, path)

    return {"hash": chunk_hash, "size": len(data), "node_id": NODE_ID, "already_stored": False}


@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str) -> Response:
    validate_hash(chunk_hash)
    path = chunk_path(chunk_hash)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")

    data = path.read_bytes()
    actual_hash = sha256(data)
    if actual_hash != chunk_hash:
        logger.error("Corrupted chunk rejected: %s (content hash %s)", chunk_hash, actual_hash)
        raise HTTPException(
            status_code=500,
            detail=f"Corrupted chunk rejected: expected {chunk_hash}, got {actual_hash}",
        )
    return Response(content=data, media_type="application/octet-stream")


@app.head("/chunks/{chunk_hash}")
def head_chunk(chunk_hash: str) -> Response:
    validate_hash(chunk_hash)
    path = chunk_path(chunk_hash)
    if not path.is_file():
        return Response(status_code=404)
    return Response(status_code=200, headers={"Content-Length": str(path.stat().st_size)})


@app.delete("/chunks/{chunk_hash}", status_code=204)
def delete_chunk(chunk_hash: str) -> Response:
    validate_hash(chunk_hash)
    path = chunk_path(chunk_hash)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")
    path.unlink()
    return Response(status_code=204)