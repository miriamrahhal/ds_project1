"""PostgreSQL metadata store: objects (versioned, ordered chunk lists) + chunk reference counts."""
import os
import time
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://dsuser:dsdevpassword@postgres:5432/dsmetadata"
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS chunks (
    hash        CHAR(64) PRIMARY KEY,
    size_bytes  BIGINT NOT NULL,
    ref_count   INTEGER NOT NULL CHECK (ref_count >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS objects (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    version       INTEGER NOT NULL,
    size_bytes    BIGINT NOT NULL,
    chunking      TEXT NOT NULL,
    chunk_hashes  TEXT[] NOT NULL,          -- ordered
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket, key, version)
);
CREATE TABLE IF NOT EXISTS buckets (
    name        TEXT PRIMARY KEY,
    mode        TEXT NOT NULL DEFAULT 'replication',
    k           INTEGER,
    m           INTEGER,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'replication';
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS k INTEGER;
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS m INTEGER;
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS fragment_hashes TEXT[];
"""

_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(1, 10, DATABASE_URL)
    return _pool


def init_schema(retries: int = 30):
    for attempt in range(retries):
        try:
            with transaction() as cur:
                cur.execute(SCHEMA)
            return
        except psycopg2.OperationalError:
            if attempt == retries - 1:
                raise
            time.sleep(1)


@contextmanager
def transaction():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def lock_object(cur, bucket, key):
    """Serialize concurrent writers/deleters of the same bucket/key."""
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{bucket}/{key}",))


def add_chunk_refs(cur, sizes: dict, counts: dict) -> set:
    """Add references. Returns the hashes that did not exist yet (must be uploaded)."""
    new = set()
    for h in sorted(counts):          # fixed order avoids deadlocks between writers
        cur.execute(
            """INSERT INTO chunks (hash, size_bytes, ref_count) VALUES (%s, %s, %s)
               ON CONFLICT (hash) DO UPDATE SET ref_count = chunks.ref_count + EXCLUDED.ref_count
               RETURNING (xmax = 0) AS inserted""",
            (h, sizes[h], counts[h]),
        )
        if cur.fetchone()["inserted"]:
            new.add(h)
    return new


def release_chunk_refs(cur, counts: dict) -> list:
    """Drop references. Returns hashes whose count reached 0 (rows are deleted)."""
    orphaned = []
    for h in sorted(counts):
        cur.execute(
            "UPDATE chunks SET ref_count = ref_count - %s WHERE hash = %s RETURNING ref_count",
            (counts[h], h),
        )
        row = cur.fetchone()
        if row and row["ref_count"] <= 0:
            cur.execute("DELETE FROM chunks WHERE hash = %s", (h,))
            orphaned.append(h)
    return orphaned


def next_version(cur, bucket, key) -> int:
    cur.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 AS v FROM objects WHERE bucket=%s AND key=%s",
        (bucket, key),
    )
    return cur.fetchone()["v"]


def insert_object(cur, bucket, key, version, size, chunking, chunk_hashes):
    cur.execute(
        """INSERT INTO objects (bucket, key, version, size_bytes, chunking, chunk_hashes)
           VALUES (%s, %s, %s, %s, %s, %s) RETURNING created_at""",
        (bucket, key, version, size, chunking, chunk_hashes),
    )
    return cur.fetchone()["created_at"]


def get_object(cur, bucket, key, version=None):
    if version is None:
        cur.execute(
            "SELECT * FROM objects WHERE bucket=%s AND key=%s ORDER BY version DESC LIMIT 1",
            (bucket, key),
        )
    else:
        cur.execute(
            "SELECT * FROM objects WHERE bucket=%s AND key=%s AND version=%s",
            (bucket, key, version),
        )
    return cur.fetchone()


def delete_object(cur, bucket, key, version=None):
    row = get_object(cur, bucket, key, version)
    if row:
        cur.execute(
            "DELETE FROM objects WHERE bucket=%s AND key=%s AND version=%s",
            (bucket, key, row["version"]),
        )
    return row


def all_chunks(cur) -> list:
    cur.execute("SELECT * FROM chunks")
    return cur.fetchall()


def get_chunks(cur, hashes) -> dict:
    cur.execute("SELECT * FROM chunks WHERE hash = ANY(%s)", (list(hashes),))
    return {r["hash"]: r for r in cur.fetchall()}


def set_chunk_fragments(cur, chunk_hash, k, m, fragment_hashes):
    cur.execute(
        "UPDATE chunks SET mode='erasure', k=%s, m=%s, fragment_hashes=%s WHERE hash=%s",
        (k, m, fragment_hashes, chunk_hash),
    )


def create_bucket(cur, name, mode, k, m) -> bool:
    cur.execute(
        """INSERT INTO buckets (name, mode, k, m) VALUES (%s, %s, %s, %s)
           ON CONFLICT (name) DO NOTHING RETURNING name""",
        (name, mode, k, m),
    )
    return cur.fetchone() is not None


def get_bucket(cur, name):
    cur.execute("SELECT * FROM buckets WHERE name=%s", (name,))
    return cur.fetchone()


def list_buckets(cur) -> list:
    cur.execute("SELECT * FROM buckets ORDER BY name")
    return cur.fetchall()


def stats(cur) -> dict:
    cur.execute("SELECT COUNT(*) AS n, COALESCE(SUM(size_bytes), 0) AS b FROM objects")
    obj = cur.fetchone()
    cur.execute("SELECT COUNT(*) AS n, COALESCE(SUM(size_bytes), 0) AS b FROM chunks")
    chk = cur.fetchone()
    logical, physical = int(obj["b"]), int(chk["b"])
    return {
        "object_versions": obj["n"],
        "unique_chunks": chk["n"],
        "logical_bytes": logical,
        "unique_bytes": physical,
        "dedup_ratio": round(logical / physical, 3) if physical else None,
    }