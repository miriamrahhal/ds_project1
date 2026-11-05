"""Compare replication and erasure coding on the small-file and large-file workloads."""
import argparse
import csv
import hashlib
import json
import random
import subprocess
import time
from datetime import datetime
from pathlib import Path

import requests

BASE = "http://localhost:8000"
RESULTS = Path("benchmarks/results")
POSTGRES = "ds-postgres"
KB = 1024
MB = 1024 * 1024


def docker(*args: str) -> str:
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True).stdout


def ring_nodes() -> list:
    return requests.get(f"{BASE}/ring", timeout=10).json()["nodes"]


def alive_nodes() -> list:
    return [n["node_id"] for n in requests.get(f"{BASE}/nodes", timeout=10).json() if n["status"] == "alive"]


def wait_for_nodes(nodes: list) -> None:
    for _ in range(120):
        if set(nodes) <= set(ring_nodes()) and set(nodes) <= set(alive_nodes()):
            return
        time.sleep(2)
    raise TimeoutError("Nodes did not return")


def reset(nodes: list) -> None:
    docker("exec", POSTGRES, "psql", "-U", "dsuser", "-d", "dsmetadata",
           "-c", "TRUNCATE objects, chunks, buckets;")
    for node in nodes:
        docker("exec", node, "sh", "-c", "rm -rf /data/chunks/*")


def stored_bytes(nodes: list) -> tuple:
    """Return (file bytes, disk bytes) across all nodes. Disk bytes include block rounding."""
    file_bytes = disk_bytes = 0
    for node in nodes:
        out = docker("exec", node, "sh", "-c",
                     "find /data/chunks -type f -printf '%s %b\\n'")
        for line in out.splitlines():
            size, blocks = line.split()
            file_bytes += int(size)
            disk_bytes += int(blocks) * 512
    return file_bytes, disk_bytes


def small_files(count: int):
    rng = random.Random(1)
    for i in range(count):
        size = rng.randint(1 * KB, 50 * KB)
        yield f"small/{i:05d}.json", rng.randbytes(size)


def large_files(count: int, size_mb: int):
    rng = random.Random(2)
    for i in range(count):
        yield f"large/{i:02d}.bin", rng.randbytes(size_mb * MB)


def run_case(workload: str, files, mode: str, nodes: list) -> dict:
    print(f"\n=== {workload} / {mode} ===", flush=True)
    wait_for_nodes(nodes)
    reset(nodes)
    bucket = f"bench-{mode}"
    url = f"{BASE}/buckets/{bucket}?mode={mode}" + ("&k=4&m=2" if mode == "erasure" else "")
    requests.post(url, timeout=30).raise_for_status()

    session = requests.Session()
    logical = count = 0
    checks = []
    started = time.time()
    for key, data in files():
        session.put(f"{BASE}/objects/{bucket}/{key}", data=data, timeout=600).raise_for_status()
        logical += len(data)
        count += 1
        if count <= 5:
            checks.append((key, hashlib.sha256(data).hexdigest()))
        if count % 500 == 0:
            print(f"  uploaded {count} files", flush=True)
    upload_seconds = time.time() - started
    print(f"  uploaded {count} files, {logical / MB:.1f} MB, in {upload_seconds:.1f}s", flush=True)

    file_bytes, disk_bytes = stored_bytes(nodes)
    jobs_before = len(requests.get(f"{BASE}/repair/jobs", timeout=30).json())

    failed = nodes[0]
    container = failed
    print(f"  stopping {container} and waiting for the repair job...", flush=True)
    docker("stop", container)
    job = None
    for _ in range(3600):
        jobs = requests.get(f"{BASE}/repair/jobs", timeout=30).json()
        if len(jobs) > jobs_before and jobs[-1]["status"] != "running":
            job = jobs[-1]
            break
        time.sleep(2)
    if job is None:
        raise TimeoutError("Repair job did not finish")

    verified = all(
        hashlib.sha256(session.get(f"{BASE}/objects/{bucket}/{key}", timeout=600).content).hexdigest() == expected
        for key, expected in checks
    )
    docker("start", container)
    wait_for_nodes(nodes)

    read = job["replication_bytes_read"] + job["erasure_bytes_read"]
    written = job["replication_bytes_written"] + job["erasure_bytes_written"]
    seconds = (datetime.fromisoformat(job["finished_at"]) - datetime.fromisoformat(job["started_at"])).total_seconds()
    result = {
        "workload": workload, "mode": mode, "files": count,
        "logical_mb": round(logical / MB, 1),
        "stored_mb": round(file_bytes / MB, 1),
        "disk_mb": round(disk_bytes / MB, 1),
        "file_overhead": round(file_bytes / logical, 2),
        "disk_overhead": round(disk_bytes / logical, 2),
        "upload_seconds": round(upload_seconds, 1),
        "repair_read_mb": round(read / MB, 1),
        "repair_written_mb": round(written / MB, 1),
        "repair_traffic_mb": round((read + written) / MB, 1),
        "repair_seconds": round(seconds, 1),
        "repair_failures": job["failures"],
        "files_verified_after_repair": verified,
    }
    print(json.dumps(result), flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--small-files", type=int, default=5000)
    parser.add_argument("--large-files", type=int, default=10)
    parser.add_argument("--large-mb", type=int, default=100)
    args = parser.parse_args()

    nodes = ring_nodes()
    if len(nodes) < 6:
        raise SystemExit(f"Expected 6 nodes in the ring, found {len(nodes)}: {nodes}")
    workloads = [
        ("A: small files", lambda: small_files(args.small_files)),
        ("B: large files", lambda: large_files(args.large_files, args.large_mb)),
    ]
    results = [
        run_case(name, files, mode, nodes)
        for name, files in workloads
        for mode in ("replication", "erasure")
    ]
    reset(nodes)

    RESULTS.mkdir(parents=True, exist_ok=True)
    with open(RESULTS / "redundancy_comparison.csv", "w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    columns = ["workload", "mode", "logical_mb", "stored_mb", "disk_mb", "file_overhead",
               "disk_overhead", "repair_read_mb", "repair_written_mb", "repair_traffic_mb",
               "repair_seconds", "repair_failures", "files_verified_after_repair"]
    print("\nRESULTS")
    print(" | ".join(columns))
    for row in results:
        print(" | ".join(str(row[c]) for c in columns))


if __name__ == "__main__":
    main()