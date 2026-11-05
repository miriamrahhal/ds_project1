"""Fixed-size vs FastCDC: how much dedup survives a small insertion?
Sizes are scaled down (64 KB average) so the pure-Python chunker runs fast."""
import random
import time

from metadata_service.chunking import fastcdc_chunks, fixed_size_chunks, sha256_hex

KB = 1024
rng = random.Random(7)
original = rng.randbytes(4 * 1024 * KB)
edited = original[:1000] + rng.randbytes(100) + original[1000:]     # insert 100 bytes near the start

chunkers = {
    "fixed 64KB": lambda d: fixed_size_chunks(d, 64 * KB),
    "FastCDC (16/64/256KB)": lambda d: fastcdc_chunks(d, 16 * KB, 64 * KB, 256 * KB),
}

print(f"{'chunker':24} {'chunks':>7} {'avg KB':>7} {'shared after edit':>18} {'time s':>7}")
for name, fn in chunkers.items():
    t = time.time()
    a = [c for c in fn(original)]
    b = [c for c in fn(edited)]
    elapsed = time.time() - t
    ha, hb = {sha256_hex(c) for c in a}, {sha256_hex(c) for c in b}
    shared = 100 * len(ha & hb) / len(hb)
    print(f"{name:24} {len(a):7d} {sum(map(len, a)) / len(a) / KB:7.1f} {shared:17.1f}% {elapsed:7.2f}")
    assert b"".join(a) == original and b"".join(b) == edited