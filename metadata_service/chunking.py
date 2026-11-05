"""Chunking strategies: fixed-size and FastCDC (content-defined)."""
import hashlib
import random
from typing import Iterator

FIXED_CHUNK_SIZE = 4 * 1024 * 1024
CDC_MIN = 1 * 1024 * 1024
CDC_AVG = 4 * 1024 * 1024
CDC_MAX = 8 * 1024 * 1024


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fixed_size_chunks(data: bytes, chunk_size: int = FIXED_CHUNK_SIZE) -> Iterator[bytes]:
    """Cut every `chunk_size` bytes. Simple, but one inserted byte shifts every later chunk."""
    for start in range(0, len(data), chunk_size):
        yield data[start:start + chunk_size]


# --- FastCDC (gear rolling hash + normalized chunking) ---------------------
_rng = random.Random(0x5EED)          # fixed seed: same table on every node/run
GEAR = [_rng.getrandbits(64) for _ in range(256)]
_MASK64 = (1 << 64) - 1


def _mask(bits: int) -> int:
    """`bits` one-bits placed at the top of a 64-bit word."""
    return ((1 << bits) - 1) << (64 - bits)


def _cut_point(data: bytes, start: int, min_size: int, avg_size: int,
               max_size: int, mask_s: int, mask_l: int) -> int:
    remaining = len(data) - start
    if remaining <= min_size:
        return remaining

    normal = min(avg_size, remaining)
    limit = min(max_size, remaining)
    gear = GEAR
    fp = 0
    i = min_size                       # skip cut-point search below min_size

    while i < normal:                  # strict mask: fewer cuts before the average
        fp = ((fp << 1) + gear[data[start + i]]) & _MASK64
        i += 1
        if not fp & mask_s:
            return i
    while i < limit:                   # loose mask: more cuts after the average
        fp = ((fp << 1) + gear[data[start + i]]) & _MASK64
        i += 1
        if not fp & mask_l:
            return i
    return limit


def fastcdc_chunks(data: bytes, min_size: int = CDC_MIN, avg_size: int = CDC_AVG,
                   max_size: int = CDC_MAX) -> Iterator[bytes]:
    """Content-defined chunking: boundaries depend on the bytes, not on offsets,
    so an insertion only changes the chunks around it."""
    if not 0 < min_size <= avg_size <= max_size:
        raise ValueError("need 0 < min_size <= avg_size <= max_size")

    bits = max(avg_size.bit_length() - 1, 3)      # log2(avg)
    mask_s, mask_l = _mask(bits + 2), _mask(bits - 2)

    start = 0
    while start < len(data):
        length = _cut_point(data, start, min_size, avg_size, max_size, mask_s, mask_l)
        yield data[start:start + length]
        start += length


CHUNKERS = {"fixed": fixed_size_chunks, "cdc": fastcdc_chunks}