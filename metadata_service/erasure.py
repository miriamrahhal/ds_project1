"""Reed-Solomon erasure coding using zfec: k data fragments + m parity fragments."""
from zfec.easyfec import Decoder, Encoder


def encode(data: bytes, k: int, m: int) -> list[bytes]:
    """Split data into k + m fragments. Any k fragments can rebuild the data."""
    return list(Encoder(k, k + m).encode(data))


def decode(fragments: dict[int, bytes], k: int, m: int, size: int) -> bytes:
    """Rebuild the original data from any k fragments, given as {index: fragment}."""
    if len(fragments) < k:
        raise ValueError(f"need {k} fragments, got {len(fragments)}")
    indexes = sorted(fragments)[:k]
    padding = k * len(fragments[indexes[0]]) - size
    return Decoder(k, k + m).decode([fragments[i] for i in indexes], indexes, padding)