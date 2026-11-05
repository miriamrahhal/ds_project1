"""Stores one sample object in PostgreSQL and reads it back."""
from metadata_service import metadata_store as store
from metadata_service.chunking import fixed_size_chunks, sha256_hex

data = b"sample object for the metadata store " * 1000
hashes = [sha256_hex(chunk) for chunk in fixed_size_chunks(data, 8 * 1024)]

store.init_schema()
with store.transaction() as cur:
    version = store.next_version(cur, "demo-bucket", "sample.txt")
    store.insert_object(cur, "demo-bucket", "sample.txt", version, len(data), "fixed", hashes)

with store.transaction() as cur:
    row = store.get_object(cur, "demo-bucket", "sample.txt")

print("bucket:", row["bucket"])
print("key:", row["key"])
print("size:", row["size_bytes"], "bytes")
print("version:", row["version"])
print("created:", row["created_at"])
print("chunks in order:", len(row["chunk_hashes"]))
for index, chunk_hash in enumerate(row["chunk_hashes"]):
    print(f"  {index}: {chunk_hash}")