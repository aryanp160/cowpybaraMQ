# Partitions Documentation

Topic partitioning increases scalability and parallel processing capabilities.

## Partition Hashing

When producing a message, the partition is selected deterministically:
```python
if key is not None:
    partition_id = zlib.crc32(key.encode("utf-8")) % num_partitions
else:
    partition_id = 0
```
This ensures that:
- Messages published with the same key always route to the same partition.
- Order is strictly preserved inside each partition.
- Keyless messages route to partition 0 for backward compatibility.
