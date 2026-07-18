import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple

from internal.config import COMPRESSION_THRESHOLD, COMPRESSION_TYPE, NUM_PARTITIONS
from internal.partition import Partition


@dataclass
class Storage:
    log_dir: Path
    num_partitions: int = NUM_PARTITIONS
    compression_type: str = COMPRESSION_TYPE
    compression_threshold: int = COMPRESSION_THRESHOLD
    # topic_name -> partition_id -> Partition
    partitions: Dict[str, Dict[int, Partition]] = field(
        init=False, default_factory=dict
    )
    # Round-robin counter per topic for messages published without key
    _rr_counters: Dict[str, int] = field(init=False, default_factory=dict)

    def __post_init__(self):
        """Initialize storage by ensuring log dir exists and loading partition logs."""
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Discover and load existing partition files from disk
        for file_path in self.log_dir.glob("*-*.jsonl"):
            stem = file_path.stem
            parts = stem.rsplit("-", 1)
            if len(parts) == 2 and parts[1].isdigit():
                topic_name = parts[0]
                partition_id = int(parts[1])

                if topic_name not in self.partitions:
                    self.partitions[topic_name] = {}
                self.partitions[topic_name][partition_id] = Partition(
                    topic_name=topic_name,
                    partition_id=partition_id,
                    log_dir=self.log_dir,
                )

    def get_partition(self, topic_name: str, partition_id: int) -> Partition:
        """Get an existing partition or create it automatically if it doesn't exist."""
        if topic_name not in self.partitions:
            self.partitions[topic_name] = {}
        if partition_id not in self.partitions[topic_name]:
            self.partitions[topic_name][partition_id] = Partition(
                topic_name=topic_name,
                partition_id=partition_id,
                log_dir=self.log_dir,
            )
        return self.partitions[topic_name][partition_id]

    def append(
        self,
        topic_name: str,
        message: Dict[str, Any],
        key: str = None,
        partition_id: int = None,
        offset: int = None,
    ) -> Tuple[int, int]:
        """Route to partition and append, returning (partition_id, offset)."""
        if partition_id is None:
            if key is not None:
                # Hash message key to select partition
                h_val = zlib.crc32(key.encode("utf-8"))
                partition_id = h_val % self.num_partitions
            else:
                # Default to partition 0 if key is absent for backward
                # compatibility until partition assignment is implemented.
                partition_id = 0

        partition = self.get_partition(topic_name, partition_id)
        offset = partition.append(
            message,
            offset,
            codec=self.compression_type,
            threshold=self.compression_threshold,
        )
        return partition_id, offset

    def read_all(self, topic_name: str, partition_id: int = 0) -> List[Dict[str, Any]]:
        """Read all messages from the specified partition of a topic."""
        partition = self.get_partition(topic_name, partition_id)
        return partition.read_all()

    def flush(self):
        """Flush all partitions managed by this Storage instance to disk."""
        for topic_name, partition_map in list(self.partitions.items()):
            for partition_id, partition in list(partition_map.items()):
                partition.flush()
