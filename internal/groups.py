import json
import logging
import threading
from pathlib import Path
from typing import Dict, List
from dataclasses import dataclass
from internal.config import NUM_PARTITIONS


@dataclass(frozen=True)
class TopicPartition:
    topic: str
    partition: int


class GroupManager:
    def __init__(
        self,
        filepath: str = "storage/group_offsets.json",
        num_partitions: int = NUM_PARTITIONS,
    ):
        self.filepath = Path(filepath)
        self.num_partitions = num_partitions
        self.lock = threading.RLock()

        # group_id -> topic -> partition_id -> offset
        self.offsets: Dict[str, Dict[str, Dict[str, int]]] = {}

        # group_id -> topic -> list of active consumer_ids
        self.members: Dict[str, Dict[str, List[str]]] = {}

        # group_id -> topic -> consumer_id -> list of TopicPartition
        self.assignments: Dict[str, Dict[str, Dict[str, List[TopicPartition]]]] = {}

        self.load()

    def load(self):
        """Load group offsets from disk."""
        with self.lock:
            if not self.filepath.exists():
                self.filepath.parent.mkdir(parents=True, exist_ok=True)
                self.offsets = {}
                self.save_unlocked()
                return
            try:
                with self.filepath.open("r", encoding="utf-8") as f:
                    self.offsets = json.load(f)
            except (json.JSONDecodeError, IOError):
                self.offsets = {}

    def save_unlocked(self):
        """Save group offsets to disk (assumes lock is held)."""
        with self.filepath.open("w", encoding="utf-8") as f:
            json.dump(self.offsets, f, indent=4)

    def get_offset(self, group_id: str, tp: TopicPartition) -> int:
        """Get the current offset for a group and TopicPartition. Default to 0."""
        with self.lock:
            p_str = str(tp.partition)
            if group_id not in self.offsets:
                self.offsets[group_id] = {}
            if tp.topic not in self.offsets[group_id]:
                self.offsets[group_id][tp.topic] = {}
            if p_str not in self.offsets[group_id][tp.topic]:
                self.offsets[group_id][tp.topic][p_str] = 0
                self.save_unlocked()
            return self.offsets[group_id][tp.topic][p_str]

    def update_offset(self, group_id: str, tp: TopicPartition, offset: int):
        """Update the offset for a group and TopicPartition, saving to disk."""
        with self.lock:
            p_str = str(tp.partition)
            if group_id not in self.offsets:
                self.offsets[group_id] = {}
            if tp.topic not in self.offsets[group_id]:
                self.offsets[group_id][tp.topic] = {}
            self.offsets[group_id][tp.topic][p_str] = offset
            self.save_unlocked()

    def register_consumer(
        self, group_id: str, consumer_id: str, topic: str
    ) -> List[TopicPartition]:
        """Register a consumer in the group and perform a rebalance."""
        with self.lock:
            if group_id not in self.members:
                self.members[group_id] = {}
            if topic not in self.members[group_id]:
                self.members[group_id][topic] = []

            if consumer_id not in self.members[group_id][topic]:
                self.members[group_id][topic].append(consumer_id)
                self._rebalance_unlocked(group_id, topic)

            return (
                self.assignments.get(group_id, {}).get(topic, {}).get(consumer_id, [])
            )

    def deregister_consumer(self, group_id: str, consumer_id: str, topic: str):
        """Deregister a consumer from the group and trigger a rebalance."""
        with self.lock:
            if group_id in self.members and topic in self.members[group_id]:
                if consumer_id in self.members[group_id][topic]:
                    self.members[group_id][topic].remove(consumer_id)
                    if not self.members[group_id][topic]:
                        del self.members[group_id][topic]
                        if not self.members[group_id]:
                            del self.members[group_id]
                    self._rebalance_unlocked(group_id, topic)

    def get_assignment(
        self, group_id: str, consumer_id: str, topic: str
    ) -> List[TopicPartition]:
        """Get the current partition assignment for a consumer."""
        with self.lock:
            return (
                self.assignments.get(group_id, {}).get(topic, {}).get(consumer_id, [])
            )

    def _rebalance_unlocked(self, group_id: str, topic: str):
        """Assign partitions to active members in a group (Round-robin)."""
        if group_id not in self.assignments:
            self.assignments[group_id] = {}
        self.assignments[group_id][topic] = {}

        members = self.members.get(group_id, {}).get(topic, [])
        if not members:
            logger = logging.getLogger(__name__)
            logger.info(
                f"[Rebalance] Group '{group_id}' topic '{topic}': No active members."
            )
            return

        # Round-robin distribution of all partitions
        partitions = [TopicPartition(topic, p) for p in range(self.num_partitions)]

        for i, tp in enumerate(partitions):
            assigned_member = members[i % len(members)]
            if assigned_member not in self.assignments[group_id][topic]:
                self.assignments[group_id][topic][assigned_member] = []
            self.assignments[group_id][topic][assigned_member].append(tp)

        # Logging partition ownership
        logger = logging.getLogger(__name__)
        ownership = {
            m: [tp.partition for tp in self.assignments[group_id][topic].get(m, [])]
            for m in members
        }
        logger.info(
            f"[Rebalance] Group '{group_id}' topic '{topic}' "
            f"ownership assignments: {ownership}"
        )

    def flush(self):
        """Force write all group offsets to disk and call fsync."""
        with self.lock:
            self.save_unlocked()
            try:
                import os

                fd = os.open(str(self.filepath), os.O_RDWR)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except Exception:
                pass
