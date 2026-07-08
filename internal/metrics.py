import os
import time
from typing import Dict, Any, List


class MetricsManager:
    def __init__(self, broker=None):
        self.broker = broker
        self.reset()

    def reset(self):
        """Reset all metrics counter and latency arrays."""
        self.start_time = time.time()
        self.messages_produced = 0
        self.messages_consumed = 0
        self.bytes_written = 0
        self.bytes_read = 0
        self.replication_events = 0
        self.leader_changes = 0

        # Latency trackers
        self.produce_latencies: List[float] = []
        self.consume_latencies: List[float] = []

    def record_produce(self, size_bytes: int, latency_ms: float):
        self.messages_produced += 1
        self.bytes_written += size_bytes
        self.produce_latencies.append(latency_ms)

    def record_consume(self, size_bytes: int, latency_ms: float):
        self.messages_consumed += 1
        self.bytes_read += size_bytes
        self.consume_latencies.append(latency_ms)

    def record_replication(self):
        self.replication_events += 1

    def record_leader_change(self):
        self.leader_changes += 1

    def snapshot(self) -> Dict[str, Any]:
        """Generate a complete real-time snapshot of all metrics."""
        elapsed = time.time() - self.start_time
        if elapsed <= 0:
            elapsed = 0.001

        avg_prod_lat = (
            sum(self.produce_latencies) / len(self.produce_latencies)
            if self.produce_latencies
            else 0.0
        )
        avg_cons_lat = (
            sum(self.consume_latencies) / len(self.consume_latencies)
            if self.consume_latencies
            else 0.0
        )

        tp_msg_sec = self.messages_produced / elapsed
        tp_bytes_sec = self.bytes_written / elapsed

        # Default storage metrics
        partition_sizes: Dict[str, int] = {}
        log_size = 0
        disk_usage = 0

        # Default consumer metrics
        active_consumers = 0
        active_consumer_groups = 0
        lag_per_group: Dict[str, int] = {}

        if self.broker:
            # 1. Storage & Disk metrics
            storage = self.broker.storage
            if storage and storage.log_dir:
                try:
                    # Scan log directory
                    for entry in os.scandir(storage.log_dir):
                        if entry.is_file() and entry.name.endswith(".jsonl"):
                            sz = entry.stat().st_size
                            partition_sizes[entry.name] = sz
                            log_size += sz
                            disk_usage += sz
                        elif entry.is_file():
                            disk_usage += entry.stat().st_size
                except Exception:
                    pass

            # 2. Consumer Metrics
            # Standalone consumers
            active_consumers += sum(
                len(queues) for queues in self.broker.consumers.values()
            )
            # Group consumers
            for topic_cids in self.broker.group_queues.values():
                for cids in topic_cids.values():
                    active_consumers += 1

            # Active consumer groups
            group_manager = self.broker.group_manager
            if group_manager:
                with group_manager.lock:
                    active_consumer_groups = len(group_manager.assignments)

                    # Compute lag per consumer group
                    # group_id -> topic -> consumer_id -> tps
                    for g_id, topics_map in group_manager.assignments.items():
                        total_group_lag = 0
                        for topic, assignments in topics_map.items():
                            # Find all partitions for this topic
                            for part_id in range(storage.num_partitions):
                                # Get latest offset
                                partition = storage.get_partition(topic, part_id)
                                latest_offset = partition.next_offset

                                # Get committed offset
                                from internal.groups import TopicPartition

                                tp = TopicPartition(topic, part_id)
                                committed_offset = group_manager.get_offset(g_id, tp)

                                lag = max(0, latest_offset - committed_offset)
                                total_group_lag += lag
                        lag_per_group[g_id] = total_group_lag

        return {
            "broker": {
                "messages_produced": self.messages_produced,
                "messages_consumed": self.messages_consumed,
                "bytes_written": self.bytes_written,
                "bytes_read": self.bytes_read,
                "replication_events": self.replication_events,
                "leader_changes": self.leader_changes,
            },
            "performance": {
                "average_produce_latency_ms": avg_prod_lat,
                "average_consume_latency_ms": avg_cons_lat,
                "throughput_messages_sec": tp_msg_sec,
                "throughput_bytes_sec": tp_bytes_sec,
            },
            "storage": {
                "partition_sizes": partition_sizes,
                "log_size_bytes": log_size,
                "disk_usage_bytes": disk_usage,
            },
            "consumers": {
                "active_consumers": active_consumers,
                "active_consumer_groups": active_consumer_groups,
                "lag_per_consumer_group": lag_per_group,
            },
        }

    def pretty_print(self) -> str:
        """Return a formatted string report of the metrics snapshot."""
        snap = self.snapshot()
        lines = [
            "==================================================",
            "              COWPYBARAMQ RUNTIME METRICS         ",
            "==================================================",
            "Broker Metrics:",
            f"  Messages Produced:   {snap['broker']['messages_produced']}",
            f"  Messages Consumed:   {snap['broker']['messages_consumed']}",
            f"  Bytes Written:       {snap['broker']['bytes_written']} bytes",
            f"  Bytes Read:          {snap['broker']['bytes_read']} bytes",
            f"  Replication Events:  {snap['broker']['replication_events']}",
            f"  Leader Changes:      {snap['broker']['leader_changes']}",
            "--------------------------------------------------",
            "Performance Metrics:",
            f"  Avg Produce Latency: {snap['performance']['average_produce_latency_ms']:.2f} ms",
            f"  Avg Consume Latency: {snap['performance']['average_consume_latency_ms']:.2f} ms",
            f"  Throughput (msgs):   {snap['performance']['throughput_messages_sec']:.2f} msgs/sec",
            f"  Throughput (bytes):  {snap['performance']['throughput_bytes_sec']:.2f} bytes/sec",
            "--------------------------------------------------",
            "Storage Metrics:",
            f"  Total Log Size:      {snap['storage']['log_size_bytes']} bytes",
            f"  Total Disk Usage:    {snap['storage']['disk_usage_bytes']} bytes",
            f"  Partitions Scan:     {snap['storage']['partition_sizes']}",
            "--------------------------------------------------",
            "Consumer Metrics:",
            f"  Active Consumers:    {snap['consumers']['active_consumers']}",
            f"  Active Groups:       {snap['consumers']['active_consumer_groups']}",
            f"  Group Lag Details:   {snap['consumers']['lag_per_consumer_group']}",
            "==================================================",
        ]
        return "\n".join(lines)
