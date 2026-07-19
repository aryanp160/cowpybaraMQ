import asyncio
import logging
from typing import Any, Dict, List, Tuple

from internal.groups import GroupManager, TopicPartition
from internal.offsets import OffsetManager
from internal.protocol import format_response
from internal.storage import Storage

logger = logging.getLogger(__name__)


class Broker:
    def __init__(
        self,
        storage: Storage,
        offset_manager: OffsetManager = None,
        group_manager: GroupManager = None,
        role: str = None,
        leader_host: str = None,
        leader_port: int = None,
        broker_id: int = None,
        cluster_members: str = None,
        heartbeat_interval: float = None,
        heartbeat_timeout: float = None,
        compression_type: str = None,
        compression_threshold: int = None,
    ):
        from internal.config import BROKER_ROLE, LEADER_HOST, LEADER_PORT
        from internal.replication import ReplicationManager

        self.storage = storage
        if compression_type is not None:
            self.storage.compression_type = compression_type
        if compression_threshold is not None:
            self.storage.compression_threshold = compression_threshold
        self.offset_manager = offset_manager or OffsetManager()
        self.group_manager = group_manager or GroupManager()
        self.shutting_down = False
        from internal.metrics import MetricsManager

        self.metrics = MetricsManager(self)
        # Map of topic -> list of active standalone consumer queues
        self.consumers: Dict[str, List[asyncio.Queue]] = {}
        # group_id -> topic -> consumer_id -> asyncio.Event
        self.group_events: Dict[str, Dict[str, Dict[str, asyncio.Event]]] = {}
        # group_id -> topic -> consumer_id -> asyncio.Queue
        self.group_queues: Dict[str, Dict[str, Dict[str, asyncio.Queue]]] = {}

        self.msg_counter = 0
        self.messages_per_second = 0
        self.active_producers = 0
        self.throughput_task = None

        role = role or BROKER_ROLE
        leader_host = leader_host or LEADER_HOST
        leader_port = leader_port or LEADER_PORT

        from internal.cluster import ClusterManager

        self.cluster_manager = ClusterManager(
            self,
            broker_id=broker_id,
            cluster_members=cluster_members,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
        )
        asyncio.create_task(self.cluster_manager.start())

        self.replication_manager = ReplicationManager(self, role=role)
        if role == "follower":
            self.replication_manager.start_follower_sync(leader_host, leader_port)

    def _ensure_throughput_task(self):
        if self.throughput_task is None:
            try:
                self.throughput_task = asyncio.create_task(self._track_throughput())
            except RuntimeError:
                pass

    async def _track_throughput(self):
        while True:
            await asyncio.sleep(1.0)
            self.messages_per_second = self.msg_counter
            self.msg_counter = 0

    def get_stats(self) -> Dict[str, Any]:
        self._ensure_throughput_task()

        topics = list(self.storage.partitions.keys())
        partitions_dict = {t: self.storage.num_partitions for t in topics}

        standalone_consumers = []
        for topic, queues in self.consumers.items():
            for i in range(len(queues)):
                standalone_consumers.append(f"consumer-{topic}-{i}")

        group_ids = list(self.group_events.keys())
        group_consumers = []
        ownership_map = {}
        for g_id in group_ids:
            ownership_map[g_id] = {}
            for topic in self.group_events[g_id].keys():
                ownership_map[g_id][topic] = {}
                members = list(self.group_events[g_id][topic].keys())
                group_consumers.extend(members)
                with self.group_manager.lock:
                    assignments = self.group_manager.assignments.get(g_id, {}).get(
                        topic, {}
                    )
                    for cid, tps in assignments.items():
                        ownership_map[g_id][topic][cid] = [tp.partition for tp in tps]

        all_consumers = sorted(
            list(set(standalone_consumers + group_consumers + group_ids))
        )

        offsets_dict = {}
        total_messages = 0
        for topic in topics:
            for p_id, partition in self.storage.partitions[topic].items():
                offset = partition.next_offset
                offsets_dict[f"{topic}-{p_id}"] = offset
                total_messages += offset

        return {
            "topics": partitions_dict,
            "consumers": all_consumers,
            "offsets": offsets_dict,
            "total_messages": total_messages,
            "messages_sec": self.messages_per_second,
            "connected_producers": self.active_producers,
            "partition_ownership": ownership_map,
            "metrics": (
                self.metrics.snapshot()
                if hasattr(self, "metrics") and self.metrics
                else None
            ),
        }

    async def publish(
        self, topic: str, payload: Dict[str, Any], key: str = None, acks: str = "1"
    ) -> Tuple[int, int]:
        """Store the message to disk and broadcast to all active consumers."""
        if self.cluster_manager.killed:
            raise ConnectionError("Broker is killed")
        if self.cluster_manager.disconnected:
            raise ConnectionError("Broker is disconnected from network")

        if self.replication_manager.role == "follower":
            raise PermissionError("Error: Not a leader")

        import time

        start_time = time.time()

        # Log ACK mode
        logger.info(f"Publish request received on topic {topic} with acks={acks}")

        self.msg_counter += 1
        # 1. Store message
        partition_id, offset = self.storage.append(topic, payload, key)

        # Broadcast replication to followers asynchronously
        await self.replication_manager.broadcast_replication(
            topic, partition_id, offset, payload
        )

        if acks == "all":
            # Wait for replication acknowledgment from all active followers
            success = await self.replication_manager.wait_for_acks(
                topic, partition_id, offset
            )
            latency = (time.time() - start_time) * 1000
            self.cluster_manager.latency_metrics.append(latency)
            logger.info(
                f"Replication for offset {offset} with acks=all finished in {latency:.2f}ms (success={success})"
            )
            if not success:
                raise TimeoutError("Timed out waiting for replication ACKs")
        elif acks == "1":
            latency = (time.time() - start_time) * 1000
            self.cluster_manager.latency_metrics.append(latency)
            logger.info(
                f"Replication for offset {offset} with acks=1 finished in {latency:.2f}ms"
            )

        message_data = {
            "topic": topic,
            "payload": payload,
            "offset": offset,
            "partition": partition_id,
        }

        # 2. Broadcast new messages to active standalone consumers
        if topic in self.consumers:
            for queue in self.consumers[topic]:
                await queue.put(message_data)

        # 3. Broadcast to assigned group consumers
        for g_id in list(self.group_events.keys()):
            if topic in self.group_events[g_id]:
                tp = TopicPartition(topic, partition_id)
                assigned_consumer = None
                with self.group_manager.lock:
                    assignments = self.group_manager.assignments.get(g_id, {}).get(
                        topic, {}
                    )
                    for cid, tps in assignments.items():
                        if tp in tps:
                            assigned_consumer = cid
                            break

                if assigned_consumer:
                    q = (
                        self.group_queues.get(g_id, {})
                        .get(topic, {})
                        .get(assigned_consumer)
                    )
                    if q:
                        await q.put(message_data)

        # Record metrics
        import json

        from internal.compression import compress_payload

        compressed_msg, _ = compress_payload(
            payload,
            self.storage.compression_type,
            self.storage.compression_threshold,
        )
        compressed_bytes = len(json.dumps(compressed_msg).encode("utf-8"))
        payload_bytes = len(json.dumps(payload).encode("utf-8"))
        total_prod_latency = (time.time() - start_time) * 1000
        self.metrics.record_produce(compressed_bytes, total_prod_latency, payload_bytes)

        return partition_id, offset

    async def subscribe(
        self,
        topic: str,
        offset: int,
        writer: asyncio.StreamWriter,
        consumer_id: str = None,
        group_id: str = None,
        partition: int = 0,
    ):
        """Read historical messages and keep connection alive to stream new messages."""
        if group_id:
            if not consumer_id:
                import uuid

                consumer_id = f"consumer-{uuid.uuid4().hex[:8]}"

            rebalance_event = asyncio.Event()
            self._register_group_event(group_id, topic, consumer_id, rebalance_event)
            self._trigger_group_rebalance(group_id, topic)

            active_tasks: Dict[int, asyncio.Task] = {}
            queue = asyncio.Queue()
            self._register_group_queue(group_id, topic, consumer_id, queue)

            try:
                while True:
                    assignments = self.group_manager.register_consumer(
                        group_id, consumer_id, topic
                    )
                    assigned_partitions = {tp.partition for tp in assignments}

                    # Start tasks for newly assigned partitions
                    for p in assigned_partitions:
                        if p not in active_tasks:
                            active_tasks[p] = asyncio.create_task(
                                self._consume_group_partition(
                                    group_id, topic, p, consumer_id, writer
                                )
                            )

                    # Stop tasks for partitions that are no longer assigned
                    for p in list(active_tasks.keys()):
                        if p not in assigned_partitions:
                            active_tasks[p].cancel()
                            del active_tasks[p]

                    get_task = asyncio.create_task(queue.get())
                    wait_rebalance_task = asyncio.create_task(rebalance_event.wait())

                    done, pending = await asyncio.wait(
                        [get_task, wait_rebalance_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

                    if wait_rebalance_task in done:
                        rebalance_event.clear()
                        continue

                    if get_task in done:
                        msg_data = get_task.result()
                        p = msg_data.get("partition", 0)
                        if p in assigned_partitions:
                            import time

                            start_c = time.time()
                            response = format_response("ok", **msg_data)
                            writer.write(response)
                            await writer.drain()
                            latency_c = (time.time() - start_c) * 1000
                            import json

                            msg_bytes = len(json.dumps(msg_data).encode("utf-8"))
                            self.metrics.record_consume(msg_bytes, latency_c)

                            msg_offset = msg_data.get("offset")
                            if msg_offset is not None:
                                self.group_manager.update_offset(
                                    group_id, TopicPartition(topic, p), msg_offset + 1
                                )

            except asyncio.CancelledError:
                pass
            except ConnectionError:
                pass
            except Exception as e:
                logger.error(
                    f"Error streaming to group consumer '{consumer_id}' "
                    f"for topic '{topic}': {e}"
                )
            finally:
                for task in active_tasks.values():
                    task.cancel()
                self.group_manager.deregister_consumer(group_id, consumer_id, topic)
                self._deregister_group_event(group_id, topic, consumer_id)
                self._deregister_group_queue(group_id, topic, consumer_id)
                self._trigger_group_rebalance(group_id, topic)
                logger.info(
                    f"Group consumer '{consumer_id}' disconnected from '{topic}'"
                )
            return

        if consumer_id:
            # Stored offset overrides client-supplied offset if present
            offset = self.offset_manager.get_offset(consumer_id, topic)

        logger.info(
            f"Consumer {consumer_id or ''} subscribed to '{topic}' "
            f"starting at offset {offset}"
        )

        # 1. Read all historical messages and send them
        historical_messages = self.storage.read_all(topic)
        for msg in historical_messages:
            msg_offset = msg.get("offset")
            if msg_offset is not None and msg_offset >= offset:
                import time

                start_c = time.time()
                response = format_response("ok", **msg)
                writer.write(response)
                await writer.drain()
                latency_c = (time.time() - start_c) * 1000
                import json

                msg_bytes = len(json.dumps(msg).encode("utf-8"))
                self.metrics.record_consume(msg_bytes, latency_c)

                if consumer_id:
                    self.offset_manager.update_offset(
                        consumer_id, topic, msg_offset + 1
                    )

        # 2. Track consumer queue
        queue = asyncio.Queue()
        if topic not in self.consumers:
            self.consumers[topic] = []
        self.consumers[topic].append(queue)

        try:
            # 3. Stream new messages (keeps connection alive)
            while True:
                msg_data = await queue.get()
                import time

                start_c = time.time()
                response = format_response("ok", **msg_data)
                writer.write(response)
                await writer.drain()
                latency_c = (time.time() - start_c) * 1000
                import json

                msg_bytes = len(json.dumps(msg_data).encode("utf-8"))
                self.metrics.record_consume(msg_bytes, latency_c)

                if consumer_id:
                    msg_offset = msg_data.get("offset")
                    if msg_offset is not None:
                        self.offset_manager.update_offset(
                            consumer_id, topic, msg_offset + 1
                        )
        except asyncio.CancelledError:
            pass
        except ConnectionError:
            pass
        except Exception as e:
            logger.error(f"Error streaming to consumer for topic '{topic}': {e}")
        finally:
            # 4. Clean up consumer on disconnect
            if topic in self.consumers:
                if queue in self.consumers[topic]:
                    self.consumers[topic].remove(queue)
                if not self.consumers[topic]:
                    del self.consumers[topic]
            logger.info(f"Consumer disconnected from '{topic}'")

    def _register_group_event(
        self, group_id: str, topic: str, consumer_id: str, event: asyncio.Event
    ):
        if group_id not in self.group_events:
            self.group_events[group_id] = {}
        if topic not in self.group_events[group_id]:
            self.group_events[group_id][topic] = {}
        self.group_events[group_id][topic][consumer_id] = event

    def _deregister_group_event(self, group_id: str, topic: str, consumer_id: str):
        if group_id in self.group_events and topic in self.group_events[group_id]:
            if consumer_id in self.group_events[group_id][topic]:
                del self.group_events[group_id][topic][consumer_id]
            if not self.group_events[group_id][topic]:
                del self.group_events[group_id][topic]
            if not self.group_events[group_id]:
                del self.group_events[group_id]

    def _register_group_queue(
        self, group_id: str, topic: str, consumer_id: str, queue: asyncio.Queue
    ):
        if group_id not in self.group_queues:
            self.group_queues[group_id] = {}
        if topic not in self.group_queues[group_id]:
            self.group_queues[group_id][topic] = {}
        self.group_queues[group_id][topic][consumer_id] = queue

    def _deregister_group_queue(self, group_id: str, topic: str, consumer_id: str):
        if group_id in self.group_queues and topic in self.group_queues[group_id]:
            if consumer_id in self.group_queues[group_id][topic]:
                del self.group_queues[group_id][topic][consumer_id]
            if not self.group_queues[group_id][topic]:
                del self.group_queues[group_id][topic]
            if not self.group_queues[group_id]:
                del self.group_queues[group_id]

    def _trigger_group_rebalance(self, group_id: str, topic: str):
        if group_id in self.group_events and topic in self.group_events[group_id]:
            for event in self.group_events[group_id][topic].values():
                event.set()

    async def _consume_group_partition(
        self,
        group_id: str,
        topic: str,
        partition_id: int,
        consumer_id: str,
        writer: asyncio.StreamWriter,
    ):
        try:
            tp = TopicPartition(topic, partition_id)
            group_offset = self.group_manager.get_offset(group_id, tp)
            historical_messages = self.storage.read_all(topic, partition_id)
            for msg in historical_messages:
                msg_offset = msg.get("offset")
                if msg_offset is not None and msg_offset >= group_offset:
                    import time

                    start_c = time.time()
                    response = format_response("ok", **msg)
                    writer.write(response)
                    await writer.drain()
                    latency_c = (time.time() - start_c) * 1000
                    import json

                    msg_bytes = len(json.dumps(msg).encode("utf-8"))
                    self.metrics.record_consume(msg_bytes, latency_c)

                    self.group_manager.update_offset(group_id, tp, msg_offset + 1)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(
                f"Error consuming partition {partition_id} "
                f"for group {group_id}: {e}"
            )

    async def shutdown(self):
        """Idempotently shut down the broker and all its background systems."""
        if hasattr(self, "shutting_down") and self.shutting_down:
            logger.info("Broker already shutting down. Ignoring duplicate request.")
            return

        logger.info("Starting graceful shutdown sequence for Broker...")
        self.shutting_down = True

        # 1. Stop throughput tracking task
        if self.throughput_task:
            logger.info("Stopping throughput tracking task...")
            self.throughput_task.cancel()
            try:
                await self.throughput_task
            except asyncio.CancelledError:
                pass
            self.throughput_task = None

        # 2. Stop Cluster Manager
        if hasattr(self, "cluster_manager") and self.cluster_manager:
            logger.info("Stopping Cluster Manager...")
            await self.cluster_manager.stop()

        # 3. Stop Replication Manager
        if hasattr(self, "replication_manager") and self.replication_manager:
            logger.info("Stopping Replication Manager...")
            await self.replication_manager.stop()

        # 4. Flush Storage to disk
        if hasattr(self, "storage") and self.storage:
            logger.info("Flushing pending partition writes to disk...")
            self.storage.flush()

        # 5. Flush Offset Manager to disk
        if hasattr(self, "offset_manager") and self.offset_manager:
            logger.info("Flushing consumer offsets to disk...")
            self.offset_manager.flush()

        # 6. Flush Group Manager to disk
        if hasattr(self, "group_manager") and self.group_manager:
            logger.info("Flushing consumer group offsets to disk...")
            self.group_manager.flush()

        logger.info("Broker graceful shutdown sequence completed.")
