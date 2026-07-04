import asyncio
import logging
from typing import Dict, Any, List, Tuple
from internal.storage import Storage
from internal.protocol import format_response
from internal.offsets import OffsetManager
from internal.groups import GroupManager, TopicPartition

logger = logging.getLogger(__name__)


class Broker:
    def __init__(
        self,
        storage: Storage,
        offset_manager: OffsetManager = None,
        group_manager: GroupManager = None,
    ):
        self.storage = storage
        self.offset_manager = offset_manager or OffsetManager()
        self.group_manager = group_manager or GroupManager()
        # Map of topic -> list of active standalone consumer queues
        self.consumers: Dict[str, List[asyncio.Queue]] = {}
        # group_id -> topic -> consumer_id -> asyncio.Event
        self.group_events: Dict[str, Dict[str, Dict[str, asyncio.Event]]] = {}
        # group_id -> topic -> consumer_id -> asyncio.Queue
        self.group_queues: Dict[str, Dict[str, Dict[str, asyncio.Queue]]] = {}

    async def publish(
        self, topic: str, payload: Dict[str, Any], key: str = None
    ) -> Tuple[int, int]:
        """Store the message to disk and broadcast to all active consumers."""
        # 1. Store message
        partition_id, offset = self.storage.append(topic, payload, key)

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
                            response = format_response("ok", **msg_data)
                            writer.write(response)
                            await writer.drain()
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
                response = format_response("ok", **msg)
                writer.write(response)
                await writer.drain()
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
                response = format_response("ok", **msg_data)
                writer.write(response)
                await writer.drain()
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
                    response = format_response("ok", **msg)
                    writer.write(response)
                    await writer.drain()
                    self.group_manager.update_offset(group_id, tp, msg_offset + 1)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(
                f"Error consuming partition {partition_id} "
                f"for group {group_id}: {e}"
            )
