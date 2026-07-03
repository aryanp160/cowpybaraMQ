import asyncio
import logging
from typing import Dict, Any, List
from internal.storage import Storage
from internal.protocol import format_response
from internal.offsets import OffsetManager

logger = logging.getLogger(__name__)


class Broker:
    def __init__(self, storage: Storage, offset_manager: OffsetManager = None):
        self.storage = storage
        self.offset_manager = offset_manager or OffsetManager()
        # Map of topic -> list of active consumer queues
        self.consumers: Dict[str, List[asyncio.Queue]] = {}

    async def publish(self, topic: str, payload: Dict[str, Any]) -> int:
        """Store the message to disk and broadcast to all active consumers."""
        # 1. Store message
        offset = self.storage.append(topic, payload)

        message_data = {"topic": topic, "payload": payload, "offset": offset}

        # 2. Broadcast new messages to active consumers
        if topic in self.consumers:
            for queue in self.consumers[topic]:
                await queue.put(message_data)

        return offset

    async def subscribe(
        self,
        topic: str,
        offset: int,
        writer: asyncio.StreamWriter,
        consumer_id: str = None,
    ):
        """Read historical messages and keep connection alive to stream new messages."""
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
