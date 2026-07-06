import asyncio
import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class ReplicationManager:
    def __init__(self, broker, role: str = "leader"):
        self.broker = broker
        self.role = role
        # broker_id -> writer
        self.followers: Dict[str, asyncio.StreamWriter] = {}
        self.sync_task = None
        self.running = True

    async def register_follower(
        self,
        broker_id: str,
        offsets: Dict[str, int],
        writer: asyncio.StreamWriter,
    ):
        """Leader side: Register a follower and stream historical logs."""
        print(f"DEBUG: Registering follower '{broker_id}' with offsets: {offsets}")
        self.followers[broker_id] = writer

        # Send historical messages the follower is missing
        try:
            for tp_key, offset in offsets.items():
                parts = tp_key.rsplit("-", 1)
                if len(parts) == 2 and parts[1].isdigit():
                    topic = parts[0]
                    partition_id = int(parts[1])

                    historical = self.broker.storage.read_all(topic, partition_id)
                    for msg in historical:
                        msg_offset = msg.get("offset")
                        if msg_offset is not None and msg_offset >= offset:
                            req = {
                                "action": "replicate",
                                "topic": topic,
                                "partition": partition_id,
                                "offset": msg_offset,
                                "payload": msg.get("message"),
                            }
                            writer.write((json.dumps(req) + "\n").encode())
                            await writer.drain()
        except (ConnectionError, asyncio.CancelledError) as e:
            logger.error(f"Failed to catch up follower '{broker_id}': {e}")
            if broker_id in self.followers:
                del self.followers[broker_id]

    async def broadcast_replication(
        self, topic: str, partition: int, offset: int, payload: Dict[str, Any]
    ):
        """Leader side: Broadcast newly appended log entry."""
        print(
            f"DEBUG: broadcast_replication self.followers = {list(self.followers.keys())}"
        )
        if not self.followers:
            return

        req = {
            "action": "replicate",
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "payload": payload,
        }
        line = (json.dumps(req) + "\n").encode()

        for broker_id, writer in list(self.followers.items()):
            try:
                print(f"DEBUG: Sending replication to {broker_id}")
                writer.write(line)
                await writer.drain()
                logger.info(
                    f"Successfully replicated {topic}-{partition} "
                    f"offset {offset} to follower {broker_id}"
                )
            except Exception as e:
                logger.error(f"Failed to replicate to follower '{broker_id}': {e}")
                if broker_id in self.followers:
                    del self.followers[broker_id]

    def start_follower_sync(self, leader_host: str, leader_port: int):
        """Follower side: Start background sync task."""
        self.sync_task = asyncio.create_task(self._sync_loop(leader_host, leader_port))

    async def stop(self):
        self.running = False
        if self.sync_task:
            self.sync_task.cancel()
            try:
                await self.sync_task
            except asyncio.CancelledError:
                pass
        # Close follower connections
        for writer in list(self.followers.values()):
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        self.followers.clear()

    async def _sync_loop(self, leader_host: str, leader_port: int):
        """Follower side: Connect to leader, register, and apply logs."""
        broker_id = f"follower-{id(self.broker)}"

        while self.running:
            try:
                logger.info(
                    f"Follower connecting to leader at " f"{leader_host}:{leader_port}"
                )
                reader, writer = await asyncio.open_connection(leader_host, leader_port)

                # Gather local offsets
                offsets = {}
                for topic, parts in self.broker.storage.partitions.items():
                    for p_id, part in parts.items():
                        offsets[f"{topic}-{p_id}"] = part.next_offset

                # Send register request
                reg_req = {
                    "action": "register_follower",
                    "broker_id": broker_id,
                    "offsets": offsets,
                }
                writer.write((json.dumps(reg_req) + "\n").encode())
                await writer.drain()

                logger.info("Registered with leader. Syncing logs...")

                while self.running:
                    line = await reader.readline()
                    print(f"DEBUG: Follower received line: {line}")
                    if not line:
                        logger.warning("Disconnected from leader.")
                        break

                    try:
                        data = json.loads(line.decode("utf-8").strip())
                        if data.get("action") == "replicate":
                            topic = data.get("topic")
                            partition = data.get("partition")
                            offset = data.get("offset")
                            payload = data.get("payload")

                            if (
                                not topic
                                or partition is None
                                or offset is None
                                or payload is None
                            ):
                                continue

                            # Append replicated message
                            self.broker.storage.append(
                                topic_name=topic,
                                message=payload,
                                partition_id=partition,
                                offset=offset,
                            )
                            logger.info(
                                f"Replicated and stored {topic}-{partition} "
                                f"offset {offset}"
                            )
                    except json.JSONDecodeError:
                        pass

            except ConnectionRefusedError:
                logger.warning("Leader connection refused. Retrying in 1s...")
            except (ConnectionError, asyncio.CancelledError):
                pass
            except Exception as e:
                logger.error(f"Unexpected error in sync loop: {e}")

            if self.running:
                await asyncio.sleep(1.0)
