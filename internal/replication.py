import asyncio
import json
import logging
import time
from typing import Dict, Any

logger = logging.getLogger(__name__)


class ReplicationManager:
    def __init__(self, broker, role: str = "leader"):
        self.broker = broker
        self._role = role
        # broker_id -> writer
        self.followers: Dict[str, asyncio.StreamWriter] = {}
        # broker_id -> {topic-partition: offset}
        self.follower_offsets: Dict[str, Dict[str, int]] = {}
        self.sync_task = None
        self.running = True

    @property
    def role(self):
        return self._role

    @role.setter
    def role(self, new_role):
        old_role = getattr(self, "_role", None)
        self._role = new_role
        if old_role is not None and old_role != new_role:
            if hasattr(self.broker, "metrics") and self.broker.metrics:
                self.broker.metrics.record_leader_change()

    async def register_follower(
        self,
        broker_id: str,
        offsets: Dict[str, int],
        writer: asyncio.StreamWriter,
    ):
        """Leader side: Register a follower and stream historical logs."""
        print(f"DEBUG: Registering follower '{broker_id}' with offsets: {offsets}")
        self.followers[broker_id] = writer

        full_offsets = dict(offsets)
        if hasattr(self.broker.storage, "partitions") and isinstance(
            self.broker.storage.partitions, dict
        ):
            for topic, parts in self.broker.storage.partitions.items():
                if hasattr(parts, "keys"):
                    for p_id in parts.keys():
                        key = f"{topic}-{p_id}"
                        if key not in full_offsets:
                            full_offsets[key] = 0

        self.follower_offsets[broker_id] = {k: v - 1 for k, v in full_offsets.items()}

        # Send historical messages the follower is missing
        try:
            for tp_key, offset in full_offsets.items():
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
            if self.followers.get(broker_id) == writer:
                del self.followers[broker_id]
                if broker_id in self.follower_offsets:
                    del self.follower_offsets[broker_id]

    async def handle_replicate_ack(
        self, broker_id: str, topic: str, partition: int, offset: int
    ):
        """Leader side: Handle replica progress ACK from a follower."""
        if broker_id not in self.follower_offsets:
            self.follower_offsets[broker_id] = {}
        self.follower_offsets[broker_id][f"{topic}-{partition}"] = offset
        if hasattr(self.broker, "metrics") and self.broker.metrics:
            self.broker.metrics.record_replication()

    async def wait_for_acks(
        self, topic: str, partition: int, offset: int, timeout: float = 2.0
    ) -> bool:
        """Wait until all connected followers have replicated up to offset."""
        start_time = time.time()
        tp_key = f"{topic}-{partition}"
        while time.time() - start_time < timeout:
            if not self.followers:
                return True
            all_acked = True
            for broker_id in list(self.followers.keys()):
                fol_offset = self.follower_offsets.get(broker_id, {}).get(tp_key, -1)
                if fol_offset < offset:
                    all_acked = False
                    break
            if all_acked:
                return True
            await asyncio.sleep(0.02)
        return False

    async def broadcast_replication(
        self, topic: str, partition: int, offset: int, payload: Dict[str, Any]
    ):
        """Leader side: Broadcast newly appended log entry."""
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
                writer.write(line)
                await writer.drain()
            except Exception as e:
                logger.error(f"Failed to replicate to follower '{broker_id}': {e}")
                if self.followers.get(broker_id) == writer:
                    del self.followers[broker_id]
                    if broker_id in self.follower_offsets:
                        del self.follower_offsets[broker_id]

    def start_follower_sync(self, leader_host: str, leader_port: int):
        """Follower side: Start background sync task."""
        self.running = True
        if self.sync_task and not self.sync_task.done():
            self.sync_task.cancel()
        self.sync_task = asyncio.create_task(self._sync_loop(leader_host, leader_port))

    async def stop_follower_sync(self):
        self.running = False
        if self.sync_task:
            self.sync_task.cancel()
            try:
                await self.sync_task
            except asyncio.CancelledError:
                pass
            self.sync_task = None

    async def stop(self):
        self.running = False
        await self.stop_follower_sync()
        # Close follower connections
        for writer in list(self.followers.values()):
            try:
                writer.close()
            except Exception:
                pass
        self.followers.clear()
        self.follower_offsets.clear()

    async def _sync_loop(self, leader_host: str, leader_port: int):
        """Follower side: Connect to leader, register, and apply logs."""
        broker_id = str(self.broker.cluster_manager.broker_id)

        while self.running:
            if (
                self.broker.cluster_manager.killed
                or self.broker.cluster_manager.disconnected
            ):
                await asyncio.sleep(0.5)
                continue

            writer = None
            try:
                logger.info(
                    f"Follower connecting to leader at {leader_host}:{leader_port}"
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
                    if (
                        self.broker.cluster_manager.killed
                        or self.broker.cluster_manager.disconnected
                    ):
                        break

                    try:
                        line = await asyncio.wait_for(reader.readline(), timeout=0.2)
                    except asyncio.TimeoutError:
                        continue

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

                            # Send replicate_ack to leader
                            ack_req = {
                                "action": "replicate_ack",
                                "broker_id": broker_id,
                                "topic": topic,
                                "partition": partition,
                                "offset": offset,
                            }
                            writer.write((json.dumps(ack_req) + "\n").encode())
                            await writer.drain()

                    except json.JSONDecodeError:
                        pass

            except ConnectionRefusedError:
                pass
            except (ConnectionError, asyncio.CancelledError):
                pass
            except Exception as e:
                logger.error(f"Unexpected error in sync loop: {e}")
            finally:
                if writer:
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass

            if self.running:
                await asyncio.sleep(0.05)
