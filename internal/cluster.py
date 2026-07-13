import asyncio
import json
import logging
import time
from typing import List

logger = logging.getLogger(__name__)


class ClusterManager:
    def __init__(
        self,
        broker,
        broker_id=None,
        cluster_members=None,
        heartbeat_interval=None,
        heartbeat_timeout=None,
    ):
        self.broker = broker
        from internal.config import (
            CLUSTER_MEMBERS,
            BROKER_ID,
            HEARTBEAT_INTERVAL,
            HEARTBEAT_TIMEOUT,
        )

        self.broker_id = broker_id if broker_id is not None else BROKER_ID
        self.heartbeat_interval = (
            heartbeat_interval if heartbeat_interval is not None else HEARTBEAT_INTERVAL
        )
        self.heartbeat_timeout = (
            heartbeat_timeout if heartbeat_timeout is not None else HEARTBEAT_TIMEOUT
        )

        members_str = (
            cluster_members if cluster_members is not None else CLUSTER_MEMBERS
        )
        # Parse cluster members: list of (host, port)
        self.members: List[tuple] = []
        for m in members_str.split(","):
            m = m.strip()
            if m:
                parts = m.split(":")
                if len(parts) == 2:
                    self.members.append((parts[0], int(parts[1])))

        self.heartbeat_service = HeartbeatService(self)
        self.election_manager = ElectionManager(self)
        self.killed = False
        self.disconnected = False
        self.latency_metrics: List[float] = []

    def log_event(self, event: str):
        logger.warning(f"[ELECTION EVENT] {event}")
        print(f"[ELECTION EVENT] {event}")

    async def start(self):
        self.heartbeat_service.start()

    async def stop(self):
        await self.heartbeat_service.stop()


class HeartbeatService:
    def __init__(self, cluster_manager: ClusterManager):
        self.cm = cluster_manager
        self.last_heartbeat_time = time.time()
        self.heartbeat_task = None
        self.running = False
        self.leader_id = None

    def start(self):
        self.running = True
        self.last_heartbeat_time = time.time()
        self.heartbeat_task = asyncio.create_task(self._loop())

    async def stop(self):
        self.running = False
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass

    def receive_heartbeat(self, sender_id: str, role: str):
        if self.cm.killed or self.cm.disconnected:
            return
        if role == "leader":
            self.last_heartbeat_time = time.time()
            self.leader_id = sender_id
            # If we were previously a leader but received a heartbeat from a leader with higher ID,
            # step down. Or if we are follower, keep follower state.
            if (
                self.cm.broker.replication_manager.role == "leader"
                and int(sender_id) > self.cm.broker_id
            ):
                self.cm.log_event(
                    f"Broker {self.cm.broker_id} stepping down as leader. "
                    f"Detected leader {sender_id}."
                )
                self.cm.broker.replication_manager.role = "follower"
                # Connect follower sync to new leader
                # Find port of sender_id
                for host, port in self.cm.members:
                    if port == int(sender_id):
                        self.cm.broker.replication_manager.start_follower_sync(
                            host, port
                        )
                        break

    async def _loop(self):
        while self.running:
            try:
                await asyncio.sleep(self.cm.heartbeat_interval)
                if self.cm.killed or self.cm.disconnected:
                    continue

                role = self.cm.broker.replication_manager.role
                if role == "leader":
                    # Broadcast heartbeat to all members
                    heartbeat_req = {
                        "action": "heartbeat",
                        "sender_id": str(self.cm.broker_id),
                        "role": "leader",
                    }
                    line = (json.dumps(heartbeat_req) + "\n").encode()
                    for host, port in self.cm.members:
                        if port == self.cm.broker_id:
                            continue
                        try:
                            _, writer = await asyncio.wait_for(
                                asyncio.open_connection(host, port), timeout=0.1
                            )
                            writer.write(line)
                            await writer.drain()
                            writer.close()
                            try:
                                await asyncio.wait_for(
                                    writer.wait_closed(), timeout=0.1
                                )
                            except Exception:
                                pass
                        except Exception:
                            pass
                else:
                    # Follower: check for heartbeat timeout
                    age = time.time() - self.last_heartbeat_time
                    if age > self.cm.heartbeat_timeout:
                        self.cm.log_event(
                            f"Heartbeat timeout ({age:.2f}s). "
                            f"Triggering leader election."
                        )
                        # Reset heartbeat time to prevent immediate re-trigger
                        self.last_heartbeat_time = time.time()
                        await self.cm.election_manager.trigger_election()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")


class ElectionManager:
    def __init__(self, cluster_manager: ClusterManager):
        self.cm = cluster_manager

    async def trigger_election(self):
        if self.cm.killed or self.cm.disconnected:
            return

        self.cm.log_event(f"Broker {self.cm.broker_id} initiating leader election.")

        # Contact all other brokers to determine who is active and has highest broker ID.
        active_ids = [self.cm.broker_id]
        elect_req = {"action": "elect", "candidate_id": str(self.cm.broker_id)}
        line = (json.dumps(elect_req) + "\n").encode()

        for host, port in self.cm.members:
            if port == self.cm.broker_id:
                continue
            try:

                async def _send_and_read():
                    r, w = await asyncio.open_connection(host, port)
                    try:
                        w.write(line)
                        await w.drain()
                        res = await r.readline()
                        return res
                    finally:
                        w.close()
                        try:
                            await asyncio.wait_for(w.wait_closed(), timeout=0.1)
                        except Exception:
                            pass

                resp = await asyncio.wait_for(_send_and_read(), timeout=0.3)
                if resp:
                    data = json.loads(resp.decode().strip())
                    if data.get("status") == "ok":
                        active_ids.append(port)
            except Exception:
                pass

        highest_id = max(active_ids)
        self.cm.log_event(
            f"Active brokers discovered: {active_ids}. Highest ID: {highest_id}."
        )

        if highest_id == self.cm.broker_id:
            # Promote ourselves to leader!
            self.cm.log_event(f"Broker {self.cm.broker_id} promoting itself to LEADER.")
            # Stop follower sync if it is running
            await self.cm.broker.replication_manager.stop_follower_sync()
            self.cm.broker.replication_manager.role = "leader"
        else:
            self.cm.log_event(
                f"Broker {self.cm.broker_id} remains Follower. "
                f"New leader is broker {highest_id}."
            )
            self.cm.broker.replication_manager.role = "follower"
            # Start/Redirect sync to the new leader
            for host, port in self.cm.members:
                if port == highest_id:
                    self.cm.broker.replication_manager.start_follower_sync(host, port)
                    break
