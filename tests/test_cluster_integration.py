import asyncio
import json
import socket
import pytest
from internal.storage import Storage
from internal.broker import Broker
from internal.networking import Server


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class ClusterHelper:
    def __init__(self, tmp_path, ports):
        self.ports = ports
        self.tmp_path = tmp_path
        self.brokers = {}
        self.servers = {}
        self.tasks = {}

    async def start_broker(self, port, role, leader_port):
        log_dir = self.tmp_path / f"broker-{port}"
        storage = Storage(log_dir=log_dir)

        cluster_members = ",".join([f"127.0.0.1:{p}" for p in self.ports])
        broker = Broker(
            storage=storage,
            role=role,
            leader_host="127.0.0.1",
            leader_port=leader_port,
            broker_id=port,
            cluster_members=cluster_members,
            heartbeat_interval=0.05,
            heartbeat_timeout=0.2,
        )
        server = Server("127.0.0.1", port, broker)
        task = asyncio.create_task(server.start())

        self.brokers[port] = broker
        self.servers[port] = server
        self.tasks[port] = task
        await asyncio.sleep(0.05)

    async def stop_all(self):
        for port in list(self.servers.keys()):
            try:
                await self.servers[port].stop()
            except Exception:
                pass
            try:
                await self.brokers[port].replication_manager.stop()
            except Exception:
                pass
            try:
                await self.brokers[port].cluster_manager.stop()
            except Exception:
                pass
            self.tasks[port].cancel()
            try:
                await self.tasks[port]
            except asyncio.CancelledError:
                pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cluster_failures_and_acks(tmp_path):
    # Dynamic ports
    ports = [get_free_port() for _ in range(3)]
    # Leader is the first port (highest ID is max of ports)
    ports.sort()

    cluster = ClusterHelper(tmp_path, ports)

    try:
        # Start brokers
        # ports[2] = Leader
        await cluster.start_broker(ports[2], "leader", ports[2])
        # ports[0] & ports[1] = Followers
        await cluster.start_broker(ports[0], "follower", ports[2])
        await cluster.start_broker(ports[1], "follower", ports[2])

        await asyncio.sleep(0.3)

        # ----------------------------------------------------
        # Test ACK Modes: ACK=0
        # ----------------------------------------------------
        reader, writer = await asyncio.open_connection("127.0.0.1", ports[2])
        req_ack0 = {
            "action": "produce",
            "topic": "test-topic",
            "payload": {"msg": "ack0"},
            "acks": "0",
        }
        writer.write((json.dumps(req_ack0) + "\n").encode())
        await writer.drain()

        # Fire and forget: should receive absolutely no response
        try:
            resp = await asyncio.wait_for(reader.readline(), timeout=0.1)
            assert False, f"Expected no response for acks=0, but got: {resp}"
        except asyncio.TimeoutError:
            pass  # Expected behavior
        writer.close()
        await writer.wait_closed()

        # ----------------------------------------------------
        # Test ACK Modes: ACK=1
        # ----------------------------------------------------
        reader, writer = await asyncio.open_connection("127.0.0.1", ports[2])
        req_ack1 = {
            "action": "produce",
            "topic": "test-topic",
            "payload": {"msg": "ack1"},
            "acks": "1",
        }
        writer.write((json.dumps(req_ack1) + "\n").encode())
        await writer.drain()
        resp = await asyncio.wait_for(reader.readline(), timeout=0.5)
        resp_data = json.loads(resp.decode().strip())
        assert resp_data["status"] == "ok"
        writer.close()
        await writer.wait_closed()

        # ----------------------------------------------------
        # Test ACK Modes: ACK=all
        # ----------------------------------------------------
        reader, writer = await asyncio.open_connection("127.0.0.1", ports[2])
        req_ack_all = {
            "action": "produce",
            "topic": "test-topic",
            "payload": {"msg": "ackall"},
            "acks": "all",
        }
        writer.write((json.dumps(req_ack_all) + "\n").encode())
        await writer.drain()
        resp = await asyncio.wait_for(reader.readline(), timeout=0.5)
        resp_data = json.loads(resp.decode().strip())
        assert resp_data["status"] == "ok"
        writer.close()
        await writer.wait_closed()

        # Verify replication consistency
        for p in ports:
            messages = []
            for _ in range(30):
                messages = cluster.brokers[p].storage.read_all("test-topic", 0)
                if len(messages) >= 2:
                    break
                await asyncio.sleep(0.05)
            assert len(messages) >= 2  # At least ack1 and ackall should be present
            # Verify ordering
            payloads = [m["message"]["msg"] for m in messages]
            assert "ack1" in payloads
            assert "ackall" in payloads

        # ----------------------------------------------------
        # Test Follower Failure (Crash)
        # ----------------------------------------------------
        # Kill Follower 0
        cluster.brokers[ports[0]].cluster_manager.killed = True
        await asyncio.sleep(0.1)

        # Produce more messages to leader
        reader, writer = await asyncio.open_connection("127.0.0.1", ports[2])
        req = {
            "action": "produce",
            "topic": "test-topic",
            "payload": {"msg": "after-crash"},
            "acks": "1",
        }
        writer.write((json.dumps(req) + "\n").encode())
        await writer.drain()
        await reader.readline()
        writer.close()
        await writer.wait_closed()

        # Verify Follower 1 has the message, but Follower 0 does not (since it is killed)
        await asyncio.sleep(0.1)
        msgs_f1 = cluster.brokers[ports[1]].storage.read_all("test-topic", 0)
        payloads_f1 = [m["message"]["msg"] for m in msgs_f1]
        assert "after-crash" in payloads_f1

        # Recover Follower 0
        cluster.brokers[ports[0]].cluster_manager.killed = False

        # Verify Follower 0 caught up and recovered the message (with retry)
        payloads_f0 = []
        for _ in range(30):
            msgs_f0 = cluster.brokers[ports[0]].storage.read_all("test-topic", 0)
            payloads_f0 = [m["message"]["msg"] for m in msgs_f0]
            if "after-crash" in payloads_f0:
                break
            await asyncio.sleep(0.05)
        assert "after-crash" in payloads_f0

        # ----------------------------------------------------
        # Test Network Interruption (Disconnection)
        # ----------------------------------------------------
        # Disconnect Follower 1
        cluster.brokers[ports[1]].cluster_manager.disconnected = True
        await asyncio.sleep(0.1)

        # Produce message
        reader, writer = await asyncio.open_connection("127.0.0.1", ports[2])
        req = {
            "action": "produce",
            "topic": "test-topic",
            "payload": {"msg": "partition-test"},
            "acks": "1",
        }
        writer.write((json.dumps(req) + "\n").encode())
        await writer.drain()
        await reader.readline()
        writer.close()
        await writer.wait_closed()

        # Recover Follower 1
        cluster.brokers[ports[1]].cluster_manager.disconnected = False

        # Verify Follower 1 caught up and recovered the message (with retry)
        payloads_f1 = []
        for _ in range(30):
            msgs_f1 = cluster.brokers[ports[1]].storage.read_all("test-topic", 0)
            payloads_f1 = [m["message"]["msg"] for m in msgs_f1]
            if "partition-test" in payloads_f1:
                break
            await asyncio.sleep(0.05)
        assert "partition-test" in payloads_f1

        # ----------------------------------------------------
        # Test Leader Failure & Automatic Election
        # ----------------------------------------------------
        # Kill the current leader (ports[2])
        cluster.brokers[ports[2]].cluster_manager.killed = True

        # Wait for heartbeats to time out and election to happen.
        # Heartland timeout is 0.4s. Let's wait 1.5s to be safe on slow CI runners.
        await asyncio.sleep(1.5)

        # The new leader should be the active broker with highest ID.
        # Since ports[2] is dead, ports[1] must become the new leader!
        assert cluster.brokers[ports[1]].replication_manager.role == "leader"

        # Verify new leader accepts Produce requests
        reader, writer = await asyncio.open_connection("127.0.0.1", ports[1])
        req_new_leader = {
            "action": "produce",
            "topic": "test-topic",
            "payload": {"msg": "to-new-leader"},
            "acks": "1",
        }
        writer.write((json.dumps(req_new_leader) + "\n").encode())
        await writer.drain()
        resp = await reader.readline()
        resp_data = json.loads(resp.decode().strip())
        assert resp_data["status"] == "ok"
        writer.close()
        await writer.wait_closed()

    finally:
        await cluster.stop_all()
