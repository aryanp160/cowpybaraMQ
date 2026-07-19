import asyncio
import json
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def query_cluster_status(port):
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        req = {"action": "cluster_status"}
        writer.write((json.dumps(req) + "\n").encode("utf-8"))
        await writer.drain()
        line = await reader.readline()
        writer.close()
        await writer.wait_closed()
        if line:
            return json.loads(line.decode("utf-8").strip())
    except Exception:
        pass
    return None


async def send_produce(port, topic, payload, key=None, acks="1"):
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        req = {"action": "produce", "topic": topic, "payload": payload, "acks": acks}
        if key is not None:
            req["key"] = key
        writer.write((json.dumps(req) + "\n").encode("utf-8"))
        await writer.drain()
        if acks == "0":
            writer.close()
            await writer.wait_closed()
            return {"status": "ok"}
        line = await reader.readline()
        writer.close()
        await writer.wait_closed()
        if line:
            return json.loads(line.decode("utf-8").strip())
    except Exception as e:
        return {"status": "error", "message": str(e)}
    return {"status": "error", "message": "no response"}


@pytest.fixture
def cluster():
    ports = sorted([get_free_port() for _ in range(3)])
    cluster_members = ",".join(f"127.0.0.1:{p}" for p in ports)

    processes = {}
    log_dirs = {}

    # Clean any leftover logs
    for p in ports:
        log_dir = Path(f"./logs-{p}")
        if log_dir.exists():
            shutil.rmtree(log_dir, ignore_errors=True)
        log_dirs[p] = log_dir

    repo_root = Path(__file__).resolve().parent.parent

    def start_broker(port, role, leader_port):
        proc = subprocess.Popen(
            [
                sys.executable,
                "cmd/broker.py",
                "--port",
                str(port),
                "--role",
                role,
                "--broker-id",
                str(port),
                "--cluster-members",
                cluster_members,
                "--leader-host",
                "127.0.0.1",
                "--leader-port",
                str(leader_port),
                "--heartbeat-interval",
                "0.1",
                "--heartbeat-timeout",
                "0.3",
            ],
            cwd=str(repo_root),
        )
        processes[port] = proc
        return proc

    # Start leader (lowest port/ID)
    start_broker(ports[0], "leader", ports[0])
    # Start followers
    start_broker(ports[1], "follower", ports[0])
    start_broker(ports[2], "follower", ports[0])

    # Wait for startup
    time.sleep(2.0)

    yield {
        "ports": ports,
        "cluster_members": cluster_members,
        "processes": processes,
        "start_broker": start_broker,
        "log_dirs": log_dirs,
    }

    # Cleanup
    for proc in list(processes.values()):
        try:
            proc.terminate()
            proc.wait(timeout=2.0)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    for log_dir in log_dirs.values():
        shutil.rmtree(log_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_end_to_end_cluster_flow(cluster):
    ports = cluster["ports"]

    # 1. Validate Initial Cluster Roles
    status0 = await query_cluster_status(ports[0])
    status1 = await query_cluster_status(ports[1])
    status2 = await query_cluster_status(ports[2])

    assert status0 is not None and status0["stats"]["role"] == "leader"
    assert status1 is not None and status1["stats"]["role"] == "follower"
    assert status2 is not None and status2["stats"]["role"] == "follower"

    # 2. Validate Asynchronous Replication & ACK modes
    res_ack0 = await send_produce(ports[0], "e2e-topic", {"val": 100}, acks="0")
    assert res_ack0["status"] == "ok"
    res_ack1 = await send_produce(ports[0], "e2e-topic", {"val": 200}, acks="1")
    assert res_ack1["status"] == "ok"

    res_ackall = await send_produce(ports[0], "e2e-topic", {"val": 300}, acks="all")
    assert res_ackall["status"] == "ok"

    # Wait for replication catchup
    await asyncio.sleep(1.0)

    status1_after = await query_cluster_status(ports[1])
    status2_after = await query_cluster_status(ports[2])

    assert status1_after["stats"]["offsets"].get("e2e-topic-0", 0) >= 1
    assert status2_after["stats"]["offsets"].get("e2e-topic-0", 0) >= 1

    # 3. Validate Partition Routing using keys
    res_key_a = await send_produce(
        ports[0], "e2e-topic", {"val": "key_a"}, key="a", acks="1"
    )
    res_key_b = await send_produce(
        ports[0], "e2e-topic", {"val": "key_b"}, key="b", acks="1"
    )
    assert res_key_a["status"] == "ok"
    assert res_key_b["status"] == "ok"

    # 4. Consumer Group Workload Sharing
    reader_c1, writer_c1 = await asyncio.open_connection("127.0.0.1", ports[0])
    req_c1 = {
        "action": "consume",
        "topic": "e2e-topic",
        "group_id": "group-e2e",
        "consumer_id": "c-1",
    }
    writer_c1.write((json.dumps(req_c1) + "\n").encode("utf-8"))
    await writer_c1.drain()

    # Sleep to let consumer 1 register and settle
    await asyncio.sleep(0.2)

    reader_c2, writer_c2 = await asyncio.open_connection("127.0.0.1", ports[0])
    req_c2 = {
        "action": "consume",
        "topic": "e2e-topic",
        "group_id": "group-e2e",
        "consumer_id": "c-2",
    }
    writer_c2.write((json.dumps(req_c2) + "\n").encode("utf-8"))
    await writer_c2.drain()

    # Sleep to complete group rebalancing
    await asyncio.sleep(0.2)

    # Produce another message to trigger streaming consume
    await send_produce(ports[0], "e2e-topic", {"val": "streaming-msg"}, acks="1")

    # Read messages with a timeout
    try:
        line1 = await asyncio.wait_for(reader_c1.readline(), timeout=1.0)
    except asyncio.TimeoutError:
        line1 = b""

    try:
        line2 = await asyncio.wait_for(reader_c2.readline(), timeout=1.0)
    except asyncio.TimeoutError:
        line2 = b""

    assert line1 or line2

    # Clean up consumers
    writer_c1.close()
    await writer_c1.wait_closed()
    writer_c2.close()
    await writer_c2.wait_closed()

    # 5. Validate Leader Failover
    cluster["processes"][ports[0]].terminate()
    cluster["processes"][ports[0]].wait()

    # Wait for heartbeat timeout & election
    await asyncio.sleep(1.5)

    status1_failover = await query_cluster_status(ports[1])
    status2_failover = await query_cluster_status(ports[2])
    assert status1_failover is not None
    assert (
        status2_failover is not None and status2_failover["stats"]["role"] == "leader"
    )

    # 6. Checksum Validation & Corrupted Log Recovery
    # Stop follower 1 (ports[1])
    cluster["processes"][ports[1]].terminate()
    cluster["processes"][ports[1]].wait()

    # Manually corrupt the log file for follower 1 (ports[1])
    partition_file = Path(f"./logs-{ports[1]}/e2e-topic-0.jsonl")
    if partition_file.exists():
        with open(partition_file, "r") as f:
            lines = f.readlines()
        if lines:
            # Corrupt the last line by inserting invalid json
            lines[-1] = "{invalid_json_garbage_data}\n"
            with open(partition_file, "w") as f:
                f.writelines(lines)

    # Restart ports[1] as follower
    cluster["start_broker"](ports[1], "follower", ports[2])
    await asyncio.sleep(1.5)

    # Verify follower 1 recovered and re-established contact
    status1_recovered = await query_cluster_status(ports[1])
    assert status1_recovered is not None
    assert status1_recovered["stats"]["role"] == "follower"
