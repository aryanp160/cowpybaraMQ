import asyncio
import json
import socket
import time
import zlib

import pytest

from internal.broker import Broker
from internal.networking import Server
from internal.offsets import OffsetManager
from internal.storage import Storage

BENCHMARK_RESULTS = []


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="module", autouse=True)
def benchmark_report():
    yield
    report_path = "recovery_benchmark_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== COWPYBARAMQ RECOVERY BENCHMARK REPORT ===\n\n")
        for res in BENCHMARK_RESULTS:
            f.write(f"Test Scenario: {res['name']}\n")
            f.write(f"  Startup Recovery Time: {res['recovery_time'] * 1000:.3f} ms\n")
            f.write(f"  Status: {res['status']}\n")
            f.write(f"  Details: {res['details']}\n\n")


@pytest.mark.asyncio
async def test_graceful_vs_forced_shutdown(tmp_path):
    log_dir = tmp_path / "logs"
    offsets_file = tmp_path / "offsets.json"

    port = get_free_port()
    host = "127.0.0.1"
    storage = Storage(log_dir=log_dir)
    offset_manager = OffsetManager(filepath=offsets_file)
    broker = Broker(storage=storage, offset_manager=offset_manager)
    server = Server(host, port, broker)
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.05)

    try:
        reader, writer = await asyncio.open_connection(host, port)
        prod_req = {
            "action": "produce",
            "topic": "graceful-topic",
            "payload": {"val": 100},
            "acks": "1",
        }
        writer.write((json.dumps(prod_req) + "\n").encode())
        await writer.drain()
        await reader.readline()
        writer.close()
    finally:
        t0 = time.perf_counter()
        await broker.shutdown()
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass
        graceful_shutdown_time = time.perf_counter() - t0

    port2 = get_free_port()
    storage2 = Storage(log_dir=log_dir)
    offset_manager2 = OffsetManager(filepath=offsets_file)
    broker2 = Broker(storage=storage2, offset_manager=offset_manager2)
    server2 = Server(host, port2, broker2)
    server_task2 = asyncio.create_task(server2.start())
    await asyncio.sleep(0.05)

    try:
        reader2, writer2 = await asyncio.open_connection(host, port2)
        prod_req2 = {
            "action": "produce",
            "topic": "forced-topic",
            "payload": {"val": 200},
            "acks": "1",
        }
        writer2.write((json.dumps(prod_req2) + "\n").encode())
        await writer2.drain()
        await reader2.readline()
        writer2.close()
    finally:
        await server2.stop()
        server_task2.cancel()
        try:
            await server_task2
        except asyncio.CancelledError:
            pass

    t_start = time.perf_counter()
    storage3 = Storage(log_dir=log_dir)
    offset_manager3 = OffsetManager(filepath=offsets_file)
    broker3 = Broker(storage=storage3, offset_manager=offset_manager3)
    recovery_time = time.perf_counter() - t_start

    assert broker3.storage.get_partition("graceful-topic", 0).next_offset == 1
    assert broker3.storage.get_partition("forced-topic", 0).next_offset == 1

    BENCHMARK_RESULTS.append(
        {
            "name": "Graceful vs Forced Shutdown Recovery",
            "recovery_time": recovery_time,
            "status": "PASSED",
            "details": f"Graceful shutdown sequence: {graceful_shutdown_time*1000:.2f}ms. Replay recovered all topics.",
        }
    )


@pytest.mark.asyncio
async def test_repeated_crash_recovery_cycles(tmp_path):
    log_dir = tmp_path / "logs"
    offsets_file = tmp_path / "offsets.json"
    host = "127.0.0.1"

    total_recovery_time = 0.0
    for cycle in range(5):
        port = get_free_port()
        storage = Storage(log_dir=log_dir)
        offset_manager = OffsetManager(filepath=offsets_file)

        t_start = time.perf_counter()
        broker = Broker(storage=storage, offset_manager=offset_manager)
        total_recovery_time += time.perf_counter() - t_start

        server = Server(host, port, broker)
        server_task = asyncio.create_task(server.start())
        await asyncio.sleep(0.05)

        try:
            reader, writer = await asyncio.open_connection(host, port)
            prod_req = {
                "action": "produce",
                "topic": "cycle-topic",
                "payload": {"cycle": cycle},
                "acks": "1",
            }
            writer.write((json.dumps(prod_req) + "\n").encode())
            await writer.drain()
            await reader.readline()
            writer.close()
        finally:
            await server.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    storage_final = Storage(log_dir=log_dir)
    offset_manager_final = OffsetManager(filepath=offsets_file)
    broker_final = Broker(storage=storage_final, offset_manager=offset_manager_final)
    assert broker_final.storage.get_partition("cycle-topic", 0).next_offset == 5

    BENCHMARK_RESULTS.append(
        {
            "name": "Repeated Crash Cycles (5 iterations)",
            "recovery_time": total_recovery_time / 5,
            "status": "PASSED",
            "details": "Successfully recovered all 5 cycle messages after 5 consecutive crash sequences.",
        }
    )


@pytest.mark.asyncio
async def test_checksum_integrity_and_recovery(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    offsets_file = tmp_path / "offsets.json"

    partition_file = log_dir / "integrity-test-topic-0.jsonl"
    valid = {"offset": 0, "message": {"val": 1}, "checksum": 0}
    valid["checksum"] = zlib.crc32(
        json.dumps(
            {"offset": valid["offset"], "message": valid["message"]}, sort_keys=True
        ).encode("utf-8")
    )

    corrupt = '{"offset": 1, "message": {"val": 2}, "checksum": 99999}'
    truncated = '{"offset": 2, "message": {"val":'

    with open(partition_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(valid) + "\n")
        f.write(corrupt + "\n")
        f.write(truncated + "\n")

    t_start = time.perf_counter()
    storage = Storage(log_dir=log_dir)
    offset_manager = OffsetManager(filepath=offsets_file)
    broker = Broker(storage=storage, offset_manager=offset_manager)
    recovery_time = time.perf_counter() - t_start

    assert broker.storage.get_partition("integrity-test-topic", 0).next_offset == 1

    BENCHMARK_RESULTS.append(
        {
            "name": "Checksum Integrity and Recovery",
            "recovery_time": recovery_time,
            "status": "PASSED",
            "details": "Gracefully skipped 1 corrupted and 1 truncated entry. Recovered 1 valid entry.",
        }
    )


@pytest.mark.asyncio
async def test_replication_cluster_recovery_and_leader_failover(tmp_path):
    log_dir_l = tmp_path / "logs-leader"
    log_dir_f = tmp_path / "logs-follower"

    p1 = get_free_port()
    p2 = get_free_port()
    port_l = min(p1, p2)
    port_f = max(p1, p2)
    host = "127.0.0.1"

    # Define variables to ensure finally block has access
    server_task_l = None
    server_task_f = None
    server_task_l2 = None
    broker_l = None
    broker_f = None
    broker_l2 = None
    server_l = Server("127.0.0.1", port_l, None)
    server_f = Server("127.0.0.1", port_f, None)
    server_l2 = Server("127.0.0.1", port_l, None)

    try:
        print("Starting leader...")
        storage_l = Storage(log_dir=log_dir_l)
        broker_l = Broker(
            storage=storage_l,
            role="leader",
            broker_id=port_l,
            cluster_members=f"127.0.0.1:{port_l},127.0.0.1:{port_f}",
            heartbeat_interval=0.1,
            heartbeat_timeout=0.3,
        )
        server_l.broker = broker_l
        server_task_l = asyncio.create_task(server_l.start())

        print("Starting follower...")
        storage_f = Storage(log_dir=log_dir_f)
        broker_f = Broker(
            storage=storage_f,
            role="follower",
            leader_host=host,
            leader_port=port_l,
            broker_id=port_f,
            cluster_members=f"127.0.0.1:{port_l},127.0.0.1:{port_f}",
            heartbeat_interval=0.1,
            heartbeat_timeout=0.3,
        )
        server_f.broker = broker_f
        server_task_f = asyncio.create_task(server_f.start())
        await asyncio.sleep(0.8)

        print("Producing to leader...")
        reader, writer = await asyncio.open_connection(host, port_l)
        prod_req = {
            "action": "produce",
            "topic": "replica-topic",
            "payload": {"val": 500},
            "acks": "all",
        }
        writer.write((json.dumps(prod_req) + "\n").encode())
        await writer.drain()
        resp = await reader.readline()
        print("Produce resp:", resp)
        assert json.loads(resp.decode().strip())["status"] == "ok"
        writer.close()
        await asyncio.sleep(0.8)

        print("Follower offset check...")
        assert broker_f.storage.get_partition("replica-topic", 0).next_offset == 1

        print("Crashing leader...")
        await server_l.stop()
        await broker_l.shutdown()
        server_task_l.cancel()
        try:
            await server_task_l
        except asyncio.CancelledError:
            pass
        server_task_l = None

        print("Waiting for failover...")
        await asyncio.sleep(1.0)
        print("Follower role check:", broker_f.replication_manager.role)
        assert broker_f.replication_manager.role == "leader"

        print("Recovering leader as follower...")
        t_start = time.perf_counter()
        storage_l2 = Storage(log_dir=log_dir_l)
        broker_l2 = Broker(
            storage=storage_l2,
            role="follower",
            leader_host=host,
            leader_port=port_f,
            broker_id=port_l,
            cluster_members=f"127.0.0.1:{port_l},127.0.0.1:{port_f}",
            heartbeat_interval=0.1,
            heartbeat_timeout=0.3,
        )
        server_l2.broker = broker_l2
        server_task_l2 = asyncio.create_task(server_l2.start())
        await asyncio.sleep(0.8)
        recovery_time = time.perf_counter() - t_start

        print("Producing to new leader...")
        reader2, writer2 = await asyncio.open_connection(host, port_f)
        prod_req2 = {
            "action": "produce",
            "topic": "replica-topic",
            "payload": {"val": 600},
            "acks": "all",
        }
        writer2.write((json.dumps(prod_req2) + "\n").encode())
        await writer2.drain()
        resp2 = await reader2.readline()
        print("New leader produce resp:", resp2)
        assert json.loads(resp2.decode().strip())["status"] == "ok"
        writer2.close()
        await asyncio.sleep(0.8)

        print("Checking replication on recovered node...")
        assert broker_l2.storage.get_partition("replica-topic", 0).next_offset == 2

    finally:
        print("Teardown cleaning up...")
        if server_task_l:
            server_task_l.cancel()
        if server_task_f:
            server_task_f.cancel()
        if server_task_l2:
            server_task_l2.cancel()

        if broker_l:
            await broker_l.shutdown()
        if broker_f:
            await broker_f.shutdown()
        if broker_l2:
            await broker_l2.shutdown()

        await server_l.stop()
        await server_f.stop()
        await server_l2.stop()

    BENCHMARK_RESULTS.append(
        {
            "name": "Replication Cluster Failover and Recovery",
            "recovery_time": recovery_time,
            "status": "PASSED",
            "details": "Successfully handled failover of follower to leader and leader restart as syncing follower.",
        }
    )


@pytest.mark.asyncio
async def test_concurrent_producers_during_recovery(tmp_path):
    log_dir = tmp_path / "logs"
    offsets_file = tmp_path / "offsets.json"
    port = get_free_port()
    host = "127.0.0.1"

    t_start = time.perf_counter()
    storage = Storage(log_dir=log_dir)
    offset_manager = OffsetManager(filepath=offsets_file)
    broker = Broker(storage=storage, offset_manager=offset_manager)
    server = Server(host, port, broker)
    server_task = asyncio.create_task(server.start())
    recovery_time = time.perf_counter() - t_start

    try:

        async def run_producer(val):
            await asyncio.sleep(0.01)
            r, w = await asyncio.open_connection(host, port)
            prod_req = {
                "action": "produce",
                "topic": "concurrent-topic",
                "payload": {"val": val},
                "acks": "1",
            }
            w.write((json.dumps(prod_req) + "\n").encode())
            await w.drain()
            res = await r.readline()
            w.close()
            return json.loads(res.decode().strip())

        results = await asyncio.gather(*(run_producer(i) for i in range(10)))
        for res in results:
            assert res["status"] == "ok"

        assert broker.storage.get_partition("concurrent-topic", 0).next_offset == 10
    finally:
        await broker.shutdown()
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

    BENCHMARK_RESULTS.append(
        {
            "name": "Concurrent Producers during Recovery",
            "recovery_time": recovery_time,
            "status": "PASSED",
            "details": "Spanned 10 concurrent producers immediately upon recovery without request drop.",
        }
    )
