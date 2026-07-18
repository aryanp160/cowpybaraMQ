import asyncio
import json
import socket
import pytest
from internal.storage import Storage
from internal.offsets import OffsetManager
from internal.broker import Broker
from internal.networking import Server


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.asyncio
async def test_unexpected_crash_recovery(tmp_path):
    log_dir = tmp_path / "logs"
    offsets_file = tmp_path / "offsets.json"

    port = get_free_port()
    host = "127.0.0.1"

    # Start first broker
    storage = Storage(log_dir=log_dir)
    offset_manager = OffsetManager(filepath=offsets_file)
    broker = Broker(storage=storage, offset_manager=offset_manager)
    server = Server(host, port, broker)
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.1)

    # 1. Produce 2 messages
    reader, writer = await asyncio.open_connection(host, port)
    for i in range(2):
        prod_req = {
            "action": "produce",
            "topic": "recovery-topic",
            "payload": {"data": f"msg{i}"},
            "acks": "1",
        }
        writer.write((json.dumps(prod_req) + "\n").encode())
        await writer.drain()
        resp = await reader.readline()
        assert json.loads(resp.decode().strip())["status"] == "ok"

    # 2. Consume 1 message to commit offset 1
    cons_req = {
        "action": "consume",
        "topic": "recovery-topic",
        "offset": 0,
        "consumer_id": "c1",
    }
    writer.write((json.dumps(cons_req) + "\n").encode())
    await writer.drain()
    cons_resp = await reader.readline()
    assert (
        json.loads(cons_resp.decode().strip())["status"] == "ok"
    )  # Offset updated to 1
    writer.close()

    # 3. Simulate unexpected crash by stopping the process/tasks instantly
    # We do NOT run clean broker shutdown so we can simulate raw filesystem states
    await server.stop()
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

    # 4. Inject corrupted and truncated writes directly to the partition file to simulate interrupted writes
    partition_file = log_dir / "recovery-topic-0.jsonl"
    with open(partition_file, "a", encoding="utf-8") as f:
        f.write('{"offset": 2, "message": {"data": "corrupt"}, "checksum": 123}\n')
        f.write('{"offset": 3, "message": {"data": "trunc"\n')

    # 5. Start broker again (recovery mode)
    port2 = get_free_port()
    storage2 = Storage(log_dir=log_dir)
    offset_manager2 = OffsetManager(filepath=offsets_file)
    broker2 = Broker(storage=storage2, offset_manager=offset_manager2)
    server2 = Server(host, port2, broker2)
    server_task2 = asyncio.create_task(server2.start())
    await asyncio.sleep(0.1)

    # 6. Verify that it recovered up to offset 2 (0 and 1 are valid, 2 is corrupt, 3 is truncated)
    assert broker2.storage.get_partition("recovery-topic", 0).next_offset == 2

    # Verify offset restored
    assert broker2.offset_manager.get_offset("c1", "recovery-topic") == 2

    # Verify we can resume producing starting at offset 2
    reader2, writer2 = await asyncio.open_connection(host, port2)
    prod_req = {
        "action": "produce",
        "topic": "recovery-topic",
        "payload": {"data": "recovered-msg"},
        "acks": "1",
    }
    writer2.write((json.dumps(prod_req) + "\n").encode())
    await writer2.drain()
    resp2 = await reader2.readline()
    resp_data = json.loads(resp2.decode().strip())
    assert resp_data["status"] == "ok"
    assert resp_data["offset"] == 2

    # Clean shutdown of recovered broker
    await broker2.shutdown()
    await server2.stop()
    server_task2.cancel()
    try:
        await server_task2
    except asyncio.CancelledError:
        pass
