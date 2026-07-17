import asyncio
import json
import pytest
import socket
from internal.storage import Storage
from internal.offsets import OffsetManager
from internal.groups import GroupManager
from internal.broker import Broker
from internal.networking import Server


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.asyncio
async def test_graceful_shutdown_data_survival(tmp_path):
    # Setup paths
    log_dir = tmp_path / "logs"
    offsets_file = tmp_path / "offsets.json"
    group_offsets_file = tmp_path / "group_offsets.json"

    # Start first instance
    port = get_free_port()
    host = "127.0.0.1"

    storage = Storage(log_dir=log_dir)
    offset_manager = OffsetManager(filepath=offsets_file)
    group_manager = GroupManager(filepath=group_offsets_file)
    broker = Broker(
        storage=storage, offset_manager=offset_manager, group_manager=group_manager
    )
    server = Server(host, port, broker)

    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.1)

    # 1. Produce some messages
    reader, writer = await asyncio.open_connection(host, port)
    prod_req = {
        "action": "produce",
        "topic": "shutdown-topic",
        "payload": {"msg": "hello-before-shutdown"},
        "acks": "1",
    }
    writer.write((json.dumps(prod_req) + "\n").encode("utf-8"))
    await writer.drain()
    resp = await reader.readline()
    resp_data = json.loads(resp.decode("utf-8").strip())
    assert resp_data["status"] == "ok"
    assert resp_data["offset"] == 0

    # 2. Consume and update consumer offset
    cons_req = {
        "action": "consume",
        "topic": "shutdown-topic",
        "offset": 0,
        "consumer_id": "test-consumer-1",
    }
    writer.write((json.dumps(cons_req) + "\n").encode("utf-8"))
    await writer.drain()
    cons_resp = await reader.readline()
    cons_data = json.loads(cons_resp.decode("utf-8").strip())
    assert cons_data["status"] == "ok"
    # Unified response uses both payload and message keys
    assert (cons_data.get("payload") or cons_data.get("message"))[
        "msg"
    ] == "hello-before-shutdown"

    writer.close()

    # 3. Shutdown Broker gracefully
    await broker.shutdown()
    await server.stop()
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

    # Assert persistence files exist and contain correct data
    assert offsets_file.exists()
    assert (log_dir / "shutdown-topic-0.jsonl").exists()

    # Verify offset persisted
    with open(offsets_file, "r", encoding="utf-8") as f:
        offsets = json.load(f)
        assert offsets["test-consumer-1"]["shutdown-topic"] == 1

    # 4. Start second broker instance pointing to same storage
    port2 = get_free_port()
    storage2 = Storage(log_dir=log_dir)
    offset_manager2 = OffsetManager(filepath=offsets_file)
    group_manager2 = GroupManager(filepath=group_offsets_file)
    broker2 = Broker(
        storage=storage2, offset_manager=offset_manager2, group_manager=group_manager2
    )
    server2 = Server(host, port2, broker2)

    server_task2 = asyncio.create_task(server2.start())
    await asyncio.sleep(0.1)

    # Verify consumer offset was loaded correctly and starts at 1
    assert broker2.offset_manager.get_offset("test-consumer-1", "shutdown-topic") == 1

    # Produce second message
    reader2, writer2 = await asyncio.open_connection(host, port2)
    prod_req2 = {
        "action": "produce",
        "topic": "shutdown-topic",
        "payload": {"msg": "hello-after-restart"},
        "acks": "1",
    }
    writer2.write((json.dumps(prod_req2) + "\n").encode("utf-8"))
    await writer2.drain()
    resp2 = await reader2.readline()
    resp_data2 = json.loads(resp2.decode("utf-8").strip())
    assert resp_data2["status"] == "ok"
    assert resp_data2["offset"] == 1

    # Cleanup second instance
    await broker2.shutdown()
    await server2.stop()
    server_task2.cancel()
    try:
        await server_task2
    except asyncio.CancelledError:
        pass
