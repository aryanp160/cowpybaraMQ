import socket
import pytest
import json
import asyncio
from internal.storage import Storage
from internal.broker import Broker
from internal.networking import Server


def get_free_port():
    """Dynamically finds a free port to avoid conflicts during testing."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multi_broker_replication(tmp_path):
    # Ports
    leader_port = get_free_port()
    follower_a_port = get_free_port()
    follower_b_port = get_free_port()
    host = "127.0.0.1"

    # Log directories
    leader_dir = tmp_path / "leader"
    follower_a_dir = tmp_path / "follower_a"
    follower_b_dir = tmp_path / "follower_b"

    # 1. Start Leader
    leader_storage = Storage(log_dir=leader_dir)
    leader_broker = Broker(storage=leader_storage, role="leader")
    leader_server = Server(host, leader_port, leader_broker)
    leader_task = asyncio.create_task(leader_server.start())
    await asyncio.sleep(0.05)

    # 2. Start Follower A
    fa_storage = Storage(log_dir=follower_a_dir)
    fa_broker = Broker(
        storage=fa_storage,
        role="follower",
        leader_host=host,
        leader_port=leader_port,
    )
    fa_server = Server(host, follower_a_port, fa_broker)
    fa_task = asyncio.create_task(fa_server.start())

    # 3. Start Follower B
    fb_storage = Storage(log_dir=follower_b_dir)
    fb_broker = Broker(
        storage=fb_storage,
        role="follower",
        leader_host=host,
        leader_port=leader_port,
    )
    fb_server = Server(host, follower_b_port, fb_broker)
    fb_task = asyncio.create_task(fb_server.start())

    # Allow followers to connect and register
    await asyncio.sleep(0.2)

    # 4. Produce to Leader
    reader, writer = await asyncio.open_connection(host, leader_port)
    req = {
        "action": "produce",
        "topic": "replicated-topic",
        "payload": {"val": 42},
    }
    writer.write((json.dumps(req) + "\n").encode())
    await writer.drain()
    resp = await reader.readline()
    writer.close()
    await writer.wait_closed()

    # Verify leader response
    resp_data = json.loads(resp.decode().strip())
    assert resp_data["status"] == "ok"
    assert resp_data["partition"] == 0
    assert resp_data["offset"] == 0

    # 5. Check follower logs with retry loop
    messages_a = []
    messages_b = []
    for _ in range(30):
        messages_a = fa_storage.read_all("replicated-topic", 0)
        messages_b = fb_storage.read_all("replicated-topic", 0)
        if len(messages_a) == 1 and len(messages_b) == 1:
            break
        await asyncio.sleep(0.1)

    assert len(messages_a) == 1
    assert messages_a[0]["message"]["val"] == 42
    assert messages_a[0]["offset"] == 0

    assert len(messages_b) == 1
    assert messages_b[0]["message"]["val"] == 42
    assert messages_b[0]["offset"] == 0

    # 6. Verify followers reject Produce requests
    reader_fa, writer_fa = await asyncio.open_connection(host, follower_a_port)
    req_produce = {
        "action": "produce",
        "topic": "replicated-topic",
        "payload": {"val": 99},
    }
    writer_fa.write((json.dumps(req_produce) + "\n").encode())
    await writer_fa.drain()
    resp_fa = await reader_fa.readline()
    writer_fa.close()
    await writer_fa.wait_closed()

    resp_fa_data = json.loads(resp_fa.decode().strip())
    assert resp_fa_data["status"] == "error"
    assert "Not a leader" in resp_fa_data["message"]

    # 7. Verify followers serve Consume requests
    reader_c, writer_c = await asyncio.open_connection(host, follower_a_port)
    req_consume = {
        "action": "consume",
        "topic": "replicated-topic",
        "offset": 0,
    }
    writer_c.write((json.dumps(req_consume) + "\n").encode())
    await writer_c.drain()
    resp_c = await reader_c.readline()
    writer_c.close()
    await writer_c.wait_closed()

    resp_c_data = json.loads(resp_c.decode().strip())
    assert resp_c_data["status"] == "ok"
    assert resp_c_data["payload"]["val"] == 42

    # Clean up
    await leader_server.stop()
    await fa_server.stop()
    await fb_server.stop()

    await leader_broker.replication_manager.stop()
    await fa_broker.replication_manager.stop()
    await fb_broker.replication_manager.stop()

    leader_task.cancel()
    fa_task.cancel()
    fb_task.cancel()
    try:
        await leader_task
        await fa_task
        await fb_task
    except asyncio.CancelledError:
        pass


@pytest.mark.unit
def test_replicate_duplicate_prevention(tmp_path):
    storage = Storage(log_dir=tmp_path)
    part = storage.get_partition("test-dup", 0)

    # First append works
    offset1 = part.append({"val": 1}, offset=0)
    assert offset1 == 0

    # Append again with same offset: duplicate write should not result in multiple lines
    offset2 = part.append({"val": 1}, offset=0)
    assert offset2 == 0

    # Read back to verify only 1 message exists
    msgs = part.read_all()
    assert len(msgs) == 1
    assert msgs[0]["offset"] == 0
    assert msgs[0]["message"]["val"] == 1
