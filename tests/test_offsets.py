import asyncio
import json
import threading

import pytest

from internal.offsets import OffsetManager


@pytest.mark.unit
def test_offset_manager_basic(tmp_path):
    """Test initial state, updating, and persistence of offsets."""
    filepath = tmp_path / "offsets.json"
    manager = OffsetManager(filepath=filepath)

    # Initial offset starts at 0
    assert manager.get_offset("consumer-1", "orders") == 0

    # Updating offset works and saves to disk
    manager.update_offset("consumer-1", "orders", 15)
    assert manager.get_offset("consumer-1", "orders") == 15

    # File format check
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert data["consumer-1"]["orders"] == 15

    # Loading offset on start (new instance loads persisted)
    manager2 = OffsetManager(filepath=filepath)
    assert manager2.get_offset("consumer-1", "orders") == 15


@pytest.mark.unit
def test_offset_manager_thread_safety(tmp_path):
    """Test concurrent thread-safe reads and writes to OffsetManager."""
    filepath = tmp_path / "offsets.json"
    manager = OffsetManager(filepath=filepath)

    def worker(worker_id):
        for i in range(50):
            manager.update_offset(f"consumer-{worker_id}", "topic", i)
            val = manager.get_offset(f"consumer-{worker_id}", "topic")
            assert val == i

    threads = []
    for i in range(10):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Verify final values are correct and persistent
    manager2 = OffsetManager(filepath=filepath)
    for i in range(10):
        assert manager2.get_offset(f"consumer-{i}", "topic") == 49


@pytest.mark.integration
@pytest.mark.asyncio
async def test_broker_offset_integration(temp_broker_server):
    """Test that broker correctly updates consumer offsets on message delivery."""
    host, port, storage, broker, _ = temp_broker_server

    print("\n[TEST] Publishing A, B, C...")
    # Publish 3 messages
    await broker.publish("orders", {"item": "A"})
    await broker.publish("orders", {"item": "B"})
    await broker.publish("orders", {"item": "C"})

    print("[TEST] Connecting consumer 1...")
    # Connect consumer 1 with consumer_id
    reader, writer = await asyncio.open_connection(host, port)
    req = (
        json.dumps({"action": "consume", "topic": "orders", "consumer_id": "c1"}) + "\n"
    )
    writer.write(req.encode("utf-8"))
    await writer.drain()

    print("[TEST] Reading 3 messages...")
    # Read 3 messages
    for i in range(3):
        print(f"[TEST] Reading message {i}...")
        line = await reader.readline()
        msg = json.loads(line.decode("utf-8").strip())
        print(f"[TEST] Got message: {msg}")
        assert msg["offset"] == i

    print("[TEST] Closing consumer 1...")
    writer.close()
    await writer.wait_closed()
    await asyncio.sleep(0.1)  # Allow server to process disconnect

    # Verify offset updated to 3 (next message to consume)
    print("[TEST] Verifying offset is 3...")
    assert broker.offset_manager.get_offset("c1", "orders") == 3

    print("[TEST] Publishing D...")
    # Reconnect to verify offset resumes from 3
    await broker.publish("orders", {"item": "D"})

    print("[TEST] Connecting consumer 1 again...")
    reader2, writer2 = await asyncio.open_connection(host, port)
    req2 = (
        json.dumps({"action": "consume", "topic": "orders", "consumer_id": "c1"}) + "\n"
    )
    writer2.write(req2.encode("utf-8"))
    await writer2.drain()

    print("[TEST] Reading message D...")
    line = await reader2.readline()
    msg2 = json.loads(line.decode("utf-8").strip())
    print(f"[TEST] Got message D: {msg2}")
    assert msg2["offset"] == 3
    assert msg2["message"]["item"] == "D"

    print("[TEST] Closing consumer 2...")
    writer2.close()
    await writer2.wait_closed()
    print("[TEST] Done!")
