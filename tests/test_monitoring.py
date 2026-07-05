import pytest
import json
import asyncio


@pytest.mark.integration
@pytest.mark.asyncio
async def test_monitoring_integration_full_flow(temp_broker_server):
    """Test monitoring restarts, offset recovery, and status fetching."""
    host, port, storage, broker, _ = temp_broker_server

    # Set partition count to 3
    broker.group_manager.num_partitions = 3
    broker.storage.num_partitions = 3

    # 1. Publish messages with keys to test partition routing
    await broker.publish("orders", {"item": "A"}, key="key1")
    await broker.publish("orders", {"item": "B"}, key="key2")

    # 2. Status check on initial publishes
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(json.dumps({"action": "status"}).encode() + b"\n")
    await writer.drain()
    resp = await reader.readline()
    writer.close()
    await writer.wait_closed()

    status_data = json.loads(resp.decode().strip())
    assert status_data["status"] == "ok"
    stats = status_data["stats"]
    assert stats["total_messages"] == 2
    assert "orders" in stats["topics"]
    assert stats["topics"]["orders"] == 3

    # 3. Connect active group consumer
    reader_c, writer_c = await asyncio.open_connection(host, port)
    req_c = (
        json.dumps(
            {
                "action": "consume",
                "topic": "orders",
                "group_id": "analytics-group",
                "consumer_id": "c-1",
            }
        )
        + "\n"
    )
    writer_c.write(req_c.encode())
    await writer_c.drain()

    # Allow time for registration and consumption
    await asyncio.sleep(0.1)

    # 4. Status check with connected consumer and active assignments
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(json.dumps({"action": "status"}).encode() + b"\n")
    await writer.drain()
    resp = await reader.readline()
    writer.close()
    await writer.wait_closed()

    stats2 = json.loads(resp.decode().strip())["stats"]
    assert "analytics-group" in stats2["consumers"]
    assert "c-1" in stats2["consumers"]
    assert "analytics-group" in stats2["partition_ownership"]
    assert len(stats2["partition_ownership"]["analytics-group"]["orders"]["c-1"]) == 3

    # Close consumer connection
    writer_c.close()
    await writer_c.wait_closed()
