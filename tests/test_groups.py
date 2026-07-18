import asyncio
import json

import pytest

from internal.groups import GroupManager, TopicPartition


@pytest.mark.unit
def test_group_manager_rebalance(tmp_path):
    """Test registering, partition assignment, and rebalancing of consumer groups."""
    filepath = tmp_path / "group_offsets.json"
    manager = GroupManager(filepath=filepath)

    tp = TopicPartition("orders", 0)

    # 1. First consumer registers
    assignments_1 = manager.register_consumer("analytics-group", "consumer-1", "orders")
    assert tp in assignments_1

    # 2. Second consumer registers -> rebalance should assign partition 0 to only one
    manager.register_consumer("analytics-group", "consumer-2", "orders")

    assignment_1 = manager.get_assignment("analytics-group", "consumer-1", "orders")
    assignment_2 = manager.get_assignment("analytics-group", "consumer-2", "orders")

    # Only one consumer can hold partition 0
    assert (tp in assignment_1) != (tp in assignment_2)

    # 3. Deregister holder -> rebalance assigns it to remaining consumer
    holder = "consumer-1" if tp in assignment_1 else "consumer-2"
    non_holder = "consumer-2" if holder == "consumer-1" else "consumer-1"

    manager.deregister_consumer("analytics-group", holder, "orders")
    assert tp in manager.get_assignment("analytics-group", non_holder, "orders")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_groups_workload_sharing(temp_broker_server):
    """Test group consumers share work, while different groups receive all."""
    host, port, storage, broker, _ = temp_broker_server

    # Connect group 1 consumer A
    reader_a, writer_a = await asyncio.open_connection(host, port)
    req_a = (
        json.dumps(
            {
                "action": "consume",
                "topic": "orders",
                "group_id": "group-1",
                "consumer_id": "c-a",
            }
        )
        + "\n"
    )
    writer_a.write(req_a.encode())
    await writer_a.drain()
    await asyncio.sleep(0.05)

    # Connect group 1 consumer B
    reader_b, writer_b = await asyncio.open_connection(host, port)
    req_b = (
        json.dumps(
            {
                "action": "consume",
                "topic": "orders",
                "group_id": "group-1",
                "consumer_id": "c-b",
            }
        )
        + "\n"
    )
    writer_b.write(req_b.encode())
    await writer_b.drain()

    # Connect group 2 consumer C (different group)
    reader_c, writer_c = await asyncio.open_connection(host, port)
    req_c = (
        json.dumps(
            {
                "action": "consume",
                "topic": "orders",
                "group_id": "group-2",
                "consumer_id": "c-c",
            }
        )
        + "\n"
    )
    writer_c.write(req_c.encode())
    await writer_c.drain()
    await asyncio.sleep(0.05)

    # Publish 4 messages
    await broker.publish("orders", {"msg": "1"})
    await broker.publish("orders", {"msg": "2"})
    await broker.publish("orders", {"msg": "3"})
    await broker.publish("orders", {"msg": "4"})

    # Determine who holds partition 0 in group-1
    tp = TopicPartition("orders", 0)
    assignments = broker.group_manager.assignments.get("group-1", {}).get("orders", {})
    holder_cid = "c-a" if tp in assignments.get("c-a", []) else "c-b"

    holder_reader = reader_a if holder_cid == "c-a" else reader_b
    non_holder_writer = writer_b if holder_cid == "c-a" else writer_a

    # Holder should receive all 4 messages
    received_holder = []
    for _ in range(4):
        line = await holder_reader.readline()
        received_holder.append(json.loads(line.decode().strip()))

    assert len(received_holder) == 4
    for idx, msg in enumerate(received_holder):
        assert msg["offset"] == idx

    # Non-holder receives nothing, can close cleanly
    non_holder_writer.close()
    await non_holder_writer.wait_closed()

    # Different group consumer C should receive all 4 messages
    received_c = []
    for _ in range(4):
        line = await reader_c.readline()
        received_c.append(json.loads(line.decode().strip()))

    assert len(received_c) == 4

    writer_a.close()
    writer_c.close()
    await writer_a.wait_closed()
    await writer_c.wait_closed()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_groups_rebalance_on_disconnect(temp_broker_server):
    """Test disconnecting holder triggers a rebalance and reassignment."""
    host, port, storage, broker, _ = temp_broker_server

    # Connect consumer A in group-1
    reader_a, writer_a = await asyncio.open_connection(host, port)
    req_a = (
        json.dumps(
            {
                "action": "consume",
                "topic": "orders",
                "group_id": "group-1",
                "consumer_id": "c-a",
            }
        )
        + "\n"
    )
    writer_a.write(req_a.encode())
    await writer_a.drain()
    await asyncio.sleep(0.05)

    # Connect consumer B in group-1
    reader_b, writer_b = await asyncio.open_connection(host, port)
    req_b = (
        json.dumps(
            {
                "action": "consume",
                "topic": "orders",
                "group_id": "group-1",
                "consumer_id": "c-b",
            }
        )
        + "\n"
    )
    writer_b.write(req_b.encode())
    await writer_b.drain()
    await asyncio.sleep(0.05)

    # Determine who holds partition 0
    tp = TopicPartition("orders", 0)
    assignments = broker.group_manager.assignments.get("group-1", {}).get("orders", {})
    holder_cid = "c-a" if tp in assignments.get("c-a", []) else "c-b"
    non_holder_cid = "c-b" if holder_cid == "c-a" else "c-a"

    holder_writer = writer_a if holder_cid == "c-a" else writer_b
    non_holder_reader = reader_b if holder_cid == "c-a" else reader_a
    non_holder_writer = writer_b if holder_cid == "c-a" else writer_a

    # Disconnect the partition holder
    holder_writer.close()
    await holder_writer.wait_closed()

    # Allow time for server to detect disconnect and rebalance
    await asyncio.sleep(0.15)

    # Now non_holder should have partition 0 assigned
    new_assignments = broker.group_manager.assignments.get("group-1", {}).get(
        "orders", {}
    )
    assert tp in new_assignments.get(non_holder_cid, [])

    # Publish message D
    await broker.publish("orders", {"msg": "D"})

    # Non-holder should now receive D (since it got reassigned partition 0)
    line = await non_holder_reader.readline()
    msg = json.loads(line.decode().strip())
    assert msg["payload"]["msg"] == "D"

    non_holder_writer.close()
    await non_holder_writer.wait_closed()
