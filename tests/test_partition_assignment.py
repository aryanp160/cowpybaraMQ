import pytest
import json
import asyncio
from internal.groups import GroupManager


@pytest.mark.unit
def test_round_robin_division_of_partitions(tmp_path):
    """Test round-robin partition assignments for various member counts."""
    filepath = tmp_path / "group_offsets.json"

    # 1. 3 partitions, 3 consumers -> 1 partition each
    manager3 = GroupManager(filepath=filepath, num_partitions=3)

    manager3.register_consumer("g1", "c-a", "topic")
    manager3.register_consumer("g1", "c-b", "topic")
    manager3.register_consumer("g1", "c-c", "topic")

    assign_a = manager3.get_assignment("g1", "c-a", "topic")
    assign_b = manager3.get_assignment("g1", "c-b", "topic")
    assign_c = manager3.get_assignment("g1", "c-c", "topic")

    assert len(assign_a) == 1
    assert len(assign_b) == 1
    assert len(assign_c) == 1

    # Verify no duplicate assignments
    all_assigned = set(assign_a + assign_b + assign_c)
    assert len(all_assigned) == 3

    # 2. 3 partitions, 2 consumers -> 2 partitions for first, 1 for second
    manager2 = GroupManager(filepath=filepath, num_partitions=3)

    manager2.register_consumer("g2", "c-a", "topic")
    manager2.register_consumer("g2", "c-b", "topic")

    assign_a2 = manager2.get_assignment("g2", "c-a", "topic")
    assign_b2 = manager2.get_assignment("g2", "c-b", "topic")

    assert len(assign_a2) == 2
    assert len(assign_b2) == 1
    assert len(set(assign_a2 + assign_b2)) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rebalance_on_disconnect(temp_broker_server):
    """Test rebalance reassignment of partitions when a consumer disconnects."""
    host, port, _, broker, _ = temp_broker_server

    # Set broker's GroupManager partitions count to 3
    broker.group_manager.num_partitions = 3

    # Connect consumer A
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

    # Connect consumer B
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

    # Verify partition assignments
    # Consumer A gets partition 0 and 2. Consumer B gets partition 1.
    assignments = broker.group_manager.assignments.get("group-1", {}).get("orders", {})
    assert len(assignments.get("c-a", [])) == 2
    assert len(assignments.get("c-b", [])) == 1

    # Disconnect Consumer A (owns partition 0 and 2)
    writer_a.close()
    await writer_a.wait_closed()

    # Wait for server to process disconnect and trigger rebalance
    await asyncio.sleep(0.15)

    # Now Consumer B should own all 3 partitions
    new_assignments = broker.group_manager.assignments.get("group-1", {}).get(
        "orders", {}
    )
    assert len(new_assignments.get("c-b", [])) == 3

    writer_b.close()
    await writer_b.wait_closed()
