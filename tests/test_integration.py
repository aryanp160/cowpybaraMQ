import pytest
import asyncio
import json

@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_producers_consumers(temp_broker_server):
    """
    End-to-end integration test:
    Connects multiple producers and consumers to a live broker.
    Verifies that all messages are properly stored, ordered, and successfully
    delivered to all listening consumers without race conditions.
    """
    host, port, storage, _, _ = temp_broker_server
    topic = "integration_test"
    
    # Connect 2 consumers
    reader_c1, writer_c1 = await asyncio.open_connection(host, port)
    reader_c2, writer_c2 = await asyncio.open_connection(host, port)
    
    # Subscribe both consumers from offset 0
    sub_req = json.dumps({"action": "consume", "topic": topic, "offset": 0}) + "\n"
    writer_c1.write(sub_req.encode("utf-8"))
    writer_c2.write(sub_req.encode("utf-8"))
    await writer_c1.drain()
    await writer_c2.drain()
    
    # Spin up 2 concurrent producers to blast 5 messages each
    async def produce_messages(producer_id):
        reader_p, writer_p = await asyncio.open_connection(host, port)
        for i in range(5):
            req = json.dumps({"action": "produce", "topic": topic, "payload": {"pid": producer_id, "idx": i}}) + "\n"
            writer_p.write(req.encode("utf-8"))
            await writer_p.drain()
            await reader_p.readline()
        writer_p.close()
        await writer_p.wait_closed()
        
    await asyncio.gather(
        produce_messages(1),
        produce_messages(2)
    )
    
    # Ensure consumers received all 10 messages (5 from each producer)
    async def consume_messages(reader):
        received = []
        for _ in range(10):
            line = await reader.readline()
            received.append(json.loads(line.decode("utf-8").strip()))
        return received
        
    c1_msgs, c2_msgs = await asyncio.gather(
        consume_messages(reader_c1),
        consume_messages(reader_c2)
    )
    
    # Disconnect consumers
    writer_c1.close()
    writer_c2.close()
    await writer_c1.wait_closed()
    await writer_c2.wait_closed()
    
    # Verify delivery logic
    assert len(c1_msgs) == 10
    assert len(c2_msgs) == 10
    
    # Verify broadcast parity (both got same messages in same order)
    assert c1_msgs == c2_msgs
    
    # Verify offset tracking
    for i in range(10):
        assert c1_msgs[i]["offset"] == i
        
    # Verify physical storage layer writes
    stored = storage.read_all(topic)
    assert len(stored) == 10
