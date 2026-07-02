import pytest
import asyncio
import json

@pytest.mark.stress
@pytest.mark.asyncio
async def test_high_concurrency(temp_broker_server):
    """
    High concurrency stress test:
    Spawns 10 producers and 10 consumers simultaneously.
    Blasts 1,000 messages across the TCP broker.
    Verifies the broker stays alive, no crashes occur, messages are durably stored,
    and exact sequential ordering is maintained globally.
    """
    host, port, storage, _, _ = temp_broker_server
    topic = "stress_topic"
    
    num_producers = 10
    num_consumers = 10
    msgs_per_producer = 100
    total_messages = num_producers * msgs_per_producer
    
    # 1. Spin up Consumers
    async def run_consumer():
        reader, writer = await asyncio.open_connection(host, port)
        sub_req = json.dumps({"action": "consume", "topic": topic, "offset": 0}) + "\n"
        writer.write(sub_req.encode("utf-8"))
        await writer.drain()
        
        count = 0
        while count < total_messages:
            await reader.readline()
            count += 1
            
        writer.close()
        await writer.wait_closed()
        return count
        
    consumer_tasks = [asyncio.create_task(run_consumer()) for _ in range(num_consumers)]
    
    # Allow consumers to establish connections
    await asyncio.sleep(0.5)
    
    # 2. Spin up Producers and blast messages
    async def run_producer(pid):
        reader, writer = await asyncio.open_connection(host, port)
        for i in range(msgs_per_producer):
            req = json.dumps({"action": "produce", "topic": topic, "payload": {"pid": pid, "msg": i}}) + "\n"
            writer.write(req.encode("utf-8"))
            await writer.drain()
            # Wait for ACK to prevent overwhelming socket buffers instantly
            await reader.readline()
            
        writer.close()
        await writer.wait_closed()
        
    producer_tasks = [asyncio.create_task(run_producer(i)) for i in range(num_producers)]
    
    # Wait for all producers to finish pushing
    await asyncio.gather(*producer_tasks)
    
    # Wait for all consumers to finish receiving the full payload
    consumer_results = await asyncio.gather(*consumer_tasks)
    
    # 3. Verification
    for count in consumer_results:
        assert count == total_messages
        
    # Check physical log integrity
    stored = storage.read_all(topic)
    assert len(stored) == total_messages
    
    # Validate strictly sequential, lock-protected offsets
    for idx, msg in enumerate(stored):
        assert msg["offset"] == idx
