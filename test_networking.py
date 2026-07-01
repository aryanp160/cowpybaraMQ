import asyncio
import json
import logging
from pathlib import Path
import shutil
from internal.networking import Server
from internal.storage import Storage
from internal.broker import Broker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def test_client():
    # 1. Produce some messages first
    reader_p1, writer_p1 = await asyncio.open_connection("127.0.0.1", 9099)
    
    print("\n--- Testing Produce ---")
    produce_req = {"action": "produce", "topic": "test_broker", "payload": {"msg": "hello 1"}}
    writer_p1.write((json.dumps(produce_req) + "\n").encode("utf-8"))
    await writer_p1.drain()
    
    response1 = await reader_p1.readline()
    print(f"Produce Response 1: {response1.decode('utf-8').strip()}")

    produce_req2 = {"action": "produce", "topic": "test_broker", "payload": {"msg": "hello 2"}}
    writer_p1.write((json.dumps(produce_req2) + "\n").encode("utf-8"))
    await writer_p1.drain()
    
    response2 = await reader_p1.readline()
    print(f"Produce Response 2: {response2.decode('utf-8').strip()}")
    
    writer_p1.close()
    await writer_p1.wait_closed()

    # 2. Subscribe (Should receive the 2 historical messages immediately)
    reader_c, writer_c = await asyncio.open_connection("127.0.0.1", 9099)
    print("\n--- Testing Consume (Historical) ---")
    consume_req = {"action": "consume", "topic": "test_broker", "offset": 0}
    writer_c.write((json.dumps(consume_req) + "\n").encode("utf-8"))
    await writer_c.drain()
    
    resp_c1 = await reader_c.readline()
    print(f"Historical 1: {resp_c1.decode('utf-8').strip()}")
    resp_c2 = await reader_c.readline()
    print(f"Historical 2: {resp_c2.decode('utf-8').strip()}")

    # 3. Produce a new live message while consumer is active
    print("\n--- Testing Live Stream ---")
    reader_p2, writer_p2 = await asyncio.open_connection("127.0.0.1", 9099)
    produce_req3 = {"action": "produce", "topic": "test_broker", "payload": {"msg": "live update"}}
    writer_p2.write((json.dumps(produce_req3) + "\n").encode("utf-8"))
    await writer_p2.drain()
    await reader_p2.readline()
    writer_p2.close()
    await writer_p2.wait_closed()

    # The consumer should receive the live message
    resp_c3 = await reader_c.readline()
    print(f"Live Message: {resp_c3.decode('utf-8').strip()}")
    
    writer_c.close()
    await writer_c.wait_closed()


async def main():
    log_dir = Path("./logs_test")
    if log_dir.exists():
        shutil.rmtree(log_dir)
        
    storage = Storage(log_dir=log_dir)
    broker = Broker(storage=storage)
    server = Server("127.0.0.1", 9099, broker)
    
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)
    
    try:
        await test_client()
    finally:
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass
        if log_dir.exists():
            shutil.rmtree(log_dir)

if __name__ == "__main__":
    asyncio.run(main())
