import asyncio
import json
import logging
from internal.networking import Server

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def test_client():
    reader, writer = await asyncio.open_connection("127.0.0.1", 9099)
    
    print("\n--- Testing Produce ---")
    produce_req = {"action": "produce", "topic": "test", "payload": {"msg": "hello"}}
    writer.write((json.dumps(produce_req) + "\n").encode("utf-8"))
    await writer.drain()
    
    response1 = await reader.readline()
    print(f"Produce Response: {response1.decode('utf-8').strip()}")
    
    print("\n--- Testing Consume ---")
    consume_req = {"action": "consume", "topic": "test", "offset": 0}
    writer.write((json.dumps(consume_req) + "\n").encode("utf-8"))
    await writer.drain()
    
    response2 = await reader.readline()
    print(f"Consume Response: {response2.decode('utf-8').strip()}")
    
    print("\n--- Testing Invalid Request ---")
    writer.write(b"this is not json\n")
    await writer.drain()
    
    response3 = await reader.readline()
    print(f"Invalid Response: {response3.decode('utf-8').strip()}")
    
    writer.close()
    await writer.wait_closed()

async def main():
    server = Server("127.0.0.1", 9099)
    
    # Run the server in a background task
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

if __name__ == "__main__":
    asyncio.run(main())
