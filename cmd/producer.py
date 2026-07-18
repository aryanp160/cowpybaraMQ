import argparse
import asyncio
import json


async def produce(topic, message, host="127.0.0.1", port=9092):
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print(f"Error: Could not connect to broker at {host}:{port}")
        return

    req = {"action": "produce", "topic": topic, "payload": {"msg": message}}

    writer.write((json.dumps(req) + "\n").encode("utf-8"))
    await writer.drain()

    response = await reader.readline()
    print(f"Server response: {response.decode('utf-8').strip()}")

    writer.close()
    await writer.wait_closed()


def main():
    parser = argparse.ArgumentParser(description="CowpybaraMQ Producer")
    parser.add_argument("--topic", required=True, help="Topic to produce to")
    parser.add_argument("--message", required=True, help="Message string to send")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--port", type=int, default=9092, help="Broker port")

    args = parser.parse_args()
    asyncio.run(produce(args.topic, args.message, args.host, args.port))


if __name__ == "__main__":
    main()
