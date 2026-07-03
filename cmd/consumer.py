import asyncio
import json
import argparse


async def consume(topic, offset, host="127.0.0.1", port=9092):
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print(f"Error: Could not connect to broker at {host}:{port}")
        return

    req = {"action": "consume", "topic": topic, "offset": offset}

    writer.write((json.dumps(req) + "\n").encode("utf-8"))
    await writer.drain()

    print(f"Subscribed to topic '{topic}' at offset {offset}. Waiting for messages...")

    try:
        while True:
            line = await reader.readline()
            if not line:
                print("\nDisconnected from broker.")
                break

            try:
                msg = json.loads(line.decode("utf-8").strip())
                # Historical messages use 'message', live stream uses 'payload'
                payload_data = msg.get("payload") or msg.get("message")
                print(f"[{msg.get('offset', '?')}] {payload_data}")
            except json.JSONDecodeError:
                print(f"Raw response: {line.decode('utf-8').strip()}")

    except asyncio.CancelledError:
        pass
    except ConnectionError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()


def main():
    parser = argparse.ArgumentParser(description="CowpybaraMQ Consumer")
    parser.add_argument("--topic", required=True, help="Topic to consume from")
    parser.add_argument("--offset", type=int, default=0, help="Starting offset")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--port", type=int, default=9092, help="Broker port")

    args = parser.parse_args()

    try:
        asyncio.run(consume(args.topic, args.offset, args.host, args.port))
    except KeyboardInterrupt:
        print("\nConsumer stopped.")


if __name__ == "__main__":
    main()
