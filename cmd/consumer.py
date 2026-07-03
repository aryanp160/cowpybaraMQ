import asyncio
import json
import argparse


async def consume(topic, offset, host="127.0.0.1", port=9092, consumer_id=None):
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print(f"Error: Could not connect to broker at {host}:{port}")
        return

    req = {"action": "consume", "topic": topic}
    if offset is not None:
        req["offset"] = offset
    if consumer_id:
        req["consumer_id"] = consumer_id

    writer.write((json.dumps(req) + "\n").encode("utf-8"))
    await writer.drain()

    start_info = (
        f"at offset {offset}"
        if offset is not None
        else f"with consumer_id '{consumer_id}'"
    )
    print(f"Subscribed to topic '{topic}' {start_info}. Waiting for messages...")

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
    parser.add_argument("--offset", type=int, default=None, help="Starting offset")
    parser.add_argument("--consumer-id", default=None, help="Unique consumer ID")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--port", type=int, default=9092, help="Broker port")

    args = parser.parse_args()

    offset = args.offset
    if offset is None and args.consumer_id is None:
        offset = 0

    try:
        asyncio.run(
            consume(
                args.topic,
                offset,
                consumer_id=args.consumer_id,
                host=args.host,
                port=args.port,
            )
        )
    except KeyboardInterrupt:
        print("\nConsumer stopped.")


if __name__ == "__main__":
    main()
