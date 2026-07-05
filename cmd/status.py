import asyncio
import json
import argparse


async def fetch_status(host="127.0.0.1", port=9092):
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print(f"Error: Could not connect to broker at {host}:{port}")
        return

    req = {"action": "status"}
    writer.write((json.dumps(req) + "\n").encode("utf-8"))
    await writer.drain()

    line = await reader.readline()
    writer.close()
    await writer.wait_closed()

    if not line:
        print("Error: Empty response from broker.")
        return

    try:
        response = json.loads(line.decode("utf-8").strip())
        if response.get("status") != "ok":
            print(f"Error: Broker returned status: {response.get('status')}")
            return

        stats = response.get("stats", {})

        print("Broker Status\n")

        print("Topics:")
        topics = stats.get("topics", {})
        if topics:
            for topic, partitions in topics.items():
                print(f"{topic} ({partitions} partitions)")
        else:
            print("No topics found.")
        print()

        print("Consumers:")
        consumers = stats.get("consumers", [])
        if consumers:
            for consumer in consumers:
                print(consumer)
        else:
            print("No active consumers.")
        print()

        print("Offsets:")
        offsets = stats.get("offsets", {})
        if offsets:
            for part, offset in offsets.items():
                print(f"{part} : {offset}")
        else:
            print("No offsets recorded.")
        print()

        print("Messages:")
        print(f"Total: {stats.get('total_messages', 0)}")

    except json.JSONDecodeError:
        print(f"Error decoding broker response: {line.decode('utf-8').strip()}")


def main():
    parser = argparse.ArgumentParser(description="CowpybaraMQ Status Utility")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--port", type=int, default=9092, help="Broker port")

    args = parser.parse_args()

    try:
        asyncio.run(fetch_status(host=args.host, port=args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
