import argparse
import asyncio
import logging
from pathlib import Path
from internal.networking import Server
from internal.storage import Storage
from internal.broker import Broker

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def main():
    parser = argparse.ArgumentParser(description="CowpybaraMQ Broker Server")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=9092, help="Port to bind to")
    parser.add_argument(
        "--role",
        choices=["leader", "follower"],
        default="leader",
        help="Broker role",
    )
    parser.add_argument(
        "--leader-host", default="127.0.0.1", help="Leader host address"
    )
    parser.add_argument(
        "--leader-port", type=int, default=9092, help="Leader port number"
    )

    args = parser.parse_args()

    log_dir = Path(f"./logs-{args.port}")

    storage = Storage(log_dir=log_dir)
    broker = Broker(
        storage=storage,
        role=args.role,
        leader_host=args.leader_host,
        leader_port=args.leader_port,
    )
    server = Server(args.host, args.port, broker)

    try:
        await server.start()
    finally:
        await broker.replication_manager.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBroker stopped.")
