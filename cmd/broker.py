import argparse
import asyncio
import logging
from pathlib import Path
from internal.networking import Server
from internal.storage import Storage
from internal.broker import Broker

import os

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
    parser.add_argument("--broker-id", type=int, default=None, help="Broker ID")
    parser.add_argument(
        "--cluster-members", default=None, help="Comma-separated cluster members"
    )
    parser.add_argument(
        "--compression-type",
        choices=["none", "gzip"],
        default=None,
        help="Compression type",
    )
    parser.add_argument(
        "--compression-threshold",
        type=int,
        default=None,
        help="Compression threshold size in bytes",
    )

    args = parser.parse_args()

    if args.broker_id is not None:
        os.environ["COWPYBARA_BROKER_ID"] = str(args.broker_id)
    if args.cluster_members is not None:
        os.environ["COWPYBARA_CLUSTER_MEMBERS"] = args.cluster_members
    if args.compression_type is not None:
        os.environ["COWPYBARA_COMPRESSION_TYPE"] = args.compression_type
    if args.compression_threshold is not None:
        os.environ["COWPYBARA_COMPRESSION_THRESHOLD"] = str(args.compression_threshold)

    log_dir = Path(f"./logs-{args.port}")

    storage = Storage(log_dir=log_dir)
    broker = Broker(
        storage=storage,
        role=args.role,
        leader_host=args.leader_host,
        leader_port=args.leader_port,
        compression_type=args.compression_type,
        compression_threshold=args.compression_threshold,
    )
    server = Server(args.host, args.port, broker)

    shutdown_event = asyncio.Event()

    def handle_signal(*args):
        logging.info("Graceful shutdown signal received...")
        shutdown_event.set()

    try:
        import signal

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, handle_signal)
            except NotImplementedError:
                signal.signal(
                    sig, lambda s, f: loop.call_soon_threadsafe(shutdown_event.set)
                )
    except Exception as e:
        logging.warning(f"Could not register signal handlers: {e}")

    server_task = asyncio.create_task(server.start())

    try:
        await asyncio.wait(
            [server_task, shutdown_event.wait()], return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        logging.info("Initiating broker shutdown...")
        await broker.shutdown()
        logging.info("Stopping server...")
        await server.stop()
        if not server_task.done():
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBroker stopped.")
