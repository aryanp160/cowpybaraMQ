import asyncio
import logging
from pathlib import Path
from internal.networking import Server
from internal.storage import Storage
from internal.broker import Broker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def main():
    log_dir = Path("./logs")
    host = "127.0.0.1"
    port = 9092
    
    storage = Storage(log_dir=log_dir)
    broker = Broker(storage=storage)
    server = Server(host, port, broker)
    
    await server.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBroker stopped.")
