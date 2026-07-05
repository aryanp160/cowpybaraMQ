import asyncio
import logging
from internal.protocol import (
    parse_request,
    format_response,
    ProduceRequest,
    ConsumeRequest,
    StatusRequest,
)
from internal.broker import Broker

logger = logging.getLogger(__name__)


class Server:
    def __init__(self, host: str, port: int, broker: Broker):
        self.host = host
        self.port = port
        self.server = None
        self.broker = broker

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        addr = writer.get_extra_info("peername")
        logger.info(f"Accepted connection from {addr}")

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break

                decoded_line = line.decode("utf-8").strip()
                if not decoded_line:
                    continue

                try:
                    request = parse_request(decoded_line)

                    if isinstance(request, StatusRequest):
                        stats = self.broker.get_stats()
                        writer.write(format_response("ok", stats=stats))
                        await writer.drain()

                    elif isinstance(request, ProduceRequest):
                        key = getattr(request, "key", None)
                        logger.info(
                            f"PRODUCE request: topic={request.topic}, "
                            f"payload={request.payload}, key={key}"
                        )
                        self.broker.active_producers += 1
                        try:
                            partition_id, offset = await self.broker.publish(
                                request.topic,
                                request.payload,
                                key,
                            )
                        finally:
                            self.broker.active_producers -= 1
                        writer.write(
                            format_response("ok", partition=partition_id, offset=offset)
                        )
                        await writer.drain()

                    elif isinstance(request, ConsumeRequest):
                        logger.info(
                            f"CONSUME request: topic={request.topic}, "
                            f"offset={request.offset}"
                        )
                        # Delegate to broker to send historical messages.
                        # Run in background task to detect disconnects.
                        sub_task = asyncio.create_task(
                            self.broker.subscribe(
                                request.topic,
                                request.offset,
                                writer,
                                getattr(request, "consumer_id", None),
                                getattr(request, "group_id", None),
                            )
                        )

                        # Wait for the client to disconnect (EOF)
                        while True:
                            eof_line = await reader.readline()
                            if not eof_line:
                                break

                        # Cancel subscription when client disconnects
                        sub_task.cancel()
                        try:
                            await sub_task
                        except asyncio.CancelledError:
                            pass

                        break  # Exit the main handler loop

                except ValueError as e:
                    logger.error(f"Invalid request from {addr}: {e}")
                    writer.write(format_response("error", message=str(e)))
                    await writer.drain()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}")
        finally:
            logger.info(f"Closing connection to {addr}")
            writer.close()
            await writer.wait_closed()

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        addrs = ", ".join(str(sock.getsockname()) for sock in self.server.sockets)
        logger.info(f"Serving on {addrs}")

        async with self.server:
            await self.server.serve_forever()

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("Server stopped.")
