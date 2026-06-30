import asyncio
import logging
from internal.protocol import parse_request, format_response, ProduceRequest, ConsumeRequest

logger = logging.getLogger(__name__)

class Server:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server = None

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
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
                    # Only receive requests and decode them
                    request = parse_request(decoded_line)
                    
                    if isinstance(request, ProduceRequest):
                        logger.info(f"Parsed PRODUCE request: topic={request.topic}, payload={request.payload}")
                        writer.write(format_response("ok"))
                    
                    elif isinstance(request, ConsumeRequest):
                        logger.info(f"Parsed CONSUME request: topic={request.topic}, offset={request.offset}")
                        # Returning dummy JSON response (no pub/sub logic implemented)
                        writer.write(format_response("ok", topic=request.topic, payload={}, timestamp=""))
                    
                    await writer.drain()

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
        addrs = ', '.join(str(sock.getsockname()) for sock in self.server.sockets)
        logger.info(f"Serving on {addrs}")

        async with self.server:
            await self.server.serve_forever()

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("Server stopped.")
