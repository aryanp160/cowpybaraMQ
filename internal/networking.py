import asyncio
import logging
from internal.protocol import (
    parse_request,
    format_response,
    ProduceRequest,
    ConsumeRequest,
    StatusRequest,
    RegisterFollowerRequest,
    HeartbeatRequest,
    ReplicateAckRequest,
    ElectRequest,
    ClusterStatusRequest,
    SimulateFailureRequest,
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
        if self.broker.cluster_manager.killed:
            writer.close()
            return

        addr = writer.get_extra_info("peername")
        logger.info(f"Accepted connection from {addr}")

        registered_follower_id = None

        try:
            while True:
                if self.broker.cluster_manager.killed:
                    break
                if self.broker.cluster_manager.disconnected:
                    # Connection simulated as disconnected
                    await asyncio.sleep(0.5)
                    continue

                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=0.2)
                except asyncio.TimeoutError:
                    continue

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

                    elif isinstance(request, SimulateFailureRequest):
                        if request.type == "kill":
                            self.broker.cluster_manager.killed = True
                        elif request.type == "disconnect":
                            self.broker.cluster_manager.disconnected = True
                        elif request.type == "recover":
                            self.broker.cluster_manager.killed = False
                            self.broker.cluster_manager.disconnected = False
                        writer.write(format_response("ok"))
                        await writer.drain()

                    elif isinstance(request, ClusterStatusRequest):
                        stats = self.broker.get_stats()
                        stats["broker_id"] = self.broker.cluster_manager.broker_id
                        stats["role"] = self.broker.replication_manager.role
                        stats["leader_id"] = (
                            self.broker.cluster_manager.heartbeat_service.leader_id
                        )
                        stats["killed"] = self.broker.cluster_manager.killed
                        stats["disconnected"] = self.broker.cluster_manager.disconnected
                        stats["latencies"] = self.broker.cluster_manager.latency_metrics
                        stats["followers"] = list(
                            self.broker.replication_manager.followers.keys()
                        )
                        stats["follower_offsets"] = (
                            self.broker.replication_manager.follower_offsets
                        )
                        writer.write(format_response("ok", stats=stats))
                        await writer.drain()

                    elif isinstance(request, HeartbeatRequest):
                        self.broker.cluster_manager.heartbeat_service.receive_heartbeat(
                            request.sender_id, request.role
                        )

                    elif isinstance(request, ReplicateAckRequest):
                        await self.broker.replication_manager.handle_replicate_ack(
                            request.broker_id,
                            request.topic,
                            request.partition,
                            request.offset,
                        )

                    elif isinstance(request, ElectRequest):
                        if (
                            not self.broker.cluster_manager.killed
                            and not self.broker.cluster_manager.disconnected
                        ):
                            writer.write(
                                format_response(
                                    "ok",
                                    broker_id=self.broker.cluster_manager.broker_id,
                                )
                            )
                            await writer.drain()

                    elif isinstance(request, RegisterFollowerRequest):
                        registered_follower_id = request.broker_id
                        asyncio.create_task(
                            self.broker.replication_manager.register_follower(
                                request.broker_id, request.offsets, writer
                            )
                        )

                    elif isinstance(request, ProduceRequest):
                        key = getattr(request, "key", None)
                        acks = getattr(request, "acks", "1")
                        logger.info(
                            f"PRODUCE request: topic={request.topic}, "
                            f"payload={request.payload}, key={key}, acks={acks}"
                        )
                        if acks == "0":
                            # Fire and forget: do not reply
                            asyncio.create_task(
                                self.broker.publish(
                                    request.topic, request.payload, key, acks=acks
                                )
                            )
                            continue

                        self.broker.active_producers += 1
                        try:
                            partition_id, offset = await self.broker.publish(
                                request.topic,
                                request.payload,
                                key,
                                acks=acks,
                            )
                            writer.write(
                                format_response(
                                    "ok",
                                    partition=partition_id,
                                    offset=offset,
                                )
                            )
                            await writer.drain()
                        except Exception as pe:
                            writer.write(format_response("error", message=str(pe)))
                            await writer.drain()
                        finally:
                            self.broker.active_producers -= 1

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
                            if self.broker.cluster_manager.killed:
                                break
                            try:
                                eof_line = await asyncio.wait_for(
                                    reader.readline(), timeout=0.2
                                )
                                if not eof_line:
                                    break
                            except asyncio.TimeoutError:
                                continue

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
            if (
                registered_follower_id
                and registered_follower_id in self.broker.replication_manager.followers
            ):
                del self.broker.replication_manager.followers[registered_follower_id]
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
