import asyncio
import json
import pytest
import socket
from internal.storage import Storage
from internal.broker import Broker
from internal.networking import Server


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.integration
@pytest.mark.asyncio
async def test_compression_integration(tmp_path):
    port = get_free_port()
    log_dir = tmp_path / "compressed_logs"
    storage = Storage(log_dir=log_dir)

    # Instantiate broker with compression type 'gzip' and threshold 100
    broker = Broker(
        storage=storage,
        role="leader",
        compression_type="gzip",
        compression_threshold=100,
    )
    server = Server("127.0.0.1", port, broker)
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.05)

    try:
        # Connect to broker and publish a large payload
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        large_payload = {"msg": "x" * 200}
        req = {
            "action": "produce",
            "topic": "comp-topic",
            "payload": large_payload,
            "acks": "1",
        }
        writer.write((json.dumps(req) + "\n").encode())
        await writer.drain()

        resp = await reader.readline()
        resp_data = json.loads(resp.decode().strip())
        assert resp_data["status"] == "ok"
        writer.close()
        await writer.wait_closed()

        # Connect consumer and fetch the message
        reader_c, writer_c = await asyncio.open_connection("127.0.0.1", port)
        req_c = {"action": "consume", "topic": "comp-topic", "offset": 0}
        writer_c.write((json.dumps(req_c) + "\n").encode())
        await writer_c.drain()

        resp_c = await reader_c.readline()
        resp_c_data = json.loads(resp_c.decode().strip())
        assert resp_c_data["status"] == "ok"
        assert resp_c_data["payload"] == large_payload  # Decompressed!
        writer_c.close()
        await writer_c.wait_closed()

        # Let's inspect the raw log file on disk to confirm it was stored compressed!
        part = storage.get_partition("comp-topic", 0)
        with part.file_path.open("r", encoding="utf-8") as f:
            lines = [json.loads(line.strip()) for line in f if line.strip()]
        assert len(lines) == 1
        stored_message = lines[0]["message"]
        assert "_compressed_payload" in stored_message
        assert stored_message["_compression"] == "gzip"

    finally:
        await server.stop()
        await broker.replication_manager.stop()
        await broker.cluster_manager.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass
