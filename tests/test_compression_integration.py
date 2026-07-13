import asyncio
import json
import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_compression_integration(temp_broker_server):
    host, port, storage, broker, server = temp_broker_server

    # Set compression configurations dynamically
    broker.storage.compression_type = "gzip"
    broker.storage.compression_threshold = 100

    # Connect to broker and publish a large payload
    reader, writer = await asyncio.open_connection(host, port)
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
    reader_c, writer_c = await asyncio.open_connection(host, port)
    req_c = {"action": "consume", "topic": "comp-topic", "offset": 0}
    writer_c.write((json.dumps(req_c) + "\n").encode())
    await writer_c.drain()

    resp_c = await reader_c.readline()
    resp_c_data = json.loads(resp_c.decode().strip())
    assert resp_c_data["status"] == "ok"
    assert resp_c_data["message"] == large_payload  # Decompressed!
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
