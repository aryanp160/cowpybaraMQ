import asyncio
import json

import pytest


@pytest.mark.unit
@pytest.mark.asyncio
async def test_broker_startup_shutdown(temp_broker_server):
    """Test broker starts up, accepts connections, and shuts down."""
    host, port, storage, broker, server = temp_broker_server
    reader, writer = await asyncio.open_connection(host, port)
    writer.close()
    await writer.wait_closed()
    assert True


@pytest.mark.protocol
@pytest.mark.asyncio
async def test_protocol_invalid_json(temp_broker_server):
    """Test handling of completely invalid JSON."""
    host, port, _, _, _ = temp_broker_server
    reader, writer = await asyncio.open_connection(host, port)

    writer.write(b"this is not json\n")
    await writer.drain()

    response = await reader.readline()
    resp_data = json.loads(response.decode("utf-8").strip())

    assert resp_data["status"] == "error"
    assert "Invalid JSON" in resp_data["message"]

    writer.close()
    await writer.wait_closed()


@pytest.mark.protocol
@pytest.mark.asyncio
async def test_protocol_missing_action(temp_broker_server):
    """Test handling of JSON that is missing the 'action' key."""
    host, port, _, _, _ = temp_broker_server
    reader, writer = await asyncio.open_connection(host, port)

    req = {"not_action": "produce"}
    writer.write((json.dumps(req) + "\n").encode("utf-8"))
    await writer.drain()

    response = await reader.readline()
    resp_data = json.loads(response.decode("utf-8").strip())

    assert resp_data["status"] == "error"
    assert "Missing 'action'" in resp_data["message"]

    writer.close()
    await writer.wait_closed()


@pytest.mark.protocol
@pytest.mark.asyncio
async def test_protocol_unknown_command(temp_broker_server):
    """Test handling of an action that does not exist."""
    host, port, _, _, _ = temp_broker_server
    reader, writer = await asyncio.open_connection(host, port)

    req = {"action": "delete"}
    writer.write((json.dumps(req) + "\n").encode("utf-8"))
    await writer.drain()

    response = await reader.readline()
    resp_data = json.loads(response.decode("utf-8").strip())

    assert resp_data["status"] == "error"
    assert "Unknown action" in resp_data["message"]

    writer.close()
    await writer.wait_closed()


@pytest.mark.protocol
@pytest.mark.asyncio
async def test_protocol_large_payload(temp_broker_server):
    """Test that the broker can handle a large payload without crashing."""
    host, port, _, _, _ = temp_broker_server
    reader, writer = await asyncio.open_connection(host, port)

    # 1 MB payload, exceeding asyncio 64KB default read limit
    large_str = "A" * 1024 * 1024
    req = {"action": "produce", "topic": "large", "payload": {"data": large_str}}
    writer.write((json.dumps(req) + "\n").encode("utf-8"))
    await writer.drain()

    # Server will drop the connection due to LimitOverrunError
    try:
        response = await reader.readline()
        assert response == b""  # EOF
    except ConnectionResetError:
        pass  # Also valid on Windows when connection abruptly drops

    try:
        writer.close()
        await writer.wait_closed()
    except ConnectionResetError:
        pass
