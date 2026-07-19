import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest


def load_cli_module(name, path_str):
    path = Path(path_str).absolute()
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


producer_mod = load_cli_module("cli_producer", "cmd/producer.py")
consumer_mod = load_cli_module("cli_consumer", "cmd/consumer.py")

produce = producer_mod.produce
consume = consumer_mod.consume


@pytest.mark.unit
@pytest.mark.asyncio
async def test_consumer_receives_messages(temp_broker_server, capsys):
    """Test the CLI consumer tool successfully connects and reads messages in order."""
    host, port, _, _, _ = temp_broker_server

    # Produce first so the consumer fetches them historically
    await produce("test_topic", "msg1", host, port)
    await produce("test_topic", "msg2", host, port)

    # Run consumer as a background task since it loops indefinitely
    consumer_task = asyncio.create_task(consume("test_topic", 0, host, port))

    # Allow time for messages to be received
    await asyncio.sleep(0.1)

    # Cancel the consumer loop safely
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    captured = capsys.readouterr()
    assert "[0] {'msg': 'msg1'}" in captured.out
    assert "[1] {'msg': 'msg2'}" in captured.out


@pytest.mark.unit
@pytest.mark.asyncio
async def test_consumer_nonexistent_topic(temp_broker_server, capsys):
    """Test that connecting to a non-existent topic doesn't crash the consumer."""
    host, port, _, _, _ = temp_broker_server

    consumer_task = asyncio.create_task(consume("unknown_topic", 0, host, port))
    await asyncio.sleep(0.1)

    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    captured = capsys.readouterr()
    assert (
        "Subscribed to topic 'unknown_topic' at offset 0. Waiting for messages..."
        in captured.out
    )
