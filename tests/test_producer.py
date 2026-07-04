import pytest
import importlib.util
import sys
from pathlib import Path


# Dynamically load the CLI tool to avoid shadowing standard library 'cmd'
def load_cli_module(name, path_str):
    path = Path(path_str).absolute()
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


producer_mod = load_cli_module("cli_producer", "cmd/producer.py")
produce = producer_mod.produce


@pytest.mark.unit
@pytest.mark.asyncio
async def test_producer_valid_message(temp_broker_server, capsys):
    """Test the CLI producer tool successfully sends a valid message."""
    host, port, _, _, _ = temp_broker_server

    await produce("test_topic", "hello unit test", host=host, port=port)

    captured = capsys.readouterr()
    assert (
        'Server response: {"status": "ok", "partition": 0, "offset": 0}'
        in captured.out
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_producer_connection_refused(capsys):
    """Test the CLI producer gracefully handles a connection refused error."""
    # Attempt connection to a dead port
    await produce("test_topic", "hello", host="127.0.0.1", port=9999)
    captured = capsys.readouterr()

    assert "Error: Could not connect to broker" in captured.out
