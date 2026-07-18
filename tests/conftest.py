import asyncio
import logging
import socket

import pytest
import pytest_asyncio

from internal.broker import Broker
from internal.groups import GroupManager
from internal.networking import Server
from internal.offsets import OffsetManager
from internal.storage import Storage

# Suppress noisy logging during tests unless specifically requested
logging.getLogger("internal").setLevel(logging.WARNING)


@pytest.fixture
def tmp_log_dir(tmp_path):
    """Provides a temporary, isolated log directory for tests."""
    return tmp_path / "logs"


@pytest.fixture
def storage(tmp_log_dir):
    """Provides an isolated Storage instance for unit testing."""
    return Storage(log_dir=tmp_log_dir)


def get_free_port():
    """Dynamically finds a free port to avoid conflicts during testing."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest_asyncio.fixture
async def temp_broker_server(tmp_log_dir):
    """
    Spins up a complete, isolated Broker and TCP Server in the background.
    Yields a tuple of (host, port, storage, broker, server).
    Automatically shuts down the server and cleans up after the test.
    """
    port = get_free_port()
    host = "127.0.0.1"

    storage = Storage(log_dir=tmp_log_dir)
    offset_manager = OffsetManager(filepath=tmp_log_dir / "offsets.json")
    group_manager = GroupManager(filepath=tmp_log_dir / "group_offsets.json")
    broker = Broker(
        storage=storage, offset_manager=offset_manager, group_manager=group_manager
    )
    server = Server(host, port, broker)

    server_task = asyncio.create_task(server.start())

    # Allow the server a brief moment to bind and start
    await asyncio.sleep(0.05)

    yield host, port, storage, broker, server

    # Teardown
    try:
        await server.stop()
    except Exception:
        pass
    try:
        await broker.replication_manager.stop()
    except Exception:
        pass
    try:
        await broker.cluster_manager.stop()
    except Exception:
        pass

    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

    # Storage dir cleanup is handled by pytest's tmp_path fixture automatically
