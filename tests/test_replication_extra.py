import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from internal.replication import ReplicationManager


@pytest.fixture
def mock_broker():
    broker = MagicMock()
    broker.storage = MagicMock()
    broker.metrics = MagicMock()
    broker.cluster_manager = MagicMock()
    broker.cluster_manager.killed = False
    broker.cluster_manager.disconnected = False
    broker.cluster_manager.broker_id = 999
    return broker


@pytest.mark.asyncio
async def test_catchup_follower_connection_error(mock_broker):
    rep = ReplicationManager(mock_broker, "leader")
    writer = MagicMock()
    writer.write = MagicMock(side_effect=ConnectionError("Test connection error"))
    rep.followers["fol1"] = writer
    rep.follower_offsets["fol1"] = {}

    mock_broker.storage.read_all.return_value = [
        {"offset": 1, "message": {"msg": "hello"}}
    ]

    await rep.register_follower("fol1", {"test_topic-0": 1}, writer)

    # Should handle error and remove follower
    assert "fol1" not in rep.followers
    assert "fol1" not in rep.follower_offsets


@pytest.mark.asyncio
async def test_handle_replicate_ack_new_follower(mock_broker):
    rep = ReplicationManager(mock_broker, "leader")

    await rep.handle_replicate_ack("fol2", "topic1", 0, 5)

    assert "fol2" in rep.follower_offsets
    assert rep.follower_offsets["fol2"]["topic1-0"] == 5
    mock_broker.metrics.record_replication.assert_called_once()


@pytest.mark.asyncio
async def test_wait_for_acks_no_followers(mock_broker):
    rep = ReplicationManager(mock_broker, "leader")

    res = await rep.wait_for_acks("topic1", 0, 10, timeout=0.1)
    assert res is True


@pytest.mark.asyncio
async def test_wait_for_acks_timeout(mock_broker):
    rep = ReplicationManager(mock_broker, "leader")
    rep.followers["fol1"] = MagicMock()
    rep.follower_offsets["fol1"] = {"topic1-0": 5}

    res = await rep.wait_for_acks("topic1", 0, 10, timeout=0.05)
    assert res is False


@pytest.mark.asyncio
async def test_broadcast_replication_no_followers(mock_broker):
    rep = ReplicationManager(mock_broker, "leader")

    # Should return without doing anything
    await rep.broadcast_replication("topic1", 0, 1, {"msg": "hello"})


@pytest.mark.asyncio
async def test_broadcast_replication_write_exception(mock_broker):
    rep = ReplicationManager(mock_broker, "leader")
    writer = MagicMock()
    writer.write = MagicMock(side_effect=Exception("Test write error"))
    rep.followers["fol1"] = writer
    rep.follower_offsets["fol1"] = {}

    await rep.broadcast_replication("topic1", 0, 1, {"msg": "hello"})

    assert "fol1" not in rep.followers
    assert "fol1" not in rep.follower_offsets


@pytest.mark.asyncio
async def test_stop_wait_closed_exception(mock_broker):
    rep = ReplicationManager(mock_broker, "leader")
    writer = MagicMock()
    writer.wait_closed = AsyncMock(side_effect=Exception("Test close error"))
    rep.followers["fol1"] = writer

    await rep.stop()
    # Shouldn't raise
    assert "fol1" not in rep.followers


@pytest.mark.asyncio
async def test_sync_loop_missing_fields(mock_broker):
    rep = ReplicationManager(mock_broker, "follower")

    reader = AsyncMock()
    # Missing 'payload'
    invalid_req = {"action": "replicate", "topic": "t", "partition": 0, "offset": 1}
    reader.readline = AsyncMock(
        side_effect=[json.dumps(invalid_req).encode() + b"\n", b""]
    )

    writer = MagicMock()

    with patch("asyncio.open_connection", AsyncMock(return_value=(reader, writer))):
        # Prevent it from looping forever if it doesn't break
        rep.running = False

        # We need to temporarily set running=True inside the loop, so we'll mock asyncio.sleep

        async def mock_sleep(*args, **kwargs):
            rep.running = False

        with patch("asyncio.sleep", mock_sleep):
            rep.running = True
            await rep._sync_loop("127.0.0.1", 1234)

    # Storage should not be called due to continue
    mock_broker.storage.append.assert_not_called()


@pytest.mark.asyncio
async def test_sync_loop_json_decode_error(mock_broker):
    rep = ReplicationManager(mock_broker, "follower")

    reader = AsyncMock()
    reader.readline = AsyncMock(side_effect=[b"invalid json\n", b""])
    writer = MagicMock()

    with patch("asyncio.open_connection", AsyncMock(return_value=(reader, writer))):

        async def mock_sleep(*args, **kwargs):
            rep.running = False

        with patch("asyncio.sleep", mock_sleep):
            rep.running = True
            await rep._sync_loop("127.0.0.1", 1234)

    mock_broker.storage.append.assert_not_called()


@pytest.mark.asyncio
async def test_sync_loop_connection_refused(mock_broker):
    rep = ReplicationManager(mock_broker, "follower")

    with patch(
        "asyncio.open_connection", AsyncMock(side_effect=ConnectionRefusedError())
    ):

        async def mock_sleep(*args, **kwargs):
            rep.running = False

        with patch("asyncio.sleep", mock_sleep):
            rep.running = True
            await rep._sync_loop("127.0.0.1", 1234)


@pytest.mark.asyncio
async def test_sync_loop_unexpected_exception(mock_broker):
    rep = ReplicationManager(mock_broker, "follower")

    with patch(
        "asyncio.open_connection",
        AsyncMock(side_effect=RuntimeError("Unexpected error")),
    ):

        async def mock_sleep(*args, **kwargs):
            rep.running = False

        with patch("asyncio.sleep", mock_sleep):
            rep.running = True
            await rep._sync_loop("127.0.0.1", 1234)


@pytest.mark.asyncio
async def test_sync_loop_finally_writer_exception(mock_broker):
    rep = ReplicationManager(mock_broker, "follower")

    reader = AsyncMock()
    reader.readline = AsyncMock(return_value=b"")
    writer = MagicMock()
    writer.wait_closed = AsyncMock(side_effect=Exception("Wait closed failed"))

    with patch("asyncio.open_connection", AsyncMock(return_value=(reader, writer))):

        async def mock_sleep(*args, **kwargs):
            rep.running = False

        with patch("asyncio.sleep", mock_sleep):
            rep.running = True
            await rep._sync_loop("127.0.0.1", 1234)
