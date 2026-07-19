import pytest

from internal.storage import Storage


@pytest.mark.unit
def test_topic_creation(storage):
    """Test automatic topic partition creation in storage."""
    partition = storage.get_partition("test_topic", 0)
    assert partition.partition_id == 0
    assert "test_topic" in storage.partitions


@pytest.mark.unit
def test_append_and_load_messages_with_order(storage):
    """Test appending to partition, loading, and preserving order."""
    p1, offset1 = storage.append("test_topic", {"data": "msg1"}, key="my-key")
    p2, offset2 = storage.append("test_topic", {"data": "msg2"}, key="my-key")

    assert p1 == p2
    assert offset1 == 0
    assert offset2 == 1

    messages = storage.read_all("test_topic", partition_id=p1)
    assert len(messages) == 2

    # Verify order is preserved
    assert messages[0]["offset"] == 0
    assert messages[0]["message"]["data"] == "msg1"

    assert messages[1]["offset"] == 1
    assert messages[1]["message"]["data"] == "msg2"


@pytest.mark.unit
def test_persistence_after_restart(tmp_log_dir):
    """Test reloading storage preserves state, partitions, and offsets."""
    storage1 = Storage(log_dir=tmp_log_dir)
    p1, offset1 = storage1.append("persist_topic", {"d": "1"}, key="key")
    p2, offset2 = storage1.append("persist_topic", {"d": "2"}, key="key")

    # Simulate a broker restart
    storage2 = Storage(log_dir=tmp_log_dir)

    # Topic should be automatically loaded
    assert "persist_topic" in storage2.partitions

    messages = storage2.read_all("persist_topic", partition_id=p1)
    assert len(messages) == 2
    assert messages[0]["offset"] == 0
    assert messages[1]["offset"] == 1

    # The next offset should pick up exactly where it left off (2)
    p3, offset3 = storage2.append("persist_topic", {"d": "3"}, key="key")
    assert p3 == p1
    assert offset3 == 2
