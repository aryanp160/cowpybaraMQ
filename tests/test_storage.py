import pytest
from internal.storage import Storage


@pytest.mark.unit
def test_topic_creation(storage):
    """Test automatic topic creation in storage."""
    topic = storage.get_topic("test_topic")
    assert topic.name == "test_topic"
    assert "test_topic" in storage.topics


@pytest.mark.unit
def test_append_and_load_messages_with_order(storage):
    """Test appending messages, loading them correctly, and preserving order."""
    offset1 = storage.append("test_topic", {"data": "msg1"})
    offset2 = storage.append("test_topic", {"data": "msg2"})

    assert offset1 == 0
    assert offset2 == 1

    messages = storage.read_all("test_topic")
    assert len(messages) == 2

    # Verify order is preserved
    assert messages[0]["offset"] == 0
    assert messages[0]["message"]["data"] == "msg1"

    assert messages[1]["offset"] == 1
    assert messages[1]["message"]["data"] == "msg2"


@pytest.mark.unit
def test_persistence_after_restart(tmp_log_dir):
    """Test that reloading the storage layer preserves state, topics, and offsets."""
    storage1 = Storage(log_dir=tmp_log_dir)
    storage1.append("persist_topic", {"d": "1"})
    storage1.append("persist_topic", {"d": "2"})

    # Simulate a full broker restart by instantiating new Storage on same directory
    storage2 = Storage(log_dir=tmp_log_dir)

    # Topic should be automatically loaded
    assert "persist_topic" in storage2.topics

    messages = storage2.read_all("persist_topic")
    assert len(messages) == 2
    assert messages[0]["offset"] == 0
    assert messages[1]["offset"] == 1

    # The next offset should pick up exactly where it left off (2)
    offset3 = storage2.append("persist_topic", {"d": "3"})
    assert offset3 == 2
