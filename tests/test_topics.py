import pytest

@pytest.mark.unit
def test_multiple_topics(storage):
    """Ensure offsets and messages are strictly isolated between multiple topics."""
    storage.append("topic_a", {"msg": "A1"})
    storage.append("topic_b", {"msg": "B1"})
    
    offset_a = storage.append("topic_a", {"msg": "A2"})
    offset_b = storage.append("topic_b", {"msg": "B2"})
    
    assert offset_a == 1
    assert offset_b == 1
    
    msgs_a = storage.read_all("topic_a")
    msgs_b = storage.read_all("topic_b")
    
    assert len(msgs_a) == 2
    assert msgs_a[0]["message"]["msg"] == "A1"
    assert len(msgs_b) == 2
    assert msgs_b[0]["message"]["msg"] == "B1"

@pytest.mark.unit
def test_invalid_topic_name(storage):
    """Verify how complex or strange topic names are handled."""
    strange_name = "topic-name_123.valid"
    storage.append(strange_name, {"a": 1})
    
    messages = storage.read_all(strange_name)
    assert len(messages) == 1
    assert messages[0]["offset"] == 0
