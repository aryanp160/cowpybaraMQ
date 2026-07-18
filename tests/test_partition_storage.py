import pytest

from internal.storage import Storage


@pytest.mark.unit
def test_configurable_partitions(tmp_path):
    """Test storage with a custom configuration of partition counts."""
    storage = Storage(log_dir=tmp_path / "logs", num_partitions=5)
    assert storage.num_partitions == 5

    import zlib

    keys = {}
    for i in range(100):
        k = f"key_{i}"
        part = zlib.crc32(k.encode()) % 5
        if part not in keys:
            keys[part] = k
        if len(keys) == 5:
            break

    partitions = []
    for p_id in range(5):
        p, _ = storage.append("topic", {"msg": "hello"}, key=keys[p_id])
        partitions.append(p)

    assert partitions == [0, 1, 2, 3, 4]


@pytest.mark.unit
def test_key_hashing_partition_selection(storage):
    """Test that messages with identical keys resolve to the same partition."""
    p1, _ = storage.append("topic", {"data": "msg1"}, key="user_123")
    p2, _ = storage.append("topic", {"data": "msg2"}, key="user_123")
    p3, _ = storage.append("topic", {"data": "msg3"}, key="other_key")

    assert p1 == p2
    p3_again, _ = storage.append("topic", {"data": "msg4"}, key="other_key")
    assert p3 == p3_again


@pytest.mark.unit
def test_keyless_messages_default_to_partition_0(storage):
    """Test that messages default to partition 0 when keys are absent."""
    p0, _ = storage.append("topic", {"data": "a"})
    p1, _ = storage.append("topic", {"data": "b"})
    p2, _ = storage.append("topic", {"data": "c"})
    p3, _ = storage.append("topic", {"data": "d"})

    assert p0 == 0
    assert p1 == 0
    assert p2 == 0
    assert p3 == 0


@pytest.mark.unit
def test_recovery_of_offsets_per_partition(tmp_log_dir):
    """Test offsets are recovered and preserved per partition after restart."""
    storage1 = Storage(log_dir=tmp_log_dir, num_partitions=3)

    import zlib

    keys = {}
    for i in range(100):
        k = f"key_{i}"
        part = zlib.crc32(k.encode()) % 3
        if part not in keys:
            keys[part] = k
        if len(keys) == 3:
            break

    p0, offset_0_0 = storage1.append("my_topic", {"x": "p0_1"}, key=keys[0])
    p1, offset_1_0 = storage1.append("my_topic", {"x": "p1_1"}, key=keys[1])
    p2, offset_2_0 = storage1.append("my_topic", {"x": "p2_1"}, key=keys[2])
    p0_again, offset_0_1 = storage1.append("my_topic", {"x": "p0_2"}, key=keys[0])

    assert p0 == 0
    assert p1 == 1
    assert p2 == 2
    assert p0_again == 0

    assert offset_0_0 == 0
    assert offset_1_0 == 0
    assert offset_0_1 == 1

    storage2 = Storage(log_dir=tmp_log_dir, num_partitions=3)

    p_new_0, offset_new_0 = storage2.append("my_topic", {"x": "p0_3"}, key=keys[0])
    assert p_new_0 == 0
    assert offset_new_0 == 2
