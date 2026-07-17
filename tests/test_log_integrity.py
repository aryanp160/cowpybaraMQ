import json
import zlib
import logging
from internal.partition import Partition


def test_checksum_generation(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    partition = Partition(topic_name="integrity-topic", partition_id=0, log_dir=log_dir)

    offset = partition.append({"msg": "hello"})
    assert offset == 0

    # Verify log entry contains a valid checksum
    with partition.file_path.open("r", encoding="utf-8") as f:
        line = f.readline().strip()
        entry = json.loads(line)
        assert "checksum" in entry
        assert entry["offset"] == 0

        # Manually compute expected checksum
        expected = zlib.crc32(
            json.dumps(
                {"offset": entry["offset"], "message": entry["message"]}, sort_keys=True
            ).encode("utf-8")
        )
        assert entry["checksum"] == expected


def test_corrupted_log_skipping(tmp_path, caplog):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    file_path = log_dir / "corrupt-topic-0.jsonl"

    # Write one valid entry, one corrupted entry, and one valid entry
    valid1 = {"offset": 0, "message": {"msg": "valid1"}, "checksum": 0}
    # Calculate checksum for valid1
    valid1["checksum"] = zlib.crc32(
        json.dumps(
            {"offset": valid1["offset"], "message": valid1["message"]}, sort_keys=True
        ).encode("utf-8")
    )

    corrupt = '{"offset": 1, "message": {"msg": "corrupt"}, "checksum": 99999}'  # Invalid checksum

    valid2 = {"offset": 2, "message": {"msg": "valid2"}, "checksum": 0}
    # Calculate checksum for valid2
    valid2["checksum"] = zlib.crc32(
        json.dumps(
            {"offset": valid2["offset"], "message": valid2["message"]}, sort_keys=True
        ).encode("utf-8")
    )

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(valid1) + "\n")
        f.write(corrupt + "\n")
        f.write(json.dumps(valid2) + "\n")

    with caplog.at_level(logging.WARNING):
        partition = Partition(
            topic_name="corrupt-topic", partition_id=0, log_dir=log_dir
        )
        records = partition.read_all()

    # Verify only valid records are read
    assert len(records) == 2
    assert records[0]["message"] == {"msg": "valid1"}
    assert records[1]["message"] == {"msg": "valid2"}

    # Verify next_offset is based on highest valid offset + 1
    assert partition.next_offset == 3

    # Verify detailed warning log
    assert any("checksum mismatch" in record.message for record in caplog.records)


def test_truncated_log_skipping(tmp_path, caplog):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    file_path = log_dir / "truncated-topic-0.jsonl"

    valid1 = {"offset": 0, "message": {"msg": "valid1"}, "checksum": 0}
    valid1["checksum"] = zlib.crc32(
        json.dumps(
            {"offset": valid1["offset"], "message": valid1["message"]}, sort_keys=True
        ).encode("utf-8")
    )

    truncated = '{"offset": 1, "message": {"msg": "trun'  # Partially written JSON

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(valid1) + "\n")
        f.write(truncated + "\n")

    with caplog.at_level(logging.WARNING):
        partition = Partition(
            topic_name="truncated-topic", partition_id=0, log_dir=log_dir
        )
        records = partition.read_all()

    assert len(records) == 1
    assert records[0]["message"] == {"msg": "valid1"}
    assert partition.next_offset == 1

    # Verify warning log for JSON parse failure
    assert any("failed to parse JSON" in record.message for record in caplog.records)
