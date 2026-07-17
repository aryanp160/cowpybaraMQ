import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class Partition:
    topic_name: str
    partition_id: int
    log_dir: Path
    file_path: Path = field(init=False)
    next_offset: int = field(init=False, default=0)

    def __post_init__(self):
        """Initialize partition by setting up file path and finding next offset."""
        self.file_path = self.log_dir / f"{self.topic_name}-{self.partition_id}.jsonl"

        # Create the file if it doesn't exist to never overwrite existing logs
        if not self.file_path.exists():
            self.file_path.touch()

        import logging
        import zlib

        logger = logging.getLogger(__name__)

        # Determine the next offset by validating each line
        with self.file_path.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if (
                        not isinstance(entry, dict)
                        or "offset" not in entry
                        or "message" not in entry
                    ):
                        logger.warning(
                            f"Corrupted record detected in {self.file_path} at line {line_num}: "
                            f"missing offset/message fields."
                        )
                        continue

                    if "checksum" in entry:
                        checksum = entry["checksum"]
                        expected_checksum = zlib.crc32(
                            json.dumps(
                                {
                                    "offset": entry["offset"],
                                    "message": entry["message"],
                                },
                                sort_keys=True,
                            ).encode("utf-8")
                        )
                        if checksum != expected_checksum:
                            logger.warning(
                                f"Corrupted record detected in {self.file_path} at line {line_num}: "
                                f"checksum mismatch (expected {expected_checksum}, got {checksum})."
                            )
                            continue

                    self.next_offset = max(self.next_offset, entry["offset"] + 1)
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"Corrupted record detected in {self.file_path} at line {line_num}: "
                        f"failed to parse JSON: {e}"
                    )
                    continue

    def append(
        self,
        message: Dict[str, Any],
        offset: int = None,
        codec: str = "none",
        threshold: int = 512,
    ) -> int:
        """Append a message to the partition's JSONL file with an increasing offset."""
        if offset is not None:
            if offset < self.next_offset:
                # Duplicate write, ignore and return the offset
                return offset
            self.next_offset = offset

        offset = self.next_offset

        from internal.compression import compress_payload

        compressed_msg, _ = compress_payload(message, codec, threshold)
        import zlib

        data_str = json.dumps(
            {"offset": offset, "message": compressed_msg}, sort_keys=True
        )
        checksum = zlib.crc32(data_str.encode("utf-8"))

        entry = {"offset": offset, "message": compressed_msg, "checksum": checksum}

        # Open in append mode ("a") to ensure we never overwrite logs
        with self.file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        self.next_offset += 1
        return offset

    def read_all(self) -> List[Dict[str, Any]]:
        """Read all messages from the partition's JSONL file."""
        from internal.compression import decompress_payload
        import logging
        import zlib

        logger = logging.getLogger(__name__)

        messages = []
        with self.file_path.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        entry = json.loads(line)
                        if (
                            not isinstance(entry, dict)
                            or "offset" not in entry
                            or "message" not in entry
                        ):
                            logger.warning(
                                f"Skipping corrupted record in {self.file_path} at line {line_num}: "
                                f"missing offset/message fields."
                            )
                            continue

                        if "checksum" in entry:
                            checksum = entry["checksum"]
                            expected_checksum = zlib.crc32(
                                json.dumps(
                                    {
                                        "offset": entry["offset"],
                                        "message": entry["message"],
                                    },
                                    sort_keys=True,
                                ).encode("utf-8")
                            )
                            if checksum != expected_checksum:
                                logger.warning(
                                    f"Skipping corrupted record in {self.file_path} at line {line_num}: "
                                    f"checksum mismatch."
                                )
                                continue

                        if "message" in entry:
                            entry["message"] = decompress_payload(entry["message"])
                        messages.append(entry)
                    except json.JSONDecodeError as e:
                        logger.warning(
                            f"Skipping corrupted record in {self.file_path} at line {line_num}: "
                            f"failed to parse JSON: {e}"
                        )
                        continue
        return messages

    def flush(self):
        """Flush any pending OS buffers to disk."""
        import os

        try:
            fd = os.open(str(self.file_path), os.O_RDWR | os.O_APPEND)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except Exception:
            pass
