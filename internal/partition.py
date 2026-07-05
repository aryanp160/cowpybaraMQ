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

        # Determine the next offset by counting existing lines
        with self.file_path.open("r", encoding="utf-8") as f:
            for _ in f:
                self.next_offset += 1

    def append(self, message: Dict[str, Any]) -> int:
        """Append a message to the partition's JSONL file with an increasing offset."""
        offset = self.next_offset

        entry = {"offset": offset, "message": message}

        # Open in append mode ("a") to ensure we never overwrite logs
        with self.file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        self.next_offset += 1
        return offset

    def read_all(self) -> List[Dict[str, Any]]:
        """Read all messages from the partition's JSONL file."""
        messages = []
        with self.file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    messages.append(json.loads(line))
        return messages
