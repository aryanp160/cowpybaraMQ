import json
import threading
from pathlib import Path
from typing import Dict


class OffsetManager:
    def __init__(self, filepath: str = "storage/offsets.json"):
        self.filepath = Path(filepath)
        self.lock = threading.Lock()
        self.offsets: Dict[str, Dict[str, int]] = {}
        self.load()

    def load(self):
        """Load offsets from disk."""
        with self.lock:
            if not self.filepath.exists():
                self.filepath.parent.mkdir(parents=True, exist_ok=True)
                self.offsets = {}
                self.save_unlocked()
                return
            try:
                with self.filepath.open("r", encoding="utf-8") as f:
                    self.offsets = json.load(f)
            except (json.JSONDecodeError, IOError):
                self.offsets = {}

    def save_unlocked(self):
        """Save offsets to disk (assumes lock is held)."""
        with self.filepath.open("w", encoding="utf-8") as f:
            json.dump(self.offsets, f, indent=4)

    def get_offset(self, consumer_id: str, topic: str) -> int:
        """Get the current offset for a consumer and topic. Default to 0."""
        with self.lock:
            if consumer_id not in self.offsets:
                self.offsets[consumer_id] = {}
            if topic not in self.offsets[consumer_id]:
                self.offsets[consumer_id][topic] = 0
                self.save_unlocked()
            return self.offsets[consumer_id][topic]

    def update_offset(self, consumer_id: str, topic: str, offset: int):
        """Update the offset for a consumer and topic, saving to disk."""
        with self.lock:
            if consumer_id not in self.offsets:
                self.offsets[consumer_id] = {}
            self.offsets[consumer_id][topic] = offset
            self.save_unlocked()
