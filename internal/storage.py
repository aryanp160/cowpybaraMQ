from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

from internal.topic import Topic


@dataclass
class Storage:
    log_dir: Path
    topics: Dict[str, Topic] = field(init=False, default_factory=dict)

    def __post_init__(self):
        """Initialize storage by ensuring log dir exists and loading topics."""
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Discover and load existing topics from disk
        for file_path in self.log_dir.glob("*.jsonl"):
            topic_name = file_path.stem
            self.topics[topic_name] = Topic(name=topic_name, log_dir=self.log_dir)

    def get_topic(self, topic_name: str) -> Topic:
        """Get an existing topic or create it automatically if it doesn't exist."""
        if topic_name not in self.topics:
            self.topics[topic_name] = Topic(name=topic_name, log_dir=self.log_dir)
        return self.topics[topic_name]

    def append(self, topic_name: str, message: Dict[str, Any]) -> int:
        """Append a message to the specified topic."""
        topic = self.get_topic(topic_name)
        return topic.append(message)

    def read_all(self, topic_name: str) -> List[Dict[str, Any]]:
        """Read all messages from the specified topic."""
        topic = self.get_topic(topic_name)
        return topic.read_all()
