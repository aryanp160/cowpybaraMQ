from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class Message:
    topic: str
    payload: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
