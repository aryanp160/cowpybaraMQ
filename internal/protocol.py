import json
from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class ProduceRequest:
    topic: str
    payload: Dict[str, Any]

@dataclass
class ConsumeRequest:
    topic: str
    offset: int

def parse_request(line: str) -> Any:
    """Parse a newline-delimited JSON string into a request object."""
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format")

    action = data.get("action")
    if not action:
        raise ValueError("Missing 'action' field")

    if action == "produce":
        topic = data.get("topic")
        payload = data.get("payload")
        if not topic or payload is None:
            raise ValueError("Missing 'topic' or 'payload' for produce action")
        return ProduceRequest(topic=topic, payload=payload)
    
    elif action == "consume":
        topic = data.get("topic")
        offset = data.get("offset")
        if not topic or offset is None:
            raise ValueError("Missing 'topic' or 'offset' for consume action")
        return ConsumeRequest(topic=topic, offset=offset)
    
    else:
        raise ValueError(f"Unknown action: {action}")

def format_response(status: str, **kwargs) -> bytes:
    """Format a response into newline-delimited JSON bytes."""
    response = {"status": status}
    response.update(kwargs)
    return (json.dumps(response) + "\n").encode("utf-8")
