import json
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ProduceRequest:
    topic: str
    payload: Dict[str, Any]
    key: str = None


@dataclass
class ConsumeRequest:
    topic: str
    offset: int = None
    consumer_id: str = None
    group_id: str = None


@dataclass
class StatusRequest:
    pass


def parse_request(line: str) -> Any:
    """Parse a newline-delimited JSON string into a request object."""
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format")

    action = data.get("action")
    if not action:
        raise ValueError("Missing 'action' field")

    if action == "status":
        return StatusRequest()

    if action == "produce":
        topic = data.get("topic")
        payload = data.get("payload")
        key = data.get("key")
        if not topic or payload is None:
            raise ValueError("Missing 'topic' or 'payload' for produce action")
        return ProduceRequest(topic=topic, payload=payload, key=key)

    elif action == "consume":
        topic = data.get("topic")
        offset = data.get("offset")
        consumer_id = data.get("consumer_id")
        group_id = data.get("group_id")
        if not topic:
            raise ValueError("Missing 'topic' for consume action")
        if offset is None and not consumer_id and not group_id:
            raise ValueError(
                "Missing 'offset', 'consumer_id', or 'group_id' for consume action"
            )
        return ConsumeRequest(
            topic=topic, offset=offset, consumer_id=consumer_id, group_id=group_id
        )

    else:
        raise ValueError(f"Unknown action: {action}")


def format_response(status: str, **kwargs) -> bytes:
    """Format a response into newline-delimited JSON bytes."""
    response = {"status": status}
    response.update(kwargs)
    return (json.dumps(response) + "\n").encode("utf-8")
