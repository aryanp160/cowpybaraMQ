import json
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ProduceRequest:
    topic: str
    payload: Dict[str, Any]
    key: str = None
    acks: str = "1"


@dataclass
class ConsumeRequest:
    topic: str
    offset: int = None
    consumer_id: str = None
    group_id: str = None


@dataclass
class StatusRequest:
    pass


@dataclass
class RegisterFollowerRequest:
    broker_id: str
    offsets: Dict[str, int]


@dataclass
class ReplicateRequest:
    topic: str
    partition: int
    offset: int
    payload: Dict[str, Any]


@dataclass
class HeartbeatRequest:
    sender_id: str
    role: str


@dataclass
class ReplicateAckRequest:
    broker_id: str
    topic: str
    partition: int
    offset: int


@dataclass
class ElectRequest:
    candidate_id: str


@dataclass
class ClusterStatusRequest:
    pass


@dataclass
class SimulateFailureRequest:
    type: str


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

    if action == "cluster_status":
        return ClusterStatusRequest()

    if action == "simulate_failure":
        sim_type = data.get("type")
        if not sim_type:
            raise ValueError("Missing type for simulate_failure")
        return SimulateFailureRequest(type=sim_type)

    if action == "heartbeat":
        sender_id = data.get("sender_id")
        role = data.get("role")
        if not sender_id or not role:
            raise ValueError("Missing sender_id or role for heartbeat")
        return HeartbeatRequest(sender_id=sender_id, role=role)

    if action == "replicate_ack":
        broker_id = data.get("broker_id")
        topic = data.get("topic")
        partition = data.get("partition")
        offset = data.get("offset")
        if not broker_id or not topic or partition is None or offset is None:
            raise ValueError("Missing required fields for replicate_ack")
        return ReplicateAckRequest(
            broker_id=broker_id,
            topic=topic,
            partition=int(partition),
            offset=int(offset),
        )

    if action == "elect":
        candidate_id = data.get("candidate_id")
        if not candidate_id:
            raise ValueError("Missing candidate_id for elect")
        return ElectRequest(candidate_id=candidate_id)

    if action == "register_follower":
        broker_id = data.get("broker_id")
        offsets = data.get("offsets", {})
        if not broker_id:
            raise ValueError("Missing broker_id for register_follower")
        return RegisterFollowerRequest(
            broker_id=broker_id,
            offsets=offsets,
        )

    if action == "replicate":
        topic = data.get("topic")
        partition = data.get("partition")
        offset = data.get("offset")
        payload = data.get("payload")
        if not topic or partition is None or offset is None or payload is None:
            raise ValueError("Missing required fields for replicate")
        return ReplicateRequest(
            topic=topic,
            partition=int(partition),
            offset=int(offset),
            payload=payload,
        )

    if action == "produce":
        topic = data.get("topic")
        payload = data.get("payload")
        key = data.get("key")
        acks = str(data.get("acks", "1"))
        if not topic or payload is None:
            raise ValueError("Missing 'topic' or 'payload' for produce action")
        return ProduceRequest(topic=topic, payload=payload, key=key, acks=acks)

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
