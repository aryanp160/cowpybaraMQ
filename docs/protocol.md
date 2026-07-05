# Protocol Documentation

CowpybaraMQ uses a newline-delimited JSON protocol over raw TCP.

## Produce Request

Sent by producers to write a message to a topic.

### Request Payload
```json
{
  "action": "produce",
  "topic": "orders",
  "key": "user_123",
  "payload": {"item": "A", "price": 100}
}
```

### Response Payload
```json
{
  "status": "ok",
  "partition": 1,
  "offset": 45
}
```

---

## Consume Request

Sent by consumers to subscribe to a topic or partition log.

### Request Payload
```json
{
  "action": "consume",
  "topic": "orders",
  "group_id": "analytics-group",
  "consumer_id": "c-1"
}
```

### Response Payload
Messages are pushed from the broker as they arrive:
```json
{
  "status": "ok",
  "topic": "orders",
  "payload": {"item": "A", "price": 100},
  "offset": 45,
  "partition": 1
}
```

---

## Status Request

Sent to query diagnostics and statistics from the broker.

### Request Payload
```json
{
  "action": "status"
}
```

### Response Payload
```json
{
  "status": "ok",
  "stats": {
    "topics": {
      "orders": 3
    },
    "consumers": [
      "analytics-group",
      "c-1"
    ],
    "offsets": {
      "orders-0": 12,
      "orders-1": 46
    },
    "total_messages": 58,
    "messages_sec": 0,
    "connected_producers": 0,
    "partition_ownership": {
      "analytics-group": {
        "orders": {
          "c-1": [0, 1, 2]
        }
      }
    }
  }
}
```
