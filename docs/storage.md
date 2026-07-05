# Storage Documentation

CowpybaraMQ persists all published messages sequentially to disk using an append-only structure.

## Log Segments

For each topic, the storage layer creates physical log segment files:
- `orders-0.jsonl`
- `orders-1.jsonl`
- `orders-2.jsonl`

JSONL (JSON Lines) was chosen for simple newline parsing and robust diagnostics.

## Appends

Every append operation:
1. Determines the next offset index for that partition by counting existing lines on initialization or incrementing an in-memory counter.
2. Writes the JSON payload as a single line:
   ```json
   {"offset": 45, "message": {"item": "A"}}
   ```
3. Flushes the line to guarantee persistence.

## Persistence Recovery

Upon restarting the broker:
- The storage layer discovers all `*.jsonl` files in the log directory.
- It parses the topic name and partition ID from the filename.
- It scans each file to determine the correct `next_offset` index, ensuring no historical messages are overwritten.
