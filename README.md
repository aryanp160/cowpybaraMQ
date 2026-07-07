# CowpybaraMQ

![CI](https://github.com/aryanp160/CowpybaraMQ/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-stable-brightgreen)

A distributed log-based message broker inspired by Apache Kafka, built from scratch in Python.

---

## Features

- **Producer/Consumer Model**: Non-blocking asynchronous message publishing and subscription over raw TCP sockets.
- **Consumer Groups**: Scalable consumption with round-robin partition distribution among group members.
- **Persistent Offsets**: Thread-safe group commit offset storage persisting to disk (`storage/group_offsets.json`).
- **Topic Partitioning**: Support for hashing message keys to determine target partition segments (`orders-0.jsonl`, etc.) preserving partition-level ordering.
- **Automatic Rebalancing**: Dynamic workload reallocation notifying active consumers immediately when members join or leave a group.
- **Monitoring Utilities**: Real-time stats engine and throughput metrics utility (`status`) tracking message rate (msgs/sec) and partition ownership.
- **Persistent Logs**: Structured JSON Lines (JSONL) append-only logging recovering state smoothly after broker restarts.

---

## Architecture

CowpybaraMQ follows a multi-producer, multi-consumer model inspired by Apache Kafka. It is structured into the following layers:

1. **TCP Server & Protocol Layer**: Listens on port `9092` using `asyncio` TCP sockets. Decodes newline-delimited JSON commands using a lightweight protocol parser.
2. **Central Broker**: Coordinates the delivery pipeline, dynamically subscribing consumers to partitions and dispatching active publish events.
3. **Storage Layer**: Divides topics into physical partition segments (`<topic>-<partition>.jsonl`) using key-based hash routing.
4. **Consumer Groups**: Rebalances partitions round-robin among group members upon client join/leave events.

### System Topology

```mermaid
graph TD
    Producer[Producers] -->|Publish payload, key| Broker[CowpybaraMQ Broker]
    Broker -->|Assign partition / stream| Consumer[Consumer Groups]
    
    subgraph Storage Layer
        Broker -->|Write| Part0[topic-0.jsonl]
        Broker -->|Write| Part1[topic-1.jsonl]
        Broker -->|Write| Part2[topic-2.jsonl]
    end
    
    subgraph Metadata
        Broker -->|Commit Offsets| Offsets[group_offsets.json]
    end
```

### Messaging Sequence

```mermaid
sequenceDiagram
    autonumber
    actor Producer
    participant Broker
    participant Storage
    participant GroupManager
    actor Consumer
    
    Producer->>Broker: PRODUCE (topic, payload, key)
    Broker->>Storage: Append message to partition segment log
    Storage-->>Broker: Return (partition_id, offset)
    Broker-->>Producer: Return OK response with partition/offset details
    Broker->>GroupManager: Check partition assignments
    Broker->>Consumer: Push message payload over TCP connection
    Consumer->>Broker: Acknowledge & update group offset
```

---

## Folder Structure

```text
cowpybaraMQ/
├── cmd/
│   ├── broker.py          # Asynchronous TCP Broker Server
│   ├── producer.py        # Producer CLI Utility
│   ├── consumer.py        # Consumer CLI Utility
│   └── status.py          # Status Diagnostics Monitor
├── docs/                  # In-depth architectural/protocol documentation
│   ├── architecture.md
│   ├── protocol.md
│   ├── storage.md
│   ├── consumer-groups.md
│   └── partitions.md
├── internal/              # Core modules (protocol parser, partition managers)
├── logs/                  # Storage directories for partitioned JSONL segments
├── tests/                 # Comprehensive Pytest suite (integration, unit, stress)
├── pyproject.toml         # Python dev tooling setup (Black)
├── requirements-dev.txt   # Development dependencies
└── requirements.txt       # Core dependencies
```

---

## Running Locally

### 1. Start the Broker
Run the asynchronous TCP broker:
```bash
python cmd/broker.py
```

### 2. Launch Status Diagnostics
Verify broker state:
```bash
python cmd/status.py
```

### 3. Consume from Topic Group
Run a consumer group member to consume messages:
```bash
python cmd/consumer.py --topic orders --group-id analytics-group --consumer-id c-1
```

### 4. Publish Partitioned Messages
Publish a message using key hashing routing:
```bash
python cmd/producer.py --topic orders --message "Order payload details" --key "user_abc"
```

---

## Benchmarks

Measurements conducted locally under 1000 messages load:

| Operation | Throughput | Avg Latency |
| :--- | :--- | :--- |
| **PRODUCE** | ~1,331 msgs/sec | 0.75 ms |
| **CONSUME** | ~21,318 msgs/sec | < 0.05 ms |

---

## Roadmap

- [x] **V1 Base Broker**: Basic TCP server and single-broker sequential logging.
- [x] **V2 Partitions & Rebalancing**: Dynamic partition segment logs, group rebalance triggers, and status diagnostics.
- [x] **V2 Partitions & Rebalancing**: Dynamic partition segment logs, group rebalance triggers, and status diagnostics.
- [x] **V3 Replication**: Peer-to-peer broker cluster setups, heartbeat-based leader election, configurable producer ACKs, replication logs, and failover simulation.

---

## Replication and Failover Diagrams

### 1. Normal Replication (acks=all)

```mermaid
sequenceDiagram
    autonumber
    actor Producer
    participant Leader
    participant Follower
    
    Producer->>Leader: PRODUCE (topic, payload, acks=all)
    Leader->>Leader: Append locally
    Leader->>Follower: ReplicateRequest (payload, offset)
    Follower->>Follower: Append replicated message
    Follower-->>Leader: ReplicateAckRequest (offset)
    Leader-->>Producer: Return OK response
```

### 2. Leader Failure & Heartbeat Timeout

```mermaid
sequenceDiagram
    autonumber
    participant Follower 9093
    participant Follower 9094 (Candidate)
    
    Note over Follower 9093, Follower 9094: Leader dies / Heartbeat timeout occurs
    Follower 9094->>Follower 9094: Trigger election
    Follower 9094->>Follower 9093: ElectRequest (candidate_id=9094)
    Follower 9093-->>Follower 9094: Return OK (broker_id=9093)
    Note over Follower 9094: ID 9094 is highest active ID. Promotes itself to LEADER.
```

### 3. Recovery & Resynchronization

```mermaid
sequenceDiagram
    autonumber
    participant New Leader 9094
    participant Recovered Broker 9092 (Follower)
    
    Note over Recovered Broker 9092: Restored/Recovered broker starts up
    Recovered Broker 9092->>New Leader 9094: RegisterFollowerRequest (current offsets)
    New Leader 9094-->>Recovered Broker 9092: Stream missing historical messages
    Note over Recovered Broker 9092: Resynchronized and listening for new replication broadcasts
```

---

## Contributing

We welcome contributions! Please open issues or submit pull requests. Ensure all code satisfies formatting (`black .`) and passes all linting tests (`flake8 .`).
