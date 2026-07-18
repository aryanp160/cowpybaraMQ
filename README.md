# CowpybaraMQ

A distributed log-based message broker inspired by Apache Kafka, built in Python to explore distributed systems, replication, partitions, consumer groups, and fault tolerance.

---

[![Python](https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square&logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)
[![Build Status](https://github.com/aryanp160/cowpybaraMQ/actions/workflows/ci.yml/badge.svg?branch=main&style=flat-square)](https://github.com/aryanp160/cowpybaraMQ/actions)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/psf/black)
[![Linter](https://img.shields.io/badge/linter-ruff-red?style=flat-square)](https://github.com/astral-sh/ruff)

---

## Table of Contents
- [Project Overview](#project-overview)
- [Feature Matrix](#feature-matrix)
- [System Architecture](#system-architecture)
- [Sequence Diagrams](#sequence-diagrams)
- [Recovery Workflow](#recovery-workflow)
- [Deployment & Setup](#deployment--setup)
- [Benchmarks](#benchmarks)
- [Troubleshooting & Diagnostics](#troubleshooting--diagnostics)
- [Failure Scenarios](#failure-scenarios)
- [Operational Best Practices](#operational-best-practices)
- [Protocol Specification](#protocol-specification)
- [Design Decisions](#design-decisions)
- [Comparison Matrix](#comparison-matrix)
- [Roadmap](#roadmap)
- [License](#license)

---

## Project Overview

### Why CowpybaraMQ Exists
CowpybaraMQ was built as a clean-room implementation to demystify distributed log-based event systems like Apache Kafka. It demonstrates concurrent TCP socket networking, partition ordering guarantees, dynamic consumer group coordination, automatic Bully leader elections, and fault-tolerant disk crash recovery without external heavy-weight dependencies.

### Queue Systems vs. Log-Based Brokers
- **Queue Systems (e.g., RabbitMQ)**: Destructively consume messages once acknowledged. Suitable for transient work coordination.
- **Log-Based Brokers (e.g., Kafka, CowpybaraMQ)**: Treat topics as append-only, immutable disk logs. Messages persist regardless of consumption state. Multiple consumer groups can independently read, replay, or jump offsets at will.

---

## Feature Matrix

| Feature | Description | Status |
| :--- | :--- | :---: |
| **TCP Broker** | Lightweight async TCP socket server handling concurrent clients. | **Supported** |
| **Persistent Logs** | Append-only partition storage with CRC32 checksums on disk. | **Supported** |
| **Topics & Partitions** | Segments partition topics with key-based CRC32 routing. | **Supported** |
| **Consumer Groups** | Dynamic partition load-balancing using rebalance protocols. | **Supported** |
| **Offset Persistence** | Thread-safe committed offset tracking written to local storage. | **Supported** |
| **TCP Replication** | Async log replication from Leaders to Followers. | **Supported** |
| **Leader Election** | Bully-style automatic election selecting active broker with highest ID. | **Supported** |
| **ACK Modes** | Customizable produce write safety (`acks=0`, `acks=1`, `acks=all`). | **Supported** |
| **Automatic Recovery** | Reconstructs cluster state and offsets from disk logs on restart. | **Supported** |
| **Checksum Validation** | Automatically skips corrupted/truncated writes on replay. | **Supported** |
| **CI/CD Pipeline** | Automated Black, Ruff, and pytest matrices in GitHub Actions. | **Supported** |

---

## System Architecture

### 1. Cluster Topology & Replication
Producers write to partition leaders, which asynchronously replicate payloads to followers.

```mermaid
graph TD
    subgraph Client Layer
        Producer[Producers] -->|PRODUCE| Leader[Leader Broker: 9092]
        Consumer[Consumer Groups] -->|CONSUME| Leader
    end
    
    subgraph Broker Cluster
        Leader -->|Asynchronous Replication| FollowerA[Follower Broker: 9093]
        Leader -->|Asynchronous Replication| FollowerB[Follower Broker: 9094]
    end
```

### 2. Internal Broker Component Layout
Visualizes the internal async components handling socket parsing and disk IO.

```mermaid
graph LR
    Server[networking.Server] -->|Parse| Protocol[protocol.Parser]
    Protocol -->|Route| Broker[broker.Broker]
    
    subgraph Broker Core
        Broker -->|Append| Storage[storage.Storage]
        Broker -->|Commit| Offsets[offsets.OffsetManager]
        Broker -->|Coordinating| Cluster[cluster.ClusterManager]
        Broker -->|Broadcasts| Replication[replication.ReplicationManager]
    end
```

---

## Sequence Diagrams

### 1. Write Replication with ACKS=all
```mermaid
sequenceDiagram
    autonumber
    actor Producer
    participant Leader
    participant Follower
    
    Producer->>Leader: PRODUCE (topic, payload, acks=all)
    Leader->>Leader: Append to local log & calc CRC32
    Leader->>Follower: ReplicateRequest (payload, offset)
    Follower->>Follower: Write to local log & verify CRC32
    Follower-->>Leader: ReplicateAckRequest (offset)
    Leader-->>Producer: Return OK (partition, offset)
```

### 2. Leader Election Failover
```mermaid
sequenceDiagram
    autonumber
    participant Follower 9093
    participant Follower 9094 (Candidate)
    
    Note over Follower 9093, Follower 9094: HeartbeatTimeout elapsed (Leader down)
    Follower 9094->>Follower 9094: Trigger election
    Follower 9094->>Follower 9093: ElectRequest (candidate_id=9094)
    Follower 9093-->>Follower 9094: OK (broker_id=9093)
    Note over Follower 9094: 9094 has highest active ID. Promotes to LEADER.
```

---

## Recovery Workflow

During startup, the broker executes a multi-phase automatic recovery sequence:

```mermaid
graph TD
    Start[Broker Startup] --> Scan[Scan Storage Directory]
    Scan --> LoadParts[Reconstruct Topics & Partitions]
    LoadParts --> Replay[Replay append-only JSONL logs]
    Replay --> CRC[Validate CRC32 Checksum per record]
    CRC -->|Valid| LoadRec[Load Record into Memory]
    CRC -->|Corrupted/Truncated| SkipRec[Skip corrupted entry gracefully & log alert]
    LoadRec --> LoadOffsets[Restore committed consumer offsets]
    SkipRec --> LoadOffsets
    LoadOffsets --> Cluster[Resume replication metadata & join cluster]
```

---

## Deployment & Setup

### Local Installation
1. Clone the repository and install it in editable mode:
   ```bash
   git clone https://github.com/aryanp160/cowpybaraMQ.git
   cd cowpybaraMQ
   pip install -e .
   ```

### Command Line Usage
- **Start Leader Broker**:
  ```bash
  python cmd/broker.py --port 9092 --role leader --broker-id 9092 --cluster-members "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"
  ```
- **Start Follower Broker**:
  ```bash
  python cmd/broker.py --port 9093 --role follower --broker-id 9093 --leader-port 9092 --cluster-members "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"
  ```

### Docker Compose Cluster Setup
Deploy a pre-configured 3-node cluster in one command:
```bash
docker-compose up --build
```
This spawns:
- `broker-1` (Leader at port `9092`)
- `broker-2` (Follower at port `9093`)
- `broker-3` (Follower at port `9094`)

---

## Benchmarks

Evaluating startup recovery times and replication throughput:

- **Startup Recovery Time (Graceful vs Forced)**: ~3.7 ms
- **Repeated Crash Cycles (5 iterations)**: ~0.6 ms recovery time
- **Checksum Corrupted Recovery**: ~5.7 ms (detects and ignores corrupted JSON lines, restoring all valid segments)
- **Cluster Failover Promotion Latency**: ~799.5 ms (leader termination to new leader election)
- **Concurrent Recovery Throughput**: ~2.6 ms recovery under active concurrent load

---

## Troubleshooting & Diagnostics

### 1. Checksum Corruption Warning
- **Symptoms**: Logs display `WARNING - Skip corrupted record at offset X`.
- **Cause**: Partial write on forced termination or disk storage bit rot.
- **Resolution**: No manual action needed. The broker skips the line and recovers.

### 2. Follower Synchronization Latency
- **Symptoms**: `Replication for offset X timed out`.
- **Cause**: Network congestion or follower disk IO bottleneck.
- **Resolution**: Lower `--heartbeat-interval` to check connectivity or partition logs manually.

---

## Failure Scenarios

- **Leader Crash**: Heartbeat timeout triggers Bully election, promoting the highest ID active follower.
- **Follower Crash**: Leader removes follower from broadcast list. Upon restart, the follower registers and replays missing offsets to catch up.
- **Truncated Writes**: Replay checks CRC32 checksums, dropping partial records gracefully during recovery.

---

## Operational Best Practices

1. **Heartbeat Tuning**: Use shorter timeouts (`--heartbeat-timeout 0.3`) in local testing and longer ones (`1.5s - 3s`) in virtual networks to prevent split-brain.
2. **Compression**: Enable Gzip (`--compression-type gzip`) on large text/JSON payloads above `512` bytes to save network bandwidth.
3. **Partition Allocation**: Match the number of partitions to the number of concurrent consumers in your group.

---

## Protocol Specification

Socket messages are newline-delimited JSON strings over raw TCP.

### PRODUCE Request
```json
{"action": "produce", "topic": "orders", "payload": {"val": 42}, "key": "user_1", "acks": "all"}
```
### CONSUME Request
```json
{"action": "consume", "topic": "orders", "group_id": "group-1", "consumer_id": "c-1"}
```

---

## Design Decisions

- **JSONL Disk Storage**: Simplifies debugging and readability at the cost of higher disk footprint than binary formats.
- **Bully Election Algorithm**: Fast and simple ID-based coordination, ideal for static cluster sizes.

---

## Comparison Matrix

| Feature | CowpybaraMQ | Apache Kafka | RabbitMQ | NATS |
| :--- | :---: | :---: | :---: | :---: |
| **Log-Based** | Yes | Yes | No (Queue) | No (Queue/JetStream)|
| **Ordering** | Partition | Partition | Queue | Stream |
| **Dependencies** | None | KRaft/ZooKeeper| Erlang VM | Go Runtime |

---

## Roadmap

- [x] **Automatic Crash Recovery**: Scan storage and restore offsets/states on boot.
- [x] **CRC32 Checksum Validation**: Verify segment lines integrity.
- [x] **Production CI Pipelines**: Automated tests and matrix builds.
- [ ] **Raft Consensus Integration**: Support dynamic topology scaling via Raft consensus.
- [ ] **Zero-Copy Serialization**: Byte-buffer zero-copy writes using memory views.

---

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
