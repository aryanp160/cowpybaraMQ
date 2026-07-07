# Cowpybara

A distributed log-based message broker inspired by Apache Kafka, built in Python to explore distributed systems, replication, partitions, consumer groups, and fault tolerance.

---

[![Python](https://img.shields.io/badge/python-3.11%2B-blue?style=flat-square&logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)
[![Version](https://img.shields.io/badge/version-v3.0.0-blueviolet?style=flat-square)](https://github.com/aryanp160/cowpybaraMQ)
[![Build Status](https://github.com/aryanp160/cowpybaraMQ/actions/workflows/ci.yml/badge.svg?branch=main&style=flat-square)](https://github.com/aryanp160/cowpybaraMQ/actions)
[![Last Commit](https://img.shields.io/github/last-commit/aryanp160/cowpybaraMQ?style=flat-square)](https://github.com/aryanp160/cowpybaraMQ/commits/main)

---

## Table of Contents
- [Project Overview](#project-overview)
- [Feature Matrix](#feature-matrix)
- [System Architecture](#system-architecture)
- [Sequence Diagrams](#sequence-diagrams)
- [Failure Scenarios](#failure-scenarios)
- [Benchmarks](#benchmarks)
- [Screenshots & Demo](#screenshots--demo)
- [Project Structure](#project-structure)
- [Configuration Reference](#configuration-reference)
- [Protocol Specification](#protocol-specification)
- [Design Decisions](#design-decisions)
- [Comparison Matrix](#comparison-matrix)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Testing Guide](#testing-guide)
- [License](#license)

---

## Project Overview

### Why Cowpybara Exists
Cowpybara was built as an educational tool to demystify the inner workings of modern distributed log-based event systems like Apache Kafka. It showcases how to handle concurrent connections over TCP, maintain strict ordering constraints across partitions, load balance dynamic consumer groups, and coordinate failover replication without relying on external heavy-weight frameworks.

### What Problems Message Brokers Solve
In microservice architectures, systems need to communicate reliably without direct, synchronous HTTP/gRPC coupling. Message brokers act as intermediate coordinators, enabling:
- **Asynchronous Communication**: Services publish events and proceed without blocking on consumer processing.
- **Backpressure Management**: Slow consumers can read messages at their own pace without exhausting system resources.
- **Fault Isolation**: If a consumer service goes down, messages are queued or persisted in the broker until it recovers.

### Queue Systems vs. Log-Based Brokers
Traditional queues (e.g., RabbitMQ) delete messages immediately after consumer acknowledgment, supporting transient queues. 

Log-based brokers (e.g., Apache Kafka, Cowpybara) treat topics as ordered, append-only commit logs on disk. Messages persist regardless of consumption status, allowing multiple independent consumer groups to replay history from arbitrary offsets.

### Cowpybara vs. Apache Kafka
While Kafka utilizes ZooKeeper/KRaft, complex JVM memory tuning, and custom page caches, Cowpybara is designed for educational accessibility:
- **Simplicity**: Written in lightweight async Python.
- **Accessibility**: Zero external infrastructure dependencies (e.g. no JVM, no external database).
- **Core Concepts**: Implements partitions, thread-safe persistent offsets, leader election, and multi-node TCP replication.

---

## Feature Matrix

| Feature | Description | Status |
| :--- | :--- | :---: |
| **TCP Broker** | Lightweight asynchronous TCP socket server handling concurrent clients. | **Supported** |
| **Persistent Logs** | Append-only partition storage on disk using JSONL formatting. | **Supported** |
| **Topics & Partitions** | Topic segment partitioning with key-based CRC32 routing. | **Supported** |
| **Consumer Groups** | Dynamic partition load-balancing using Round-Robin rebalances. | **Supported** |
| **Committed Offsets** | Thread-safe offset tracking persisting to local storage files. | **Supported** |
| **Replication** | Asynchronous TCP log replication from Leaders to Followers. | **Supported** |
| **Leader Election** | Bully-style automatic election selecting active broker with highest ID. | **Supported** |
| **ACK Modes** | Customizable produce write safety (`acks=0`, `acks=1`, `acks=all`). | **Supported** |
| **Simulation Utility** | CLI suite to inject broker failures, disconnections, and recoveries. | **Supported** |

---

## System Architecture

### 1. Overall Cluster Topology
Shows the multi-broker network routing traffic from producers and coordinating replication.

```mermaid
graph TD
    subgraph Producers & Consumers
        Producer[Producers] -->|PRODUCE| Leader[Leader Broker 9094]
        Consumer[Consumer Groups] -->|CONSUME| Leader
    end
    
    subgraph Cluster Topology
        Leader -->|Asynchronous Replication| FollowerA[Follower Broker 9093]
        Leader -->|Asynchronous Replication| FollowerB[Follower Broker 9092]
    end
```

### 2. Broker Components Internals
Visualizes the internal modules handling socket lines and disk writes.

```mermaid
graph LR
    Server[networking.Server] -->|Parse Request| Protocol[protocol.Parser]
    Protocol -->|Route Action| Broker[broker.Broker]
    
    subgraph Broker Core
        Broker -->|Publish / Read| Storage[storage.Storage]
        Broker -->|Commit / Load| Offsets[offsets.OffsetManager]
        Broker -->|Heartbeats & Election| Cluster[cluster.ClusterManager]
        Broker -->|Broadcasts| Replication[replication.ReplicationManager]
    end
```

### 3. Storage Layer Architecture
How partitioned JSONL logs map to segments on the local filesystem.

```mermaid
graph TD
    subgraph Storage Directory
        Topic[logs-9092 / topic-name] --> Part0[topic-name-0.jsonl]
        Topic --> Part1[topic-name-1.jsonl]
        Topic --> Part2[topic-name-2.jsonl]
    end
```

---

## Sequence Diagrams

### 1. Producer → Broker (ACKS=all)
Shows replication confirmation from followers before leader responds.

```mermaid
sequenceDiagram
    autonumber
    actor Producer
    participant Leader
    participant Follower
    
    Producer->>Leader: PRODUCE (topic, payload, acks=all)
    Leader->>Leader: Write to local log
    Leader->>Follower: ReplicateRequest (payload, offset)
    Follower->>Follower: Write to local log
    Follower-->>Leader: ReplicateAckRequest
    Leader-->>Producer: Return OK (partition, offset)
```

### 2. Heartbeat & Leader Election
Shows election trigger on heartbeat loss.

```mermaid
sequenceDiagram
    autonumber
    participant Follower 9093
    participant Follower 9094 (Candidate)
    
    Note over Follower 9093, Follower 9094: HeartbeatTimeout elapsed
    Follower 9094->>Follower 9094: Trigger election
    Follower 9094->>Follower 9093: ElectRequest (candidate_id=9094)
    Follower 9093-->>Follower 9094: OK (broker_id=9093)
    Note over Follower 9094: 9094 is highest active broker. Promotes to LEADER.
```

### 3. Consumer Group Join & Partition Rebalance
Dynamic assignment of partitions on consumer register.

```mermaid
sequenceDiagram
    autonumber
    actor Consumer1
    actor Consumer2
    participant Broker
    participant GroupManager
    
    Consumer1->>Broker: JOIN group-a
    Broker->>GroupManager: Register Consumer1
    GroupManager->>Broker: Assign Partitions [0, 1, 2] to Consumer1
    
    Consumer2->>Broker: JOIN group-a
    Broker->>GroupManager: Rebalance group-a
    GroupManager->>Broker: Assign Partitions [0, 1] to Consumer1, [2] to Consumer2
    Broker->>Consumer1: Rebalance Notification
    Broker->>Consumer2: Rebalance Notification
```

---

## Failure Scenarios

### Leader Crashes
- **Problem**: The primary broker serving writes crashes.
- **Detection**: Followers miss periodic heartbeats exceeding `HEARTBEAT_TIMEOUT`.
- **Recovery**: Active followers broadcast `ElectRequest`. The active broker with the highest `broker_id` promotes itself to Leader.
- **Expected Behaviour**: Client producers receive connection errors, reconnect to the new leader, and resume publishing. Standalone and group consumers reconnect and resume fetching.

### Follower Crashes
- **Problem**: A replica broker crashes.
- **Detection**: The Leader detects socket disconnects and drops the follower writer from the replication pool.
- **Recovery**: Upon restart, the recovered follower registers via `RegisterFollowerRequest` sending its current partition offsets.
- **Expected Behaviour**: The leader streams missing historical logs to catch the follower up.

### Offset Corruption
- **Problem**: The offset tracking JSON file becomes corrupted.
- **Detection**: JSON parsing error on startup.
- **Recovery**: Corrupted offset entries are skipped, resetting the affected consumer group offset to `0` (or end of log).
- **Expected Behaviour**: Consumer group replays partition logs from the beginning.

---

## Benchmarks

Benchmarks executed locally using `cmd/benchmark.py` (500 payload iterations with 100-byte entries):

| Operation | Throughput (msgs/sec) | Latency p99 (ms) |
| :--- | :--- | :--- |
| **PRODUCE (acks=0)** | ~90,691.58 msgs/sec | 0.51 ms |
| **PRODUCE (acks=1)** | ~670.74 msgs/sec | 8.59 ms |
| **CONSUME** | ~26,114.19 msgs/sec | < 0.05 ms |

### Testing Methodology
Metrics were measured using a dynamic leader-follower subprocess network. Producers send a 100-byte structured JSON payload. Disk write latencies reflect local NVMe SSD append operations.

---

## Screenshots & Demo

Below are placeholders for the visualization dashboard and terminals:

### Broker Dashboard
```text
+-------------------------------------------------------+
|              COWPYBARAMQ CLUSTER STATUS               |
+-------------------------------------------------------+
| Current Leader: 9094 (127.0.0.1:9094)                 |
| Followers connected: [9093, 9092]                     |
| Replication Latency: Avg 1.2ms                        |
| Total messages: 14022 | Throughput: 1300 msgs/sec     |
+-------------------------------------------------------+
```

### Producer CLI
`[PRODUCE] Topic: orders | Payload: '{"msg": "pay"}' | Port: 9094 -> OK (Partition 0, Offset 42)`

---

## Project Structure

```text
cowpybaraMQ/
├── cmd/
│   ├── broker.py          # Broker server entry point
│   ├── producer.py        # Produce command utility
│   ├── consumer.py        # Consumer subscription tool
│   ├── status.py          # Diagnostics tool
│   ├── benchmark.py       # Cluster benchmarking engine
│   └── cluster_admin.py   # Observing and Simulating cluster failover states
├── internal/
│   ├── broker.py          # Coordinates message flow, loops and metrics
│   ├── cluster.py         # ClusterManager, heartbeat tracking, elections
│   ├── config.py          # Environment settings loader
│   ├── groups.py          # Consumer group partition assignment coordinator
│   ├── networking.py      # TCP server socket management loops
│   ├── offsets.py         # Persistent commit offset file managers
│   ├── partition.py       # JSONL partition file readers and writers
│   ├── protocol.py        # Network line parser and protocol dataclasses
│   ├── replication.py     # Asymmetric peer-to-peer sync engine
│   └── storage.py         # Topics directory layout mapper
└── tests/                 # Integration, stress, and unit testing scripts
```

---

## Configuration Reference

The broker reads the following settings from environment variables or CLI arguments:

| Argument | Env Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `--port` | `COWPYBARA_PORT` | `9092` | TCP Server listening port |
| `--role` | `COWPYBARA_ROLE` | `leader` | Node role (`leader` or `follower`) |
| `--leader-host`| `COWPYBARA_LEADER_HOST`| `127.0.0.1` | Leader host address to connect to |
| `--leader-port`| `COWPYBARA_LEADER_PORT`| `9092` | Leader port to connect to |
| `--broker-id` | `COWPYBARA_BROKER_ID` | `PORT` | Unique cluster identification number |
| `--cluster-members`| `COWPYBARA_CLUSTER_MEMBERS`| `127.0.0.1:9092...` | Cluster topology registry |

---

## Protocol Specification

All socket packets are **newline-delimited JSON strings** over raw TCP.

### 1. PRODUCE Request
```json
{"action": "produce", "topic": "orders", "payload": {"val": 42}, "key": "user_1", "acks": "all"}
```
### 2. PRODUCE Response
```json
{"status": "ok", "partition": 0, "offset": 12}
```
### 3. Heartbeat Frame
```json
{"action": "heartbeat", "sender_id": "9092", "role": "leader"}
```

---

## Design Decisions

### Append-Only JSONL Logs
- **Decision**: Persist partition logs in JSON Lines format (`.jsonl`).
- **Trade-off**: Simple to read, debug, and parse sequentially. However, it consumes more disk space compared to raw binary logs (like Kafka Index files).

### Asynchronous Heartbeats
- **Decision**: Exchange heartbeats asynchronously outside the storage write path.
- **Trade-off**: Keeps latency of producing messages low, but can result in transient split-brain if network isolation occurs before election checks complete.

---

## Comparison Matrix

| Feature | Cowpybara | Apache Kafka | RabbitMQ | NATS |
| :--- | :---: | :---: | :---: | :---: |
| **Log-Based** | Yes | Yes | No (Queue) | No (Queue/JetStream)|
| **Ordering** | Partition | Partition | Queue | Stream |
| **Broker Replication**| Yes | Yes | Yes | Yes |
| **Dependencies** | None | KRaft/ZooKeeper| Erlang VM | Go Runtime |

---

## Roadmap

- [ ] **Consensus Protocol**: Migrate simplified Bully election to full Raft Consensus.
- [ ] **Zero-Copy Performance**: Integrate binary memory buffers to bypass JSON parser loops.
- [ ] **SSL/TLS**: Implement encrypted secure TCP transmission.
- [ ] **Tiered Storage**: Automatically compress and archive cold logs to S3.

---

## Contributing

We welcome contributions! Please follow these standards:
- **Code Style**: Ensure python files conform to `black` formatting standards.
- **Linter**: Verify no errors are reported by `flake8 .`.
- **Tests**: All tests in `tests/` must pass cleanly.

---

## Testing Guide

Cowpybara contains a comprehensive testing suite under `tests/`:
- **`test_broker.py`**: Validates basic JSON parser logic and socket connections.
- **`test_cluster_integration.py`**: Verifies dynamic failovers, crash simulation, disconnects, and ACK modes (`acks=0/1/all`).
- **`test_replication.py`**: Checks replication logs duplication guard.

Run the test suite locally:
```bash
pytest
```

---

## License

This project is licensed under the terms of the MIT License. See [LICENSE](LICENSE) for details.
