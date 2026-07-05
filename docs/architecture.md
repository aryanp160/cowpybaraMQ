# Architecture Documentation

CowpybaraMQ is a distributed log-based message broker written from scratch in Python. It follows a multi-producer, multi-consumer model inspired by Apache Kafka.

## Components Overview

```mermaid
graph TD
    P1[Producer 1] -->|TCP JSON| Broker[CowpybaraMQ Broker]
    P2[Producer 2] -->|TCP JSON| Broker
    Broker -->|TCP JSON| C1[Consumer 1]
    Broker -->|TCP JSON| C2[Consumer 2]
    
    subgraph Storage Layer
        Broker -->|Append| Partition0[orders-0.jsonl]
        Broker -->|Append| Partition1[orders-1.jsonl]
        Broker -->|Append| Partition2[orders-2.jsonl]
    end
    
    subgraph Metadata & Offsets
        Broker -->|Update Offsets| Offsets[group_offsets.json]
    end
```

### 1. TCP Server & Protocol Layer
The broker listens on a customizable port (default `9092`) using `asyncio.start_server`. It handles concurrent TCP client connections. All communication uses newline-delimited JSON strings.

### 2. Central Broker Logic
The `Broker` class manages state, routing messages to storage partitions and streaming them to active subscribers.

### 3. Storage Layer
Topics are divided into physical partition log segments named `<topic>-<partition>.jsonl`. The storage layer manages deterministic routing of messages to partitions based on hashing keys.

### 4. Consumer Groups & Partition Assigner
`GroupManager` tracks active consumer registrations per group and distributes topic partitions round-robin among connected members.
