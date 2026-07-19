# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [5.0.0] - 2026-07-18
### Added
- **E2E Distributed Cluster Testing Framework**: Automated 3-broker orchestration suite testing leader election, partition routing, replication, failover, consumer group rebalancing, and crash recovery.
- **Setuptools & Ruff Integration**: Modernized package building configuration in `pyproject.toml` and verified automated formatting, syntax checks, and matrix pipelines.
- **Docker Compose Setup**: Spawns a fully integrated 3-broker distributed replication cluster with a single command.

## [4.0.0] - 2026-07-17
### Added
- **Automatic Crash Recovery**: Scan persistent directories, restore partition state, replay logs, and recover offsets automatically on boot.
- **CRC32 Checksum Verification**: Enforce data integrity per log record, skipping corrupted or truncated lines gracefully.
- **Production Graceful Shutdown**: Flush partitions, offsets, and metadata to disk using `os.fsync` upon catching `SIGINT`/`SIGTERM`.

## [3.0.0] - 2026-07-15
### Added
- **Distributed TCP Replication**: Support async peer-to-peer sync engine between partition leaders and followers.
- **Bully Leader Election**: Automatic cluster coordination and follower promotion on leader heartbeat timeouts.
- **ACK Modes**: Support `acks=0` (fire & forget), `acks=1` (local confirmation), and `acks=all` (quorum synchronization).

## [2.0.0] - 2026-07-11
### Added
- **Consumer Groups & Rebalancing**: Load balance dynamic partition allocations among consumers under the same group.
- **Committed Offsets Persistence**: Auto-save partition read progress on client consumer disconnect.

## [1.0.0] - 2026-06-29
### Added
- **Persistent Storage Log**: Append-only topic segments on disk using JSONL formatting.
- **Lightweight TCP Server**: Socket handling loop parsing client request protocol.
