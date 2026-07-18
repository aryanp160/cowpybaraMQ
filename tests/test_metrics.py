import pytest

from internal.metrics import MetricsManager
from internal.storage import Storage


class DummyGroupManager:
    def __init__(self):
        self.lock = DummyLock()
        self.assignments = {"group-A": {"test-topic": []}}
        self.offsets = {"group-A": {"test-topic-0": 2}}

    def get_offset(self, group_id, tp):
        return self.offsets.get(group_id, {}).get(f"{tp.topic}-{tp.partition}", 0)


class DummyLock:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class DummyBroker:
    def __init__(self, storage):
        self.storage = storage
        self.consumers = {"test-topic": [object()]}
        self.group_queues = {"group-A": {"test-topic": {"c1": object()}}}
        self.group_manager = DummyGroupManager()


@pytest.mark.unit
def test_metrics_initial_state():
    metrics = MetricsManager()
    snap = metrics.snapshot()
    assert snap["broker"]["messages_produced"] == 0
    assert snap["broker"]["messages_consumed"] == 0
    assert snap["broker"]["bytes_written"] == 0
    assert snap["broker"]["bytes_read"] == 0
    assert snap["broker"]["replication_events"] == 0
    assert snap["broker"]["leader_changes"] == 0
    assert snap["performance"]["average_produce_latency_ms"] == 0.0
    assert snap["performance"]["average_consume_latency_ms"] == 0.0


@pytest.mark.unit
def test_metrics_recording():
    metrics = MetricsManager()
    metrics.record_produce(100, 5.0)
    metrics.record_produce(150, 10.0)
    metrics.record_consume(200, 2.0)
    metrics.record_replication()
    metrics.record_leader_change()

    snap = metrics.snapshot()
    assert snap["broker"]["messages_produced"] == 2
    assert snap["broker"]["bytes_written"] == 250
    assert snap["broker"]["messages_consumed"] == 1
    assert snap["broker"]["bytes_read"] == 200
    assert snap["broker"]["replication_events"] == 1
    assert snap["broker"]["leader_changes"] == 1
    assert snap["performance"]["average_produce_latency_ms"] == 7.5
    assert snap["performance"]["average_consume_latency_ms"] == 2.0


@pytest.mark.unit
def test_metrics_reset():
    metrics = MetricsManager()
    metrics.record_produce(100, 5.0)
    metrics.record_consume(200, 2.0)
    metrics.record_replication()
    metrics.record_leader_change()

    metrics.reset()
    snap = metrics.snapshot()
    assert snap["broker"]["messages_produced"] == 0
    assert snap["broker"]["bytes_written"] == 0
    assert snap["broker"]["messages_consumed"] == 0
    assert snap["broker"]["bytes_read"] == 0
    assert snap["broker"]["replication_events"] == 0
    assert snap["broker"]["leader_changes"] == 0


@pytest.mark.unit
def test_metrics_broker_integration(tmp_path):
    storage = Storage(log_dir=tmp_path)
    storage.num_partitions = 1
    # Append some messages to increase next_offset of test-topic partition 0 to 5
    partition = storage.get_partition("test-topic", 0)
    for i in range(5):
        partition.append({"val": i})

    broker = DummyBroker(storage)
    metrics = MetricsManager(broker)

    snap = metrics.snapshot()
    assert snap["consumers"]["active_consumers"] == 2  # 1 standalone + 1 group
    assert snap["consumers"]["active_consumer_groups"] == 1
    # Lag = latest_offset (5) - committed_offset (2) = 3
    assert snap["consumers"]["lag_per_consumer_group"] == {"group-A": 3}

    assert snap["storage"]["log_size_bytes"] > 0
    assert snap["storage"]["disk_usage_bytes"] > 0
    assert "test-topic-0.jsonl" in snap["storage"]["partition_sizes"]


@pytest.mark.unit
def test_metrics_pretty_print():
    metrics = MetricsManager()
    metrics.record_produce(100, 5.0)
    metrics.record_consume(200, 2.0)
    report = metrics.pretty_print()
    assert "COWPYBARAMQ RUNTIME METRICS" in report
    assert "Messages Produced:   1" in report
    assert "Messages Consumed:   1" in report
