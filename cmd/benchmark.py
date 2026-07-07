import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def run_produce_benchmark(port, topic, count, acks):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    latencies = []

    start_time = time.time()
    for i in range(count):
        req = {
            "action": "produce",
            "topic": topic,
            "payload": {"val": i, "padding": "x" * 100},
            "acks": acks,
        }
        req_start = time.time()
        writer.write((json.dumps(req) + "\n").encode())
        await writer.drain()

        if acks != "0":
            await reader.readline()

        latencies.append((time.time() - req_start) * 1000)

    total_time = time.time() - start_time
    writer.close()
    await writer.wait_closed()

    throughput = count / total_time if total_time > 0 else 0
    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.5)] if latencies else 0
    p90 = latencies[int(len(latencies) * 0.9)] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0

    return throughput, p50, p90, p99


async def run_consume_benchmark(port, topic, count):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    req = {"action": "consume", "topic": topic, "offset": 0}
    writer.write((json.dumps(req) + "\n").encode())
    await writer.drain()

    start_time = time.time()
    received = 0
    while received < count:
        line = await reader.readline()
        if not line:
            break
        received += 1

    total_time = time.time() - start_time
    writer.close()
    await writer.wait_closed()

    throughput = received / total_time if total_time > 0 else 0
    return throughput


async def main():
    parser = argparse.ArgumentParser(description="CowpybaraMQ Benchmarking Utility")
    parser.add_argument(
        "--count", type=int, default=1000, help="Number of messages to produce/consume"
    )
    args = parser.parse_args()

    port = get_free_port()
    log_dir = Path(f"./benchmark-logs-{port}")
    if log_dir.exists():
        import shutil

        shutil.rmtree(log_dir)

    print("=" * 60)
    print(f"Starting Cowpybara broker benchmark on port {port}...")
    print("=" * 60)

    # Start broker process
    cmd = [
        sys.executable,
        "cmd/broker.py",
        "--port",
        str(port),
        "--role",
        "leader",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.abspath(os.getcwd())
    proc = subprocess.Popen(
        cmd,
        env=env,
    )

    # Wait for broker to start up
    await asyncio.sleep(1.0)

    try:
        topic = "benchmark-topic"

        # 1. Benchmark ACK=0 (Fire-and-Forget)
        print("Benchmarking PRODUCE (acks=0)...")
        tp_ack0, p50_0, p90_0, p99_0 = await run_produce_benchmark(
            port, topic, args.count, "0"
        )
        print(f"  Throughput: {tp_ack0:.2f} msgs/sec")
        print(f"  Latency: p50={p50_0:.2f}ms | p90={p90_0:.2f}ms | p99={p99_0:.2f}ms")
        print("-" * 60)

        # 2. Benchmark ACK=1 (Leader confirmed)
        print("Benchmarking PRODUCE (acks=1)...")
        tp_ack1, p50_1, p90_1, p99_1 = await run_produce_benchmark(
            port, topic, args.count, "1"
        )
        print(f"  Throughput: {tp_ack1:.2f} msgs/sec")
        print(f"  Latency: p50={p50_1:.2f}ms | p90={p90_1:.2f}ms | p99={p99_1:.2f}ms")
        print("-" * 60)

        # 3. Benchmark CONSUME
        print("Benchmarking CONSUME...")
        tp_cons = await run_consume_benchmark(port, topic, args.count * 2)
        print(f"  Throughput: {tp_cons:.2f} msgs/sec")
        print("-" * 60)

        # Print Benchmark Report Table
        print("\n" + "=" * 60)
        print("                     BENCHMARK REPORT")
        print("=" * 60)
        print(
            f"{'Operation':<20} | {'Throughput (msgs/s)':<20} | {'Latency p99 (ms)':<15}"
        )
        print("-" * 60)
        print(f"{'PRODUCE (acks=0)':<20} | {tp_ack0:<20.2f} | {p99_0:<15.2f}")
        print(f"{'PRODUCE (acks=1)':<20} | {tp_ack1:<20.2f} | {p99_1:<15.2f}")
        print(f"{'CONSUME':<20} | {tp_cons:<20.2f} | {'N/A':<15}")
        print("=" * 60)

    finally:
        proc.terminate()
        proc.wait()
        if log_dir.exists():
            import shutil

            shutil.rmtree(log_dir)


if __name__ == "__main__":
    asyncio.run(main())
