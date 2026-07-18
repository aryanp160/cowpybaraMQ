import shutil
import subprocess
import sys
import time
from pathlib import Path


def main():
    log_dir = Path("./logs")
    if log_dir.exists():
        shutil.rmtree(log_dir)

    print("Starting broker...")
    # Start broker in background
    broker_proc = subprocess.Popen(
        [sys.executable, "-m", "cmd.broker"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    # Wait for broker to start
    time.sleep(2)

    print("Starting consumer...")
    # Start consumer in background
    consumer_proc = subprocess.Popen(
        [sys.executable, "-m", "cmd.consumer", "--topic", "test_cli", "--offset", "0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    time.sleep(1)

    print("Producing message...")
    # Run producer synchronously
    producer_proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "cmd.producer",
            "--topic",
            "test_cli",
            "--message",
            "hello from cli",
        ],
        capture_output=True,
        text=True,
    )

    print(f"\n--- Producer Output ---\n{producer_proc.stdout.strip()}")
    if producer_proc.stderr:
        print(f"--- Producer Errors ---\n{producer_proc.stderr.strip()}")

    # Wait for consumer to receive message
    time.sleep(1)

    print("\nShutting down processes...")
    consumer_proc.terminate()
    broker_proc.terminate()

    try:
        consumer_out, _ = consumer_proc.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        consumer_proc.kill()
        consumer_out, _ = consumer_proc.communicate()

    try:
        broker_out, _ = broker_proc.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        broker_proc.kill()
        broker_out, _ = broker_proc.communicate()

    print(f"\n--- Consumer Output ---\n{consumer_out.strip()}")
    print(f"\n--- Broker Output ---\n{broker_out.strip()}")

    # Cleanup
    if log_dir.exists():
        shutil.rmtree(log_dir)


if __name__ == "__main__":
    main()
