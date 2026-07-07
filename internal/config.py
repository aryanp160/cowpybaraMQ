import logging
import os

# Network configuration
HOST = os.getenv("COWPYBARA_HOST", "0.0.0.0")
PORT = int(os.getenv("COWPYBARA_PORT", 9092))

# Storage configuration
LOG_DIR = os.getenv("COWPYBARA_LOG_DIR", "./logs")
NUM_PARTITIONS = int(os.getenv("COWPYBARA_NUM_PARTITIONS", 3))

# Replication configuration
BROKER_ROLE = os.getenv("COWPYBARA_ROLE", "leader")
LEADER_HOST = os.getenv("COWPYBARA_LEADER_HOST", "127.0.0.1")
LEADER_PORT = int(os.getenv("COWPYBARA_LEADER_PORT", 9092))
BROKER_ID = int(os.getenv("COWPYBARA_BROKER_ID", PORT))
CLUSTER_MEMBERS = os.getenv(
    "COWPYBARA_CLUSTER_MEMBERS", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"
)
HEARTBEAT_INTERVAL = float(os.getenv("COWPYBARA_HEARTBEAT_INTERVAL", 0.5))
HEARTBEAT_TIMEOUT = float(os.getenv("COWPYBARA_HEARTBEAT_TIMEOUT", 2.0))


# Logging configuration
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
        ],
    )


setup_logging()
