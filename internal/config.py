import logging
import os

# Network configuration
HOST = os.getenv("COWPYBARA_HOST", "0.0.0.0")
PORT = int(os.getenv("COWPYBARA_PORT", 9092))

# Storage configuration
LOG_DIR = os.getenv("COWPYBARA_LOG_DIR", "./logs")
NUM_PARTITIONS = int(os.getenv("COWPYBARA_NUM_PARTITIONS", 3))


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
