# CowpybaraMQ
> **Current Status:** V1 in active development.

A lightweight log-based message broker built in Python.

CowpybaraMQ is an educational project that explores the fundamentals of distributed messaging systems by implementing publish/subscribe messaging, TCP networking, and append-only log storage from scratch.

## Present Features (V1 In Progress)

- **Append-Only Log Storage**: Fully implemented in `internal/storage.py`. Messages are persisted sequentially to JSONL files on disk. Topics are automatically created, and integer offsets are safely assigned and preserved across restarts.
- **Asynchronous Networking Layer**: Implemented in `internal/networking.py`. A non-blocking `asyncio` TCP server that efficiently manages concurrent producer and consumer connections.
- **Newline-Delimited JSON Protocol**: Implemented in `internal/protocol.py`. A simple, easy-to-parse communication protocol that decodes `produce` and `consume` commands and safely handles invalid data.

> **Next Up:** The central broker logic (wiring the networking layer directly to the storage layer) is currently pending implementation.

### Architecture Diagram

```text
+----------------+       +-------------------+       +----------------+
|                |       |                   |       |                |
|  Producer(s)   +------>+   CowpybaraMQ     +------>+  Consumer(s)   |
|                |       |   (TCP Broker)    |       |                |
+----------------+       +---------+---------+       +----------------+
                                   |
                                   v
                         +-------------------+
                         |   Storage Layer   |
                         |  (Append-Only Log)|
                         +-------------------+
```

## Project Structure

```text
cowpybaraMQ/
├── cmd/               # Entrypoints for the broker and CLI tools
├── internal/          # Core internal modules (config, messages, utils)
├── docs/              # Additional documentation
├── logs/              # Log files storage directory
├── tests/             # Unit and integration tests
├── Dockerfile         # Docker configuration for containerization
├── docker-compose.yml # Compose file to run the broker in Docker
└── requirements.txt   # Python dependencies
```

## Getting Started

1. **Clone the repository:**
   ```bash
   git clone https://github.com/aryanp160/cowpybaraMQ.git
   cd cowpybaraMQ
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start the broker:**
   You can run the broker using Docker:
   ```bash
   docker-compose up --build
   ```
   Or run it locally natively:
   ```bash
   export COWPYBARA_LOG_DIR=./logs
   # python -m cmd.server.main
   ```

## Protocol

CowpybaraMQ uses a JSON-based protocol over TCP. Each request is a JSON object containing the action to perform.

### Producing a Message
Send a JSON object with the `produce` action, the topic, and the message payload:
```json
{
  "action": "produce",
  "topic": "users",
  "payload": {"id": 1, "action": "login"}
}
```

### Consuming a Message
Send a JSON object with the `consume` action, specifying the topic and offset:
```json
{
  "action": "consume",
  "topic": "users",
  "offset": 0
}
```

## Example

Here's how a typical interaction flows:

1. **Producer connects** on `127.0.0.1:9092` and sends:
   ```json
   {
     "action": "produce",
     "topic": "notifications",
     "payload": {"msg": "Hello World"}
   }
   ```
   Broker responds:
   ```json
   {"status": "ok"}
   ```

2. **Consumer connects** on `127.0.0.1:9092` and sends:
   ```json
   {
     "action": "consume",
     "topic": "notifications",
     "offset": 0
   }
   ```
   Broker responds with the message data:
   ```json
   {
     "topic": "notifications",
     "payload": {"msg": "Hello World"},
     "timestamp": "2026-06-29T14:45:00"
   }
   ```

## License

MIT