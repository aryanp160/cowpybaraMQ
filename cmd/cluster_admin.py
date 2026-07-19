import argparse
import asyncio
import json
from typing import Any, Dict, List

DEFAULT_MEMBERS = ["127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"]


async def query_broker_status(address: str) -> Dict[str, Any]:
    host, port = address.split(":")
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, int(port)), timeout=0.5
        )
        req = {"action": "cluster_status"}
        writer.write((json.dumps(req) + "\n").encode())
        await writer.drain()
        resp = await reader.readline()
        writer.close()
        await writer.wait_closed()
        if resp:
            data = json.loads(resp.decode().strip())
            if data.get("status") == "ok":
                return data.get("stats")
    except Exception:
        pass
    return None


async def send_failure_command(address: str, command_type: str) -> bool:
    host, port = address.split(":")
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, int(port)), timeout=0.5
        )
        req = {"action": "simulate_failure", "type": command_type}
        writer.write((json.dumps(req) + "\n").encode())
        await writer.drain()
        resp = await reader.readline()
        writer.close()
        await writer.wait_closed()
        if resp:
            data = json.loads(resp.decode().strip())
            return data.get("status") == "ok"
    except Exception:
        pass
    return False


async def show_cluster(members: List[str]):
    print("=" * 60)
    print("                 COWPYBARAMQ CLUSTER STATUS")
    print("=" * 60)

    leader_id = None
    leader_addr = None
    brokers_info = {}

    # Query all brokers
    for addr in members:
        stats = await query_broker_status(addr)
        if stats:
            b_id = stats.get("broker_id")
            brokers_info[b_id] = {
                "address": addr,
                "stats": stats,
                "role": stats.get("role"),
                "status": "ALIVE" if not stats.get("killed") else "DEAD",
            }
            if stats.get("role") == "leader" and not stats.get("killed"):
                leader_id = b_id
                leader_addr = addr
        else:
            # Not reachable
            brokers_info[addr] = {
                "address": addr,
                "status": "UNREACHABLE",
                "role": "unknown",
                "stats": {},
            }

    print(f"Current Leader: {leader_id} ({leader_addr or 'None'})")
    print("-" * 60)

    for b_key, info in brokers_info.items():
        addr = info["address"]
        status = info["status"]
        role = info["role"]
        stats = info["stats"]

        print(f"Broker: {b_key} @ {addr} | Status: {status} | Role: {role}")
        if status == "ALIVE":
            print(
                f"  Disconnected (network partition): {stats.get('disconnected', False)}"
            )
            print(f"  Followers connected: {stats.get('followers', [])}")
            print(f"  Follower Offsets: {stats.get('follower_offsets', {})}")

            # Topics/Partitions
            topics = stats.get("topics", {})
            print(f"  Topics: {list(topics.keys())}")
            for t_name, t_info in topics.items():
                print(
                    f"    - Topic '{t_name}': {t_info.get('partitions', 0)} partitions"
                )

            # Performance stats
            latencies = stats.get("latencies", [])
            avg_lat = sum(latencies) / len(latencies) if latencies else 0.0
            print(
                f"  Replication Latency: Avg {avg_lat:.2f}ms (from {len(latencies)} acks)"
            )
            print(f"  Messages Replicated count: {stats.get('total_messages', 0)}")
            print(f"  Connected Clients: {stats.get('connected_producers', 0)}")
            print(f"  Throughput: {stats.get('messages_sec', 0)} messages/sec")
        print("-" * 60)


async def main():
    parser = argparse.ArgumentParser(description="CowpybaraMQ Cluster Admin")
    parser.add_argument(
        "--members",
        default=",".join(DEFAULT_MEMBERS),
        help="Comma-separated list of cluster members",
    )
    subparsers = parser.add_subparsers(dest="command", help="Admin command")

    subparsers.add_parser("show_cluster", help="Show current cluster status")

    kill_parser = subparsers.add_parser(
        "kill_leader", help="Simulate leader broker failure"
    )
    kill_parser.add_argument(
        "--leader-port", type=int, default=9092, help="Port of the leader to kill"
    )

    disc_parser = subparsers.add_parser(
        "disconnect_follower", help="Simulate network partition on follower"
    )
    disc_parser.add_argument(
        "--port", type=int, required=True, help="Follower port to disconnect"
    )

    rec_parser = subparsers.add_parser(
        "recover_broker", help="Recover a failed/disconnected broker"
    )
    rec_parser.add_argument(
        "--port", type=int, required=True, help="Broker port to recover"
    )

    args = parser.parse_args()
    members = [m.strip() for m in args.members.split(",") if m.strip()]

    if args.command == "show_cluster" or not args.command:
        await show_cluster(members)

    elif args.command == "kill_leader":
        target = f"127.0.0.1:{args.leader_port}"
        print(f"Simulating LEADER FAILURE by killing broker at {target}...")
        success = await send_failure_command(target, "kill")
        print("Success!" if success else "Failed to send command.")

    elif args.command == "disconnect_follower":
        target = f"127.0.0.1:{args.port}"
        print(f"Simulating network partition / disconnection on broker {target}...")
        success = await send_failure_command(target, "disconnect")
        print("Success!" if success else "Failed to send command.")

    elif args.command == "recover_broker":
        target = f"127.0.0.1:{args.port}"
        print(f"Recovering / restarting broker {target}...")
        success = await send_failure_command(target, "recover")
        print("Success!" if success else "Failed to send command.")


if __name__ == "__main__":
    asyncio.run(main())
