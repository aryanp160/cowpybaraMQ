import argparse
import asyncio
import json
import os
from typing import Any, Dict

from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

DEFAULT_MEMBERS = ["127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"]


async def query_broker_status(address: str) -> Dict[str, Any]:
    host, port = address.split(":")
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, int(port)), timeout=0.4
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


def format_uptime(seconds: float) -> str:
    if seconds <= 0:
        return "N/A"
    hrs = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hrs > 0:
        return f"{hrs}h {mins}m {secs}s"
    if mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def format_bytes(num_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} TB"


def make_header_panel(brokers_info: Dict[str, Any], leader_addr: str) -> Panel:
    alive_count = sum(1 for info in brokers_info.values() if info["status"] == "ALIVE")
    total_count = len(brokers_info)
    health_pct = int((alive_count / total_count) * 100) if total_count > 0 else 0

    health_color = (
        "green" if health_pct == 100 else "yellow" if health_pct > 0 else "red"
    )

    # Find max uptime from alive brokers
    max_uptime = 0.0
    for info in brokers_info.values():
        if info["status"] == "ALIVE":
            metrics = info["stats"].get("metrics") or {}
            max_uptime = max(
                max_uptime, metrics.get("broker", {}).get("uptime_seconds", 0.0)
            )

    title_text = Text("COWPYBARAMQ TERMINAL DASHBOARD", style="bold cyan")
    info_text = Text.assemble(
        ("\nLeader Address: ", "bold"),
        (
            leader_addr or "None (Election Pending)",
            "bold yellow" if leader_addr else "bold red",
        ),
        ("  |  Cluster Health: ", "bold"),
        (f"{health_pct}%", f"bold {health_color}"),
        ("  |  Active Nodes: ", "bold"),
        (f"{alive_count}/{total_count}", "cyan"),
        ("  |  Uptime: ", "bold"),
        (format_uptime(max_uptime), "magenta"),
    )

    return Panel(
        Text.assemble(title_text, info_text),
        title="[bold green]Status Overview[/bold green]",
        border_style="green",
    )


def make_nodes_table(brokers_info: Dict[str, Any]) -> Table:
    table = Table(expand=True)
    table.add_column("Broker ID / Port", style="bold cyan")
    table.add_column("Status", justify="center")
    table.add_column("Role", justify="center")
    table.add_column("Partitions Owned", justify="center")
    table.add_column("Followers Syncing", justify="center")

    for key, info in sorted(brokers_info.items(), key=lambda x: str(x[0])):
        status = info["status"]
        role = info["role"]
        stats = info["stats"]

        if status == "ALIVE":
            status_text = Text("ALIVE", style="bold green")
            role_text = Text(
                role.upper(), style="bold yellow" if role == "leader" else "cyan"
            )
            owned = len(stats.get("offsets", {}))
            followers = ", ".join(stats.get("followers", [])) or "None"
        elif status == "DEAD":
            status_text = Text("DEAD (KILLED)", style="bold red")
            role_text = Text("OFFLINE", style="dim")
            owned = "-"
            followers = "-"
        else:
            status_text = Text("UNREACHABLE", style="bold magenta")
            role_text = Text("UNKNOWN", style="dim")
            owned = "-"
            followers = "-"

        table.add_row(
            str(key),
            status_text,
            role_text,
            str(owned),
            followers,
        )
    return table


def make_topics_panel(brokers_info: Dict[str, Any]) -> Panel:
    # Gather distinct topics
    topics_data = {}
    for info in brokers_info.values():
        if info["status"] == "ALIVE":
            for topic, parts in info["stats"].get("topics", {}).items():
                topics_data[topic] = {
                    "partitions": parts,
                    "msg_count": 0,
                }
            # Add message counts
            for p_key, offset in info["stats"].get("offsets", {}).items():
                t_name = p_key.rsplit("-", 1)[0]
                if t_name in topics_data:
                    topics_data[t_name]["msg_count"] += offset

    table = Table(expand=True, box=None)
    table.add_column("Topic Name", style="bold yellow")
    table.add_column("Partitions", justify="right")
    table.add_column("Total Messages", justify="right")

    if not topics_data:
        table.add_row("No topics registered", "-", "-")
    else:
        for t_name, data in sorted(topics_data.items()):
            table.add_row(
                t_name,
                str(data["partitions"]),
                f"{data['msg_count']:,}",
            )

    return Panel(
        table,
        title="[bold green]Topics Overview[/bold green]",
        border_style="green",
    )


def make_consumers_panel(brokers_info: Dict[str, Any]) -> Panel:
    groups_data = {}
    for info in brokers_info.values():
        if info["status"] == "ALIVE":
            metrics = info["stats"].get("metrics") or {}
            lags = metrics.get("consumers", {}).get("lag_per_consumer_group") or {}
            for g_id, lag in lags.items():
                if g_id not in groups_data:
                    groups_data[g_id] = {"consumers": 0, "lag": 0}
                groups_data[g_id]["lag"] = max(groups_data[g_id]["lag"], lag)

            # Count members from partition ownership
            ownership = info["stats"].get("partition_ownership") or {}
            for g_id, topics_map in ownership.items():
                if g_id not in groups_data:
                    groups_data[g_id] = {"consumers": 0, "lag": 0}
                members = set()
                for topic, c_map in topics_map.items():
                    members.update(c_map.keys())
                groups_data[g_id]["consumers"] = max(
                    groups_data[g_id]["consumers"], len(members)
                )

    table = Table(expand=True, box=None)
    table.add_column("Consumer Group", style="bold cyan")
    table.add_column("Active Members", justify="right")
    table.add_column("Total Lag (msgs)", justify="right")

    if not groups_data:
        table.add_row("No active groups", "-", "-")
    else:
        for g_id, data in sorted(groups_data.items()):
            lag_color = (
                "red" if data["lag"] > 10 else "yellow" if data["lag"] > 0 else "green"
            )
            table.add_row(
                g_id,
                str(data["consumers"]),
                Text(f"{data['lag']:,}", style=f"bold {lag_color}"),
            )

    return Panel(
        table,
        title="[bold green]Consumers Overview[/bold green]",
        border_style="green",
    )


def make_metrics_panel(brokers_info: Dict[str, Any]) -> Panel:
    # Aggregate leader's metrics
    messages_produced = 0
    messages_consumed = 0
    bytes_written = 0
    bytes_read = 0
    tp_msgs = 0.0
    tp_bytes = 0.0
    avg_prod_lat = 0.0
    avg_cons_lat = 0.0
    comp_ratio = 1.0

    for info in brokers_info.values():
        if info["status"] == "ALIVE" and info["role"] == "leader":
            metrics = info["stats"].get("metrics") or {}
            broker_metrics = metrics.get("broker") or {}
            perf_metrics = metrics.get("performance") or {}

            messages_produced = broker_metrics.get("messages_produced", 0)
            messages_consumed = broker_metrics.get("messages_consumed", 0)
            bytes_written = broker_metrics.get("bytes_written", 0)
            bytes_read = broker_metrics.get("bytes_read", 0)

            tp_msgs = perf_metrics.get("throughput_messages_sec", 0.0)
            tp_bytes = perf_metrics.get("throughput_bytes_sec", 0.0)
            avg_prod_lat = perf_metrics.get("average_produce_latency_ms", 0.0)
            avg_cons_lat = perf_metrics.get("average_consume_latency_ms", 0.0)
            comp_ratio = perf_metrics.get("compression_ratio", 1.0)
            break

    # Calculate replication lag
    rep_lag = 0
    for info in brokers_info.values():
        if info["status"] == "ALIVE" and info["role"] == "leader":
            leader_offsets = info["stats"].get("offsets", {})
            follower_offsets = info["stats"].get("follower_offsets", {})
            for fol_id, tps in follower_offsets.items():
                for tp, f_off in tps.items():
                    l_off = leader_offsets.get(tp, 0)
                    rep_lag += max(0, l_off - f_off)

    grid = Table.grid(expand=True)
    grid.add_column(ratio=2)
    grid.add_column(ratio=3)

    grid.add_row("Throughput (msgs)", f"[bold cyan]{tp_msgs:.1f}[/bold cyan] msgs/sec")
    grid.add_row(
        "Throughput (bytes)",
        f"[bold cyan]{format_bytes(int(tp_bytes))}[/bold cyan]/sec",
    )
    grid.add_row("Produce Latency", f"[bold yellow]{avg_prod_lat:.2f} ms[/bold yellow]")
    grid.add_row("Consume Latency", f"[bold yellow]{avg_cons_lat:.2f} ms[/bold yellow]")
    grid.add_row("Compression Ratio", f"[bold green]{comp_ratio:.2fx}[/bold green]")
    grid.add_row("Replication Lag", f"[bold red]{rep_lag:,}[/bold red] messages")
    grid.add_row("Total Produced", f"[cyan]{messages_produced:,}[/cyan]")
    grid.add_row("Total Consumed", f"[cyan]{messages_consumed:,}[/cyan]")
    grid.add_row("Bytes Written", f"[cyan]{format_bytes(bytes_written)}[/cyan]")
    grid.add_row("Bytes Read", f"[cyan]{format_bytes(bytes_read)}[/cyan]")

    return Panel(
        grid,
        title="[bold green]Performance & Metrics[/bold green]",
        border_style="green",
    )


def make_storage_panel(brokers_info: Dict[str, Any]) -> Panel:
    total_log_size = 0
    total_disk_usage = 0
    partitions_map = {}

    for info in brokers_info.values():
        if info["status"] == "ALIVE":
            metrics = info["stats"].get("metrics") or {}
            storage = metrics.get("storage") or {}
            total_log_size += storage.get("log_size_bytes", 0)
            total_disk_usage += storage.get("disk_usage_bytes", 0)
            for part, sz in storage.get("partition_sizes", {}).items():
                partitions_map[part] = partitions_map.get(part, 0) + sz

    grid = Table.grid(expand=True)
    grid.add_column(ratio=2)
    grid.add_column(ratio=3)

    grid.add_row(
        "Total Log Size", f"[bold yellow]{format_bytes(total_log_size)}[/bold yellow]"
    )
    grid.add_row(
        "Total Disk Usage",
        f"[bold yellow]{format_bytes(total_disk_usage)}[/bold yellow]",
    )

    # Partition progress bars
    for part, sz in sorted(partitions_map.items()):
        # Draw a little visualization
        bar_len = min(10, int(sz / max(1, total_log_size) * 10))
        bar = "█" * bar_len + "░" * (10 - bar_len)
        grid.add_row(f"  {part}", f"{bar}  ({format_bytes(sz)})")

    return Panel(
        grid,
        title="[bold green]Storage & Log Allocation[/bold green]",
        border_style="green",
    )


def make_failures_panel(brokers_info: Dict[str, Any]) -> Panel:
    elections_count = 0
    leader_changes = 0
    dead_brokers = []
    unreachable_brokers = []

    for key, info in brokers_info.items():
        if info["status"] == "DEAD":
            dead_brokers.append(str(key))
        elif info["status"] == "UNREACHABLE":
            unreachable_brokers.append(str(key))

        if info["status"] == "ALIVE":
            metrics = info["stats"].get("metrics") or {}
            broker_metrics = metrics.get("broker") or {}
            leader_changes = max(
                leader_changes, broker_metrics.get("leader_changes", 0)
            )
            # Estimated elections = leader_changes
            elections_count = max(elections_count, leader_changes)

    grid = Table.grid(expand=True)
    grid.add_column(ratio=2)
    grid.add_column(ratio=3)

    grid.add_row(
        "Dead Nodes",
        f"[bold red]{', '.join(dead_brokers) or 'None'}[/bold red]",
    )
    grid.add_row(
        "Unreachable Nodes",
        f"[bold magenta]{', '.join(unreachable_brokers) or 'None'}[/bold magenta]",
    )
    grid.add_row("Elections Triggered", f"[bold yellow]{elections_count}[/bold yellow]")
    grid.add_row("Leader Changes", f"[bold yellow]{leader_changes}[/bold yellow]")

    return Panel(
        grid,
        title="[bold red]Fault & Failure Log[/bold red]",
        border_style="red",
    )


async def main():
    parser = argparse.ArgumentParser(description="CowpybaraMQ Terminal Dashboard")
    parser.add_argument(
        "--members",
        default=os.getenv("COWPYBARA_CLUSTER_MEMBERS", ",".join(DEFAULT_MEMBERS)),
        help="Comma-separated list of cluster members",
    )
    args = parser.parse_args()
    members = [m.strip() for m in args.members.split(",") if m.strip()]

    # Layout setup
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=4),
        Layout(name="body", ratio=1),
    )

    layout["body"].split_row(
        Layout(name="left", ratio=1),
        Layout(name="right", ratio=1),
    )

    layout["left"].split_column(
        Layout(name="nodes", ratio=3),
        Layout(name="failures", ratio=2),
    )

    layout["right"].split_column(
        Layout(name="topics", ratio=2),
        Layout(name="consumers", ratio=2),
        Layout(name="metrics", ratio=3),
        Layout(name="storage", ratio=2),
    )

    with Live(layout, refresh_per_second=1.0, screen=True):
        while True:
            # Query all brokers
            leader_addr = None
            brokers_info = {}

            for addr in members:
                stats = await query_broker_status(addr)
                if stats:
                    b_id = stats.get("broker_id")
                    brokers_info[b_id] = {
                        "address": addr,
                        "stats": stats,
                        "role": stats.get("role"),
                        "status": "DEAD" if stats.get("killed") else "ALIVE",
                    }
                    if stats.get("role") == "leader" and not stats.get("killed"):
                        leader_addr = addr
                else:
                    brokers_info[addr] = {
                        "address": addr,
                        "status": "UNREACHABLE",
                        "role": "unknown",
                        "stats": {},
                    }

            # Update layout sections
            layout["header"].update(make_header_panel(brokers_info, leader_addr))
            layout["nodes"].update(
                Panel(
                    make_nodes_table(brokers_info),
                    title="[bold green]Cluster Topology[/bold green]",
                    border_style="green",
                )
            )
            layout["failures"].update(make_failures_panel(brokers_info))
            layout["topics"].update(make_topics_panel(brokers_info))
            layout["consumers"].update(make_consumers_panel(brokers_info))
            layout["metrics"].update(make_metrics_panel(brokers_info))
            layout["storage"].update(make_storage_panel(brokers_info))

            await asyncio.sleep(1.0)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
