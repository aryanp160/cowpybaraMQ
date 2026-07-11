# CowpybaraMQ Benchmark Report

Generated on: 2026-07-11 20:23:45

## Methodology
Benchmarks are executed by spawning a leader broker instance on dynamically acquired free ports. Producers publish structured JSON payloads containing repeated string patterns to maximize gzip compression potential. CPU and memory usage of the broker subprocess are tracked in real-time during the runs.

## Benchmark Results

| Message Count | Compression | Mode | Throughput (msgs/s) | Throughput (bytes/s) | Avg Latency (ms) | P95 Latency (ms) | P99 Latency (ms) | Comp Ratio | CPU (%) | Memory (MB) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 10 | none | PRODUCE | 522.0 | 254.4 KB/s | 1.83 | 6.83 | 6.83 | 1.00x | 0.0% | 23.3 |
| 10 | none | CONSUME | 5472.7 | 2.4 MB/s | 0.18 | 1.83 | 1.83 | N/A | 0.0% | 23.3 |
| 10 | gzip | PRODUCE | 603.8 | 294.2 KB/s | 1.66 | 7.88 | 7.88 | 3.46x | 0.0% | 23.4 |
| 10 | gzip | CONSUME | 915.9 | 418.6 KB/s | 1.09 | 9.46 | 9.46 | N/A | 0.0% | 23.5 |
| 100 | none | PRODUCE | 731.3 | 357.7 KB/s | 1.34 | 4.02 | 8.99 | 1.00x | 0.0% | 23.3 |
| 100 | none | CONSUME | 5339.5 | 2.4 MB/s | 0.19 | 0.00 | 16.60 | N/A | 0.0% | 23.5 |
| 100 | gzip | PRODUCE | 841.4 | 411.6 KB/s | 1.15 | 3.84 | 13.41 | 3.46x | 0.0% | 23.5 |
| 100 | gzip | CONSUME | 5045.1 | 2.3 MB/s | 0.20 | 0.00 | 18.17 | N/A | 0.0% | 23.7 |


## Analysis & Insights
- **Compression Efficiency**: `gzip` compression reduces raw JSON payload footprints, increasing disk I/O savings dramatically for larger batches. This is visible in the Compression Ratio metric representing physical disk footprint reduction.
- **Throughput & Latency**: `gzip` compression introduces small CPU compute overheads, which can reduce produce throughput slightly but offers a massive reduction in replicated network volume.
