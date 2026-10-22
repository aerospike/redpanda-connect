# Aerospike output benchmark results

Append each run as its own dated section. Use the same headings for the local Docker bench and for the GCP perf lab, so the two can be compared field by field.

Suite: [`internal/impl/aerospike/bench/`](../../internal/impl/aerospike/bench/).

The real-endpoint comparison against the Aerospike Kafka sink is in the `aerospike-connect` repository, `perftests` profile `redpanda-inbound`. That work is on a branch until its pull request merges. The Redpanda AWS soak framework is not used for this connector yet.

## How to record a run

Copy this block, fill it, and append it below. Leave a field as `not collected` rather than guessing.

```markdown
## YYYY-MM-DD — <local Docker | GCP perf lab> — <short name>

- **Branch / PR:**
- **Command:**

### Environment

- **Host:** CPU, RAM, OS
- **Aerospike:** edition, version, nodes, namespace size, storage engine, CPU and memory limits
- **Connect:** version or commit, GOMAXPROCS, CPU and memory limits
- **Broker:** none, or broker count, topic, partitions

### Dataset

- **Records:** count
- **Record shape:** key, bins, bytes per record
- **Total size:** records × record bytes
- **Operation / TTL:**

### Configuration

- **Batching:** count and period
- **Other:** coalesce, max_in_flight, retries, anything changed from the default pipeline

### Throughput

| | msg/sec | MB/sec |
|---|---|---|
| First rolling sample | | |
| Steady rolling samples (skip the first) | | |
| Total | | |

Runtime:

```text
paste the rolling stats and the total stats lines here
```

### Profiling

CPU, memory, Grafana, or `not collected`.

### Observations

What limited the rate, errors, and whether this number is a smoke test or a result to quote.
```

## Local Docker — shared setup

These fields are the same for every local section below.

- **Branch:** `CONNECTOR-1748`. No pull request yet.
- **Host:** MacBook Pro, Apple M1 Pro, 10 cores (8 performance, 2 efficiency), 32 GB RAM, macOS.
- **Aerospike:** `aerospike/aerospike-server-enterprise:8.1`, one node, cluster name `redpanda-demo`, namespace `test`, replication factor 1, memory engine, `data-size 4G`, `nsup-period 10`. No CPU set and no memory limit in Compose. Host port `127.0.0.1:3310`. Runs before 2026-10-22 18:30 used `data-size 1G`.
- **Connect:** `go run .` from `internal/impl/aerospike/bench`. `generate` input, no broker. `GOMAXPROCS` comes from `CORES`.
- **Record shape:** set `players`, integer key, one bin `name` of 512 bytes. `key`, `namespace`, and `ttl` are not stored. Operation `replace`. TTL 15,000 seconds. The benchmark processor counts about 566 bytes per message, which includes the fields that are not stored.
- **Batching:** count 10, period 200ms, unless a run says otherwise. This matches the perf-lab pipeline.
- **Namespace budget:** 4 GiB. A 2,000,000-record run of the 512-byte `name` bin is about 1.0 GiB of bin data plus record overhead, so it fits. The earlier 1 GiB setting filled up and rejected writes.

## 2026-10-22 — local Docker — 100,000 records, smoke test

- **Command:** `task up`, then `task run COUNT=100000 BATCH=10 CORES=1`

### Environment

Same as the local shared setup. `CORES=1`.

### Dataset

100,000 records × 512-byte `name` bin. About 51 MB stored, plus per-record overhead. Message bytes counted by the processor are about 54 MB.

### Configuration

Batch 10 or 200ms. `GOMAXPROCS=1`. No broker.

### Throughput

| | msg/sec | MB/sec |
|---|---|---|
| Rolling samples | 25,667 – 27,851 | 14 – 16 |
| Total | 26,264 | 15 |

Runtime: about 4 seconds, from 17:01:32 to 17:01:35.

```text
rolling stats: 25667 msg/sec, 14 MB/sec
rolling stats: 26482 msg/sec, 15 MB/sec
rolling stats: 27851 msg/sec, 16 MB/sec
total stats: 26264.007626 msg/sec, 15 MB/sec
```

### Profiling

Not collected.

### Observations

The pipeline connected to 1 node and exited after the input finished. Four seconds is too short to separate warmup from a steady rate. Do not quote 26,264 msg/sec as the connector's throughput. The 2,000,000-record run is the next local section.

## 2026-10-22 — local Docker — 2,000,000 records

- **Command:** `task run COUNT=2000000 BATCH=10 CORES=1`, after `task up` on `data-size 4G`

### Environment

Same as the local shared setup. `CORES=1`. The server was recreated empty before this run.

### Dataset

2,000,000 records × 512-byte `name` bin. About 1.0 GiB stored, plus per-record overhead. Message bytes counted by the processor are about 1.08 GiB (about 566 bytes per message).

### Configuration

Batch 10 or 200ms. `GOMAXPROCS=1`. No broker.

### Throughput

| | msg/sec | MB/sec |
|---|---|---|
| First rolling sample | 26,471 | 15 |
| Steady rolling samples | 22,571 – 32,209, mostly 27,000 – 29,000 | 13 – 18 |
| Total | 28,210 | 16 |

Runtime: 71 seconds, from 18:28:00 to 18:29:11. The total line times 71 seconds is about 2,000,000 messages, so the input finished.

```text
rolling stats: 26471 msg/sec, 15 MB/sec
rolling stats: 31191 msg/sec, 18 MB/sec
rolling stats: 32209 msg/sec, 18 MB/sec
rolling stats: 22571 msg/sec, 13 MB/sec
rolling stats: 24743 msg/sec, 14 MB/sec
rolling stats: 25050 msg/sec, 14 MB/sec
total stats: 28209.842252 msg/sec, 16 MB/sec
```

The full per-second log is in the run that produced this section. The lines above are the first sample, the high sample, the three dips, and the total.

### Profiling

Not collected.

### Observations

No write errors. The pipeline connected to 1 node and exited on its own. The rate stayed near 28,000 msg/sec for the whole minute, with a few dips to about 23,000–25,000. This is the local number to quote for this laptop, this batch size, and one core.

The attempt just before this one, on `data-size 1G`, filled the namespace, rejected batches, and was stopped with Ctrl-C. Its `total stats` line of 23,756 msg/sec includes that failure and is not a result.

## GCP perf lab

Pending. Use the same headings. Fill them from the `aerospike-connect` repository, `perftests` profile `redpanda-inbound`, and from the matching `kafka-inbound` run.

Add these fields on top of the template:

- **Layout:** loader, destination Aerospike, Connect, and broker counts
- **Loader profile:** `512`, `500kb`, or `1mb-name`, and the duration (`-d`)
- **Connect pipeline:** the rendered `pipeline.yaml` (fetch sizes, batch 10 / 200ms, consumer group)
- **Comparison:** Redpanda Connect msg/sec and MB/sec next to the Kafka sink on the same profile and dataset
