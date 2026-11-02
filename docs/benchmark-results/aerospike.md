# Aerospike output benchmark results

Append each run as its own dated section. Use the same headings for the local Docker bench and for the GCP perf lab, so the two can be compared field by field.

Suite: [`internal/impl/aerospike/bench/`](../../internal/impl/aerospike/bench/).

The real-endpoint runs are in the `aerospike-connect` repository, `perftests` profile `redpanda-inbound`, on the redpanda-inbound perf-test branch until its PR merges. The GCP sections below are those runs. The matching Kafka sink comparison is not collected. The Redpanda AWS soak framework is not used for this connector yet.

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

## GCP perf lab — shared setup

These fields are the same for every GCP section below. A section only repeats a field when that run changed it.

- **Branch / PR:** the redpanda-inbound perf-test branch until its PR merges. Connect image built from this repository at ref `dev`. Image commit not collected.
- **Command:** `run_load.sh` on each loader pod, `./aerospike-kafka-loader -f /etc/aerospike-kafka-loader/conf.yml -n 0 -d 300`. Omitting a positive `-n` leaves the message count at 0, so the five-minute clock is only the producer cap. The score line is Aerospike stop-writes, with successful writes at 0/s. Records still on the topic after that are not stored.
- **Host:** GCP zone `asia-south1-a`.
- **Layout:** full **3:5:5:5** unless a section says five loaders. That is 3 loader pods, 5 destination Aerospike nodes, 5 Connect pods, 5 Redpanda brokers.
- **Aerospike:** 8.1.2, five `c2d-standard-32` nodes, namespace `test`, memory engine, `data-size` 100 GiB per node, `stop-writes-used-pct` 90, replication factor 2, `max-record-size` 8 MiB. Unique capacity is 5 × 100 GiB × 90% / 2 = 225 GiB.
- **Connect:** five `c2d-standard-32` pods. Aerospike build of Redpanda Connect (`input.redpanda`, `output.aerospike`, Prometheus). Consumer group `aerospike-sink-perf`. `GOMAXPROCS` not collected.
- **Broker:** Redpanda 26.2.2, five `c2d-standard-32` VMs, four local SSDs in RAID0, Kafka API `:9092`. Topic `players`, 40 partitions, replication factor 3, `max.message.bytes` 8 MiB. Cluster `kafka_batch_max_bytes` is 8 MiB.
- **Loaders:** `c2d-standard-8`, 8 threads. `acks=-1`, compression off, `linger.ms` 5, `batch.size` 16 KiB, `max.request.size` 8 MiB. Each large record is its own produce request. Idempotent produce is off.
- **Record shape:** set `players`, integer key, one bin `name`. `key`, `namespace`, and `ttl` are not stored. Operation `replace`. Stored bytes per object is the `name` length plus 64: 576, 512,064, or 1,048,640.
- **Batching:** output count 10, period 200ms, on every run.
- **Comparison:** Kafka sink on the same profile and dataset: not collected.
- **Profiling:** Prometheus 1-minute rates. A Connect or Aerospike rate is the sum across the tier. Per-pod Connect rate is that sum divided by 5. Broker disk GB/s can count the RAID device and the NVMe members, so it is not used as a saturation result. CPU in the 20 October sections is the maximum during the measured window. Later sections say how their CPU sample was taken. The local `benchmark` processor rolling lines were not collected.

Default Connect input, used by the 20 October sections: 1 MiB partition fetch, 32 KiB yield.

Kept Connect input, used by the 21 October sections:

| Setting | Value |
|---|---|
| `fetch_max_partition_bytes` | 16 MiB |
| `max_yield_batch_bytes` | 16 MiB |
| `partition_buffer_bytes` | 32 MiB |
| `fetch_max_bytes` | 96 MiB |
| `fetch_min_bytes` | 1 MiB |
| `fetch_max_wait` | 100 ms |
| `commit_period` | 5 s |

`fetch_max_bytes` stays under the franz-go broker read cap of 100 MiB. 128 MiB is rejected when Connect starts. `max_yield_batch_bytes` stays at or under `partition_buffer_bytes`.

## 2026-10-20 — GCP perf lab — 512 B, 3 loaders, default fetch

- **Loader profile:** `512`. `name` is exactly 512 bytes. `-d 300` on 3 pods together.
- **Cluster:** `redpanda-inbound-size512-39376104265`.

### Dataset

Produced 625,834,758 records. Connect output 583,664,724. Aerospike writes 583,671,845. `master_objects` 391,809,710. Unique data 210 GiB of 225 GiB. Stored size 576 bytes.

### Throughput

Records/s is the window average. Produce also shows its peak. MB/s is the peak network rate from the scorecard, not the byte rate of that average.

| | records/s | Peak network MB/s |
|---|---|---|
| Produce, window average / peak | 1,800,000 / 2,100,000 | 1,250 loader transmit |
| Connect output, window average | 507,000 | 728 Connect receive |
| Aerospike writes, window average / peak | 507,000 / 589,000 | 829 Aerospike receive |

Produce window 09:45:30Z–09:51:15Z (5.8 min). Sink window 09:45:30Z–10:04:45Z (19.2 min). Stop-writes at 10:04:00Z on all 5 nodes.

### Profiling

CPU maximum during the measured window: loader 53%, broker 85%, Aerospike 59%, Connect 14%. Memory available minimum: loader 28 GiB, broker 34 GiB, Aerospike 18 GiB, Connect 123 GiB. Write p50 / p99 0.6 / 7.4 ms. Batch-write p50 / p99 1.5 / 15.1 ms.

### Observations

This is the small-record result to quote for the default pipeline. The sink uses real CPU and runs far above the 250-record batching ceiling. Write timeouts were 0. Write errors were 63,927, from records rejected after stop-writes. Connect `output_error` was 1,286.

`master_objects` is below the write count because `-n 0` gives each loader a key budget of 100,000,000. Later keys overlap and replace objects already stored.

## 2026-10-20 — GCP perf lab — 500 KiB, 3 loaders, default fetch

- **Loader profile:** `500kb`. `name` is exactly 512,000 bytes. `-d 300` on 3 pods together.
- **Cluster:** `redpanda-inbound-size500k-39407792509`.

### Dataset

Produced 1,519,816 records. Connect output 471,823. Aerospike writes 471,890. `master_objects` 471,890. Unique data 225 GiB. Stored size 512,064 bytes.

### Throughput

Records/s is the window average. Produce also shows its peak. MB/s is the peak network rate from the scorecard, not the byte rate of that average.

| | records/s | Peak network MB/s |
|---|---|---|
| Produce, window average / peak | 4,610 / 5,069 | 2,640 loader transmit |
| Connect output, plateau | 198 | 106 Connect receive |
| Aerospike writes, plateau | 198–201 | 206 Aerospike receive |

Produce window 11:05:42Z–11:11:12Z (5.5 min). Sink window 11:05:42Z–11:45:27Z (39.8 min). Stop-writes at 11:44:57Z on 3 of 5 nodes.

### Profiling

CPU maximum during the measured window: loader 50%, broker 68%, Aerospike 1%, Connect 1%. Memory available minimum: loader 28 GiB, broker 34 GiB, Aerospike 30 GiB, Connect 123 GiB. Write p50 / p99 3.7 / 15.7 ms. Batch-write p50 / p99 4.0 / 15.8 ms.

### Observations

Produce bytes match the payload: 5,069 × 512,000 is 2.60 GB/s against loader transmit 2.64 GB/s. Broker receive was 7.87 GB/s against about 7.79 GB/s for replication factor 3. The sink then sat near 198/s for 40 minutes with Connect and Aerospike near 1% CPU. Quote 198/s as the untuned 500 KiB sink, and 5,069/s as the produce peak. Write timeouts were 0. Write errors were 9,530. Connect `output_error` was 9,443. `master_objects` matches the write count, so each successful write stored a new object.

## 2026-10-20 — GCP perf lab — 1 MiB, 3 loaders, default fetch

- **Loader profile:** `1mb-name`. `name` is exactly 1,048,576 bytes. No nested `address`. `-d 300` on 3 pods together.
- **Cluster:** `redpanda-inbound-size1mb-39429214815`.

### Dataset

Produced 765,050 records. Connect output 208,665. Aerospike writes 208,742. `master_objects` 208,742. Unique data 204 GiB of 225 GiB. Stored size 1,048,640 bytes.

### Throughput

Records/s is the window average. Produce also shows its peak. MB/s is the peak network rate from the scorecard, not the byte rate of that average.

| | records/s | Peak network MB/s |
|---|---|---|
| Produce, window average / peak | 2,320 / 2,565 | 2,710 loader transmit |
| Connect output, plateau | 160, peak 170 | 180 Connect receive |
| Aerospike writes, plateau | 160, peak 170 | 356 Aerospike receive |

Produce window 14:00:01Z–14:05:31Z (5.5 min). Sink window 14:00:01Z–14:21:46Z (21.8 min). Stop-writes at 14:21:16Z on 2 of 5 nodes.

### Profiling

CPU maximum during the measured window: loader 63%, broker 72%, Aerospike 1%, Connect 1.6%. Memory available minimum: loader 28 GiB, broker 34 GiB, Aerospike 29 GiB, Connect 123 GiB. Write p50 / p99 5.6 / 15.8 ms. Batch-write p50 / p99 10.0 / 31.5 ms.

### Observations

Produce bytes match the payload: 2,565 × 1,048,576 is 2.69 GB/s against loader transmit 2.71 GB/s. Broker receive was 8.11 GB/s against about 8.07 GB/s for replication factor 3. Record rate is about half of the 500 KiB peak (5,069 / 2,565 = 1.98) against a size ratio of 2.05, so the two large sizes delivered the same produce bandwidth.

The sink stayed near 160/s. A 1 MiB record fills the default 1 MiB partition fetch, so each partition returns about one record per fetch. Connect and Aerospike stayed near 1% CPU. Quote 160/s as the untuned 1 MiB sink.

Stop-writes was the server default on available memory, at 81% data used and 4% available on the two nodes that set the flag. About 556,000 records were still on the brokers. Write timeouts were 0. Write errors were 3,731. Connect `output_error` was 3,711.

## 2026-10-21 — GCP perf lab — 1 MiB fetch and yield, 3 loaders

- **Loader profile:** `1mb-name`. `-d 300` on 3 pods. Output batching stayed at 10 / 200ms. Loader threads stayed at 8 except the last row.
- **Cluster:** `redpanda-inbound-size1mb-39545169965`.

### Configuration

Same records as the 20 October 1 MiB section. Only the Connect input changed.

| Input | Sink | Stop-writes | Kept |
|---|---|---|---|
| 5 MiB partition fetch only | ~153/s | not a full fill | no |
| 16 MiB fetch, 8 MiB yield, 32 MiB buffer, 96 MiB total fetch | p50 1,686/s, peak 2,077 | 2 min 29 sec, 2 of 5 | no |
| 32 MiB fetch, 8 MiB yield | ~2,060/s | 1 min 50 sec, all 5 | no |
| 16 MiB fetch, 16 MiB yield, 32 MiB buffer | ~2,480/s, matches produce | 1 min 36 sec, all 5 by 1 min 51 sec | yes |
| 32 MiB yield, fetch still 16 MiB | ~2,480/s | same clock | no |
| 32 MiB fetch, 32 MiB yield, 96 MiB buffer, 16 loader threads | ~2,570/s, matches produce | 1 min 33 sec, all 5 | no |

### Dataset

Kept run, started 10:15:54Z. `master_objects` 212,891. Unique data 208 GiB. Stored size 1,048,640 bytes.

### Throughput

Kept run only. The sink matched produce.

| | records/s | MB/s |
|---|---|---|
| Produce, plateau | 2,480 | not collected |
| Connect output, plateau | 2,480 | not collected |
| Aerospike writes, plateau | 2,480 | not collected |

### Profiling

Kept run: loader 60%, broker 61%, Connect 16%. Aerospike CPU not collected. Write p50 / p99 3.0 / 7.9 ms. Batch-write p50 / p99 6.7 / 22.8 ms. The 16-thread row reached loader CPU 67% and broker CPU 71% at about 2,570/s.

### Observations

Quote ~2,480/s as the 3-loader 1 MiB result on the kept input. The 160/s untuned sink was the 1 MiB partition fetch. A yield larger than the partition fetch only combines later fetches. Sixteen threads and a 32 MiB fetch did not raise produce above about 2,570/s. Those values were put back. Write errors and Connect `output_error` for this run: not collected.

## 2026-10-21 — GCP perf lab — five loaders, kept input, 1 MiB then 500 KiB

- **Layout:** **5:5:5:5**. Five loader pods, five Aerospike nodes, five Connect pods, five brokers. Threads stayed at 8. Pipeline stayed at the kept 16 MiB fetch, 16 MiB yield, and 32 MiB buffer.
- **Cluster:** `redpanda-inbound-size1mb-39598518063`. The 500 KiB run is the same cluster after the loader config was switched to `name` length 512,000. The topic was recreated empty before that run. The namespace was still truncating at 14:06:30Z, so the 500 KiB fill clock starts there.

### Dataset

| | 1 MiB | 500 KiB |
|---|---|---|
| `master_objects` | 209,554 | 474,956 |
| Unique data | 205 GiB | 226 GiB |
| Stored bytes / object | 1,048,640 | 512,064 |
| Stop-writes | 13:33:00Z, 3 of 5, 1 min 47 sec | 14:08:15Z on 4 of 5, all 5 at 14:08:30Z (2 min 14 sec) |

### Throughput

| | 1 MiB records/s | 1 MiB MB/s | 500 KiB records/s | 500 KiB MB/s |
|---|---|---|---|---|
| Produce, plateau | 2,331 | 2,480 loader transmit | 4,740 | 2,460 loader transmit |
| Connect output, plateau | 2,325 | not collected | 4,740 | not collected |
| Aerospike writes, plateau | 2,326 | not collected | 4,740 | 4,880 Aerospike receive |
| Connect, per pod | 466 | — | 948 | — |

1 MiB loaders started 13:31:13Z–13:31:27Z. 500 KiB loaders started 14:06:16Z–14:06:23Z. Each of the five loaders sent about 0.49 GB/s on both sizes.

### Profiling

1 MiB plateau: loader 33%, broker 62% (two brokers at 77–80%, the others near 50–62%), Aerospike 7%, Connect 15%.

500 KiB plateau: loader 27%, broker 67% (two brokers near 77–78%), Aerospike 5%, Connect 14%. Write p50 / p99 1.9 / 4.0 ms. Batch-write p50 / p99 4.4 / 13.9 ms. 1 MiB latency for this five-loader run: not collected.

### Observations

Five loaders did not raise the byte rate above the 3-loader 1 MiB run. Both sizes sit near 2.5 GB/s. Record size 1,048,576 / 512,000 is 2.05. Produce 4,740 / 2,331 is 2.03. Writes 4,740 / 2,326 is 2.04. Connect stayed level with produce on both. Quote that pair as the large-record result on the kept pipeline.

Broker receive on the 500 KiB run was 7.33 GB/s, about three times the 2.46 GB/s loader send. Aerospike receive was 4.88 GB/s, about two copies of the stored record.

Object count is 474,956 / 209,554 = 2.27 because the 1 MiB run stopped on 3 of 5 nodes at 205 GiB and the 500 KiB run filled all 5 to 226 GiB, at 90% used and 5% available memory. Write timeouts were 0 on both. Write errors and Connect `output_error` for these two runs: not collected.

### Comparison

Kafka sink: not collected.
