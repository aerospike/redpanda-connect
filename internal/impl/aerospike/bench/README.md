# Aerospike output — local write benchmark

Measures `output.aerospike` writing to a single-node Enterprise server in Docker (`aerospike/aerospike-server-enterprise:8.1`, namespace `test`, replication factor 1). There is no Redpanda broker in this run. The `benchmark` processor sits in front of the output, so the logged rate is the rate the output accepts.

This server is separate from the integration cluster (`as-ee-1` on `127.0.0.1:3100` and `as-ee-2` on `127.0.0.1:3110`). Do not point the benchmark at that cluster.

This is the local half of connector certification. It is not a certification result. The real comparison, loader to broker to sink to a deployed Aerospike cluster, is in the `aerospike-connect` repository under `perftests`, profile `redpanda-inbound`, on the redpanda-inbound perf-test branch until its PR merges. It uses the same record shape and the same default batch (10 records or 200ms) as `perftests/k8s/stacks/redpanda-inbound/redpanda-connect/pipeline.yaml.tpl`. The Kafka sink comparison is that profile against `kafka-inbound`, not this directory.

The local runs and the GCP Redpanda runs are recorded in [`docs/benchmark-results/aerospike.md`](../../../../docs/benchmark-results/aerospike.md). That file is not a certification result. The Kafka sink comparison on the same profiles is not collected yet.

## Record

Matches the shared `512` loader profile:

| Field | Value |
|---|---|
| Namespace / set | `test` / `players` |
| Key | unique integer `key` |
| Bin | `name`, 512 bytes |
| Operation | `replace` |
| TTL | `15000` seconds, inside the loader's 10000–20000 range |
| Dropped from the record | `key`, `namespace`, `ttl` |

`500kb` and `1mb-name` stay on the perf-lab size comparison. This namespace is 4 GiB of memory, enough for the 512-byte profile at a few million records. Docker Desktop must allow the container at least 4 GiB plus Aerospike's own overhead.

## Prerequisites

- Docker, with a pull of `aerospike/aerospike-server-enterprise:8.1` allowed.
- Go, from a repository checkout where `task deps` has already run.
- `task` (go-task).

The server image ships its feature key at `/etc/aerospike/features.conf`. This bench uses that file. No host key is mounted, and `AEROSPIKE_FEATURE_KEY` is not required.

`task run` builds `main.go` in this directory through the community `service.RunCLI` entry point. That binary registers `generate`, the `benchmark` processor, Prometheus, and `output.aerospike`. It does not compile `cmd/redpanda-connect`, which pulls in every community connector. The first build takes a few minutes. Later runs reuse the cache.

## How to run

From the repository root, one command starts the server, writes the records, and stops the server even when the write fails:

```bash
cd internal/impl/aerospike/bench
task bench COUNT=100000 BATCH=10 CORES=1
```

`COUNT=100000` finishes in a few seconds. Treat that as a smoke test. A longer run, such as `COUNT=1000000`, is what to read for a steady rate.

To leave the server up and change one setting between runs:

```bash
task up
task run COUNT=1000000 BATCH=10 CORES=1
task down
```

`task up` publishes `127.0.0.1:3310`. `counter()` starts at 1 on every run, so a second run replaces the same keys. It does not add another 100,000 records. Run `task down` and `task up` when the measurement should start from an empty namespace. The `access-address` in `aerospike.conf` is `127.0.0.1` so Docker Desktop on macOS can reach the node. On Linux, the published port is the same.

| Variable | Default | What it sets |
|---|---|---|
| `COUNT` | `100000` | Messages the `generate` input produces |
| `BATCH` | `10` | `output.aerospike` `batching.count` |
| `PERIOD` | `200ms` | `output.aerospike` `batching.period` |
| `CORES` | `1` | `GOMAXPROCS` for the Connect process |

`BATCH` 10 and `PERIOD` 200ms match the perf-lab pipeline. Raise `BATCH` only when you want the output ceiling on this laptop, and label that run as such.

```bash
task run COUNT=100000 BATCH=256 PERIOD=1s CORES=4
```

Prometheus and pprof listen on `127.0.0.1:4195` while `task run` is in progress (`http.debug_endpoints` is on). With the profiling stack from `resources/docker/profiling`:

```bash
cd resources/docker/profiling
task profile:cpu
task profile:mem
```

## Expected output

```text
INFO rolling stats: <msg/sec>, <MB/sec>
INFO total stats: <msg/sec>, <MB/sec>
```

A run shorter than the 1s interval prints only `total stats`. On a longer run, read the steady `rolling stats` lines and ignore the first one, which includes client warmup. Unique keys avoid hot-key `KEY_BUSY`. A repeated key is a different test and is not what this config does.

A 100,000-record run is a smoke test. It is a laptop result, not a perf-lab result. The recorded smoke run is in [`docs/benchmark-results/aerospike.md`](../../../../docs/benchmark-results/aerospike.md).
