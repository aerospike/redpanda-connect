# Aerospike inbound for Redpanda Connect

[![Build Status][actions-badge]][actions-url]

This repository is Aerospike's fork of [Redpanda Connect][connect]. It adds an `aerospike` output that writes each message as one Aerospike record. Inputs, Bloblang, batching, and delivery are the rest of Connect.

The published `docker.redpanda.com/redpandadata/connect` image does not include this output. Build the binary from this tree.

Field reference is generated from the Go specs. `task docs` writes it to `docs/modules/components/pages/outputs/aerospike.adoc`, which is gitignored. Change the spec in Go to change the page.

## What a write does

Each message is addressed by `namespace`, `set`, and either `key` or `digest`.

- `key` is a user key. Aerospike hashes it. `key_type` must match other clients that read the same records.
- `digest` is the 20-byte record digest as standard Base64. The stored user key is null. Set one of `key` or `digest`, not both.
- `bins` is a Bloblang mapping. Bin names are 1–15 characters. Nested objects and arrays become maps and lists.
- `operation` is `write`, `replace`, `create_only`, `update_only`, or `delete`.
- An empty or `null` payload is a delete when `tombstone_as_delete` is on, which is the Kafka tombstone convention.
- `ttl` accepts a duration (`24h`), Java-style units (`24H`, `1D`, `5M`, `60S`), a bare number of seconds, or `never` / `keep` / `-1` / `-2` / `0`.
- `namespace` and `set` can be interpolated from the topic or from JSON. Namespaces must already exist on the server. Do not interpolate an unbounded set name.
- `ignore_error_codes` acknowledges selected permanent rejects (for example 13, 20, 21, 22) instead of retrying them. Leave it empty to keep the default retry behaviour. A namespace taken from the message should leave 20 unlisted and use `output.fallback`, so a typo is published rather than dropped.
- Access control supports `internal` (password), `external` (LDAP), and `pki` (client certificate, no password).
- TLS hosts use `host:tlsname:port`. `cert_refresh_interval` re-reads the configured certificate files for new handshakes. Connections already in the pool stay up.

Writes are whole-bin puts. This output does not append to a list, increment a counter, or patch a map in place. There is no Aerospike input and no lookup processor in this tree.

## Example

```yaml
input:
  redpanda:
    seed_brokers: [ "127.0.0.1:19092" ]
    topics: [ users ]
    consumer_group: aerospike-sink

output:
  aerospike:
    hosts: [ "127.0.0.1:3000" ]
    cluster_name: redpanda-demo
    namespace: test
    set: users
    key: '${! json("user_id").catch(meta("kafka_key")) }'
    bins: 'root = this.without("user_id", "ttl")'
    operation: replace
    ttl: '${! json("ttl").or("keep") }'
```

Runnable pipelines:

| File | What it shows |
|---|---|
| `config/examples/aerospike_inbound.yaml` | Per-topic replace, merge, and delete, plus TLS and PKI comments |
| `config/examples/aerospike_topic_routing.yaml` | Topic or JSON field to namespace and set, with fallback on code 20 |
| `config/examples/aerospike_inbound_dlq.yaml` | `ignore_error_codes` on one namespace and a dead-letter path on another |

```shell
task run CONF=./config/examples/aerospike_inbound.yaml
```

## Build

Requires the Go version in `go.mod`.

```shell
git clone git@github.com:aerospike/redpanda-connect
cd redpanda-connect
task build:redpanda-connect
```

`task build:all` also builds the cloud, community, and AI binaries. Components that link C libraries (for example `zmq4`) need `TAGS=x_benthos_extra` and the matching system libraries.

## Tests

```shell
task test          # unit and template tests
task fmt
task lint
task docs          # regenerate reference docs and lint the examples
```

Aerospike integration tests need Docker:

```shell
go test -count=1 -timeout 15m -run TestIntegration ./internal/impl/aerospike/
```

Enterprise login, TLS, and strong-consistency tests are local only. See `internal/impl/aerospike/testdata/ee/README.md`.

## Delivery

Connect acknowledges messages with an in-process transaction and no disk state. Against at-least-once sources and sinks that is at-least-once delivery, including across a process crash. A retried write can land twice. Full-record writes are idempotent. `fencing` rejects a stale replay when the mapping is not.

[connect]: https://github.com/redpanda-data/connect
[actions-badge]: https://github.com/aerospike/redpanda-connect/actions/workflows/test.yml/badge.svg
[actions-url]: https://github.com/aerospike/redpanda-connect/actions/workflows/test.yml
