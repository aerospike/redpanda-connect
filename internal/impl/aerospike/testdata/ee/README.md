# Local Aerospike Enterprise for `TestEE*`

These tests are **not** in GitHub Actions (the feature key changes). Engineers
run them locally. `TestEE*` skip unless the env vars below are set; they do
**not** start Docker themselves.

Community Edition (`TestIntegration*`) still uses testcontainers and does not
need this stack.

## Prerequisites

- Docker (Desktop on macOS is fine)
- Go (same version as `go.mod`)
- An Aerospike **Enterprise feature-key** file (`features.conf`). Do not commit
  it. Ask a teammate or license owner for the current file.

Image: `aerospike/aerospike-server-enterprise:8.1`. Docker Desktop project
name: `aero-redpanda-ee`.

## What you get (3 containers)

| Container | Host address | Env | Role |
|---|---|---|---|
| `as-ee-1` | `127.0.0.1:3100` | `AEROSPIKE_EE_HOSTS` | Unsecured 2-node cluster, RF=2, cluster-name `aero-integ-redpanda-ee` |
| `as-ee-2` | `127.0.0.1:3110` | (same list) | Peer of node 1; namespaces `test` (AP) and `sc` (strong consistency) |
| `as-ee-sec` | `127.0.0.1:3200` plaintext, `127.0.0.1:clusterA:4333` TLS | `AEROSPIKE_SEC_HOST`, `AEROSPIKE_TLS_HOST` | Separate cluster `aero-integ-redpanda-ee-sec`; user `rpcn` / `rpcnpass` |

On Docker Desktop, configs use loopback `access-address` / `access-port`.
Do not point tests at `172.17` / `172.20` container IPs.

TLS certs under `certs/` are gitignored. `./up.sh` reuses them if present,
otherwise runs `./gen-certs.sh`. Force new certs with `FORCE=1 ./gen-certs.sh`.

## Start

From the **repository root**:

```bash
cd internal/impl/aerospike/testdata/ee

# Either copy the key here (gitignored):
cp /path/to/features.conf .

# Or point at it:
# export AEROSPIKE_FEATURE_KEY=/path/to/features.conf

chmod +x up.sh gen-certs.sh
./up.sh
```

`./up.sh` takes a couple of minutes (image pull on first run, then cluster
form + SC roster). It prints the `export` lines to copy.

If you previously ran the old Compose project `aerospike-ee`, remove it once
so names do not collide:

```bash
docker compose -p aerospike-ee down
```

## Run tests

From the **repository root** (not from `testdata/ee`). Use an **absolute**
`AEROSPIKE_TLS_CA` (`go test` cwd is the package directory).

```bash
export AEROSPIKE_EE_HOSTS=127.0.0.1:3100,127.0.0.1:3110
export AEROSPIKE_SEC_HOST=127.0.0.1:3200
export AEROSPIKE_TLS_HOST=127.0.0.1:clusterA:4333
export AEROSPIKE_TLS_CA="$PWD/internal/impl/aerospike/testdata/ee/certs/ca.pem"

go test -count=1 -timeout 10m -v -run 'TestEE' ./internal/impl/aerospike/
```

`-count=1` disables Go's test cache. Cached `ok` does not prove the cluster
is up.

Auth tests use `rpcn` / `rpcnpass`. If user create failed, the image may use
a different admin password:

```bash
docker exec -it as-ee-sec asadm -U admin -P '<admin-password>' --enable \
  -e "manage acl create user rpcn password rpcnpass roles read-write sys-admin"
```

If `TestEEStrongConsistency` fails, wait for both unsecured nodes then restage
the `sc` roster:

```bash
docker exec as-ee-1 asinfo -v statistics | tr ';' '\n' | grep cluster_size
docker exec as-ee-1 asadm --enable -e "manage roster stage observed ns sc"
docker exec as-ee-1 asadm --enable -e "manage recluster"
```

## Stop

```bash
cd internal/impl/aerospike/testdata/ee
docker compose down
```

`features.conf` and `certs/*` (except `.gitkeep`) stay gitignored.
