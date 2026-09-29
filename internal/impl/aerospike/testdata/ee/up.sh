#!/usr/bin/env bash
# Start the local EE clusters used by TestEE* (and later tickets).
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"

KEY="${AEROSPIKE_FEATURE_KEY:-$DIR/features.conf}"
if [[ ! -f "$KEY" ]]; then
  echo "Need an Enterprise feature-key file." >&2
  echo "  export AEROSPIKE_FEATURE_KEY=/path/to/features.conf" >&2
  echo "  or copy it to $DIR/features.conf (gitignored)" >&2
  exit 1
fi
export AEROSPIKE_FEATURE_KEY="$KEY"

chmod +x "$DIR/gen-certs.sh"
"$DIR/gen-certs.sh"

cd "$DIR"
docker compose up -d

echo "waiting for as-ee-1 / as-ee-2 / as-ee-sec ..."
for c in as-ee-1 as-ee-2 as-ee-sec; do
  ready=0
  for _ in $(seq 1 60); do
    # Capture first. Piping into grep -q lets grep exit early, the producer
    # dies with SIGPIPE, and pipefail turns a ready node into a failed check.
    logs="$(docker logs "$c" 2>&1 || true)"
    if grep -q "soon there will be cake" <<<"$logs"; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "$ready" -ne 1 ]]; then
    echo "$c did not become ready" >&2
    exit 1
  fi
done

# Strong-consistency namespace needs a roster before writes succeed.
echo "waiting for as-ee-1 + as-ee-2 to form a 2-node cluster ..."
ready=0
for _ in $(seq 1 60); do
  stats="$(docker exec as-ee-1 asinfo -v statistics 2>/dev/null || true)"
  if grep -q 'cluster_size=2' <<<"$stats"; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" -ne 1 ]]; then
  echo "as-ee-1 and as-ee-2 did not form a 2-node cluster" >&2
  exit 1
fi

echo "staging SC roster on namespace sc ..."
ready=0
for _ in $(seq 1 30); do
  roster="$(docker exec as-ee-1 asinfo -v 'roster:namespace=sc' 2>/dev/null || true)"
  if [[ "$roster" == roster=* && "$roster" != roster=null* ]]; then
    echo "  $roster"
    ready=1
    break
  fi
  observed="${roster##*observed_nodes=}"
  if [[ -n "$observed" && "$observed" != "$roster" && "$observed" != null ]]; then
    docker exec as-ee-1 asinfo -v "roster-set:namespace=sc;nodes=$observed" >/dev/null 2>&1 || true
    docker exec as-ee-1 asinfo -v 'recluster:' >/dev/null 2>&1 || true
  fi
  sleep 2
done
if [[ "$ready" -ne 1 ]]; then
  echo "namespace sc never received a roster" >&2
  exit 1
fi

# Auth tests expect user rpcn / rpcnpass. Default docker admin is often admin/admin.
echo "creating user rpcn on as-ee-sec ..."
docker exec as-ee-sec asadm -U admin -P admin --enable -e \
  "manage acl create user rpcn password rpcnpass roles read-write sys-admin" || true
docker exec as-ee-sec asadm -U admin -P admin --enable -e \
  "manage acl grant user rpcn roles read-write sys-admin" || true

cat <<EOF

EE env for this checkout:

  export AEROSPIKE_EE_HOSTS=127.0.0.1:3100,127.0.0.1:3110
  export AEROSPIKE_SEC_HOST=127.0.0.1:3200
  export AEROSPIKE_TLS_HOST=127.0.0.1:clusterA:4333
  export AEROSPIKE_TLS_CA=$DIR/certs/ca.pem

  go test -count=1 -timeout 10m -v -run 'TestEE' ./internal/impl/aerospike/

Stop:  docker compose -f $DIR/docker-compose.yml down
EOF
