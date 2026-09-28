#!/usr/bin/env bash
# Generate test-only TLS material for as-ee-sec. Gitignored; not for production.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
CERTS="$DIR/certs"
mkdir -p "$CERTS"

need=(ca.pem ca.key server.pem server.key)
have_all=1
for f in "${need[@]}"; do
  if [[ ! -f "$CERTS/$f" ]]; then
    have_all=0
    break
  fi
done
if [[ "$have_all" -eq 1 && "${FORCE:-}" != "1" ]]; then
  echo "using existing $CERTS/{ca,server}.{pem,key} (FORCE=1 to regenerate)"
  exit 0
fi

cd "$CERTS"
rm -f ca.pem ca.key ca.srl server.pem server.key server.csr ext.cnf
openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
  -subj "/CN=Aerospike EE test CA" \
  -keyout ca.key -out ca.pem
openssl req -newkey rsa:2048 -nodes \
  -subj "/CN=clusterA" \
  -keyout server.key -out server.csr
cat > ext.cnf <<'EOF'
subjectAltName=DNS:localhost,DNS:as-ee-sec,DNS:clusterA,IP:127.0.0.1
EOF
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out server.pem -days 3650 -extfile ext.cnf
rm -f server.csr ext.cnf
echo "wrote $CERTS/ca.pem server.pem server.key"
