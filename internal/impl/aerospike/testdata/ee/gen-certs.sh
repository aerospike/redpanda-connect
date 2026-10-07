#!/usr/bin/env bash
# Generate test-only TLS material for as-ee-sec. Gitignored; not for production.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
CERTS="$DIR/certs"
mkdir -p "$CERTS"

cd "$CERTS"
if [[ ! -f ca.pem || ! -f ca.key || ! -f server.pem || ! -f server.key || "${FORCE:-}" == "1" ]]; then
  rm -f ca.pem ca.key ca.srl server.pem server.key client.pem client.key client.enc.key
  openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
    -subj "/CN=Aerospike EE test CA" \
    -keyout ca.key -out ca.pem
  openssl req -newkey rsa:2048 -nodes \
    -subj "/CN=clusterA" \
    -keyout server.key -out server.csr
  printf '%s\n' "subjectAltName=DNS:localhost,DNS:as-ee-sec,DNS:clusterA,IP:127.0.0.1" > server.ext
  openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -out server.pem -days 3650 -extfile server.ext
  rm -f server.csr server.ext
  echo "wrote $CERTS/ca.pem server.pem server.key"
fi

# CN rpcn matches the Aerospike user. client.enc.key is the same key, PKCS#8
# encrypted. The test password is rpcnkey. An existing CA is kept.
if [[ ! -f client.pem || ! -f client.key || "${FORCE:-}" == "1" ]]; then
  openssl req -newkey rsa:2048 -nodes \
    -subj "/CN=rpcn" \
    -keyout client.key -out client.csr
  printf '%s\n' "extendedKeyUsage=clientAuth" > client.ext
  openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -out client.pem -days 3650 -extfile client.ext
  rm -f client.csr client.ext client.enc.key
  echo "wrote $CERTS/client.pem client.key"
fi
if [[ ! -f client.enc.key || "${FORCE:-}" == "1" ]]; then
  openssl pkcs8 -topk8 -v2 aes-256-cbc \
    -in client.key -out client.enc.key -passout pass:rpcnkey
  echo "wrote $CERTS/client.enc.key"
fi
