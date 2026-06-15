#!/bin/bash
# gen-certs.sh — generate self-signed CA + server cert for integration tests.
# Certs are valid for 365 days, CN=localhost, SAN includes Docker hostnames.
set -euo pipefail

CERT_DIR="${1:-$(dirname "$0")/../fixtures/certs}"
mkdir -p "$CERT_DIR"

# Skip if certs already exist and are valid
if [ -f "$CERT_DIR/server.pem" ] && [ -f "$CERT_DIR/server.key" ]; then
    echo "[gen-certs] Certs already exist at $CERT_DIR, skipping."
    exit 0
fi

echo "[gen-certs] Generating self-signed certs in $CERT_DIR"

# --- CA ---
openssl genrsa -out "$CERT_DIR/ca.key" 2048 2>/dev/null
openssl req -x509 -new -nodes \
    -key "$CERT_DIR/ca.key" \
    -sha256 -days 365 \
    -subj "/CN=poseidon-test-ca" \
    -out "$CERT_DIR/ca.pem" 2>/dev/null

# --- Server cert with SAN ---
cat > "$CERT_DIR/server.ext" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=@alt_names

[alt_names]
DNS.1=localhost
DNS.2=nginx
DNS.3=undertow
DNS.4=nghttp2
DNS.5=*.poseidon-it.local
IP.1=127.0.0.1
IP.2=172.30.0.10
IP.3=172.30.0.11
IP.4=172.30.0.12
EOF

openssl genrsa -out "$CERT_DIR/server.key" 2048 2>/dev/null
openssl req -new \
    -key "$CERT_DIR/server.key" \
    -subj "/CN=localhost" \
    -out "$CERT_DIR/server.csr" 2>/dev/null

openssl x509 -req \
    -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca.pem" \
    -CAkey "$CERT_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERT_DIR/server.pem" \
    -days 365 -sha256 \
    -extfile "$CERT_DIR/server.ext" 2>/dev/null

# Cleanup intermediate files
rm -f "$CERT_DIR/server.csr" "$CERT_DIR/server.ext" "$CERT_DIR/ca.srl"

echo "[gen-certs] Done: server.pem, server.key, ca.pem in $CERT_DIR"
