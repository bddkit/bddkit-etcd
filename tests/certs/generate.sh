#!/bin/sh
# Regenerates the TEST-ONLY certificates the secure etcd servers of
# docker-compose.yml and the tests use. Nothing here protects anything: the
# keys are committed. Run it when they expire, then commit the result.
#
#   ca.crt         the CA the servers' and the client's certificates chain to
#   other-ca.crt   an unrelated CA, for "the server's CA is not the one given"
#   server.crt     the server certificate; its only name is etcd.test, so the
#                  address 127.0.0.1 is NOT covered (tls_server_name is needed)
#   client.crt     a client certificate for mutual TLS, signed by ca.crt
#   rogue-client.crt  a client certificate signed by other-ca.crt, which the
#                  mTLS server does not trust
set -eu
cd "$(dirname "$0")"
DAYS=36500

ca() { # <prefix> <common name>
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1.key.tmp"
  openssl req -x509 -new -key "$1.key.tmp" -sha256 -days "$DAYS" -subj "/CN=$2" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$1.crt"
}

leaf() { # <name> <common name> <extended key usage> <subject alt name or empty> <signing CA>
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1.key"
  openssl req -new -key "$1.key" -subj "/CN=$2" -out "$1.csr"
  {
    echo "basicConstraints=critical,CA:FALSE"
    echo "keyUsage=critical,digitalSignature"
    echo "extendedKeyUsage=$3"
    [ -z "$4" ] || echo "subjectAltName=$4"
  } > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA "$5.crt" -CAkey "$5.key.tmp" -CAcreateserial \
    -sha256 -days "$DAYS" -extfile "$1.ext" -out "$1.crt"
  rm "$1.csr" "$1.ext"
}

ca ca "bddkit-etcd test CA"
ca other-ca "bddkit-etcd other test CA"
leaf server etcd.test serverAuth "DNS:etcd.test" ca
leaf client bddkit-etcd-client clientAuth "" ca
leaf rogue-client bddkit-etcd-rogue clientAuth "" other-ca
# The CA keys are not kept: nothing needs to sign again until the next run.
rm -f ca.key.tmp other-ca.key.tmp ca.srl other-ca.srl
chmod 644 *.key
