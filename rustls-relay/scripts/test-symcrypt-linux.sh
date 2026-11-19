#!/usr/bin/env bash
#
# Runs the relay round-trip test in a Linux container with the symcrypt
# crypto provider.
#
# Usage: test-symcrypt-linux.sh <symcrypt-linux-generic-ARCH-release-*.tar.gz>
# Get the tarball from https://github.com/microsoft/SymCrypt/releases.
#
# The repository is mounted at the same path in the container, so the paths
# that the test gives to the Docker daemon are also valid on the host.
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
symcrypt_tgz=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
target="$repo/target/linux-symcrypt"
image=${RUST_IMAGE:-rust:1.91-bookworm}
mkdir -p "$target/registry"

docker run --rm \
  -v "$repo:$repo" -w "$repo" \
  -v "$symcrypt_tgz:/tmp/symcrypt.tar.gz:ro" \
  -v "$target/registry:/usr/local/cargo/registry" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e CARGO_TARGET_DIR="$target" \
  -e RUST_LOG="${RUST_LOG:-rustls_relay=info}" \
  "$image" bash -euo pipefail -c '
    libdir=/usr/lib/$(gcc -dumpmachine)
    mkdir -p /tmp/symcrypt
    tar -xzf /tmp/symcrypt.tar.gz -C /tmp/symcrypt
    cp -a /tmp/symcrypt/lib/libsymcrypt.so* "$libdir/"
    ldconfig
    curl -fsSL "https://download.docker.com/linux/static/stable/$(uname -m)/docker-27.5.1.tgz" \
      | tar -xz -C /usr/local/bin --strip-components=1 docker/docker
    export RELAY_DOCKER_NETWORK=container:$(hostname)
    cargo test --locked -p rustls-relay --no-default-features --features symcrypt \
      --test tls_round_trip -- --ignored --nocapture
    bin=$(ls -t "$CARGO_TARGET_DIR"/debug/deps/tls_round_trip-* | grep -v "\.d$" | head -n 1)
    echo "== ldd $bin"
    ldd "$bin"
  '
