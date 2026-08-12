#!/bin/sh
set -e

PORT="${PORT:-8080}"
mkdir -p /root/.clawrig

# Render injects PORT at runtime; clawrig reads the port from config.
sed "s/^port = .*/port = ${PORT}/" /etc/clawrig/config.toml > /root/.clawrig/config.toml

exec clawrig serve --ui