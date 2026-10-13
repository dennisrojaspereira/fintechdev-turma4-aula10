#!/usr/bin/env bash
# Aula 10: controlador do Game Day de Disaster Recovery. Sem argumentos, lista os comandos.
#   scripts/lab10.sh up | status | chaos start | promote --autorizado-por NOME | check | report ...
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="$(command -v python3 || command -v python)"
exec "$PY" "$DIR/aula10/lab10.py" "$@"
