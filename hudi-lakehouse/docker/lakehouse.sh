#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION=
BUILD=0
LOCAL_LLM=0
CHAT_CONFIG_ERROR=
usage() {
  cat <<'HELP'
Usage: lakehouse.sh <up|down|status|logs|test> [--build] [--local-llm]
  up             Start services, seed demo data, verify MCP and report chat readiness.
  --build        Build Spark Connect and Gateway images in Docker from this checkout.
  --local-llm    Start containerized Ollama and pull GATEWAY_LLM_MODEL (default qwen3:8b).
  test           Verify sample data and gateway MCP without reseeding.
  down           Remove containers/network; preserve all named data volumes.
  status|logs    Inspect this Compose installation.
  -h, --help     Show this help (also shown when no arguments are supplied).
Set COMPOSE_PROJECT_NAME to isolate an installation; configure ports/provider in .env.
An unavailable chat LLM does not fail startup; MCP clients use their own model.
HELP
}
if [[ $# -eq 0 ]]; then
  usage
  exit 0
fi
while [[ $# -gt 0 ]]; do
  case "$1" in
    up|down|status|logs|test)
      if [[ -n "$ACTION" ]]; then
        echo 'Specify exactly one command.' >&2; usage >&2; exit 2
      fi
      ACTION="$1" ;;
    --build) BUILD=1 ;;
    --local-llm) LOCAL_LLM=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done
if [[ -z "$ACTION" ]]; then
  echo 'Specify a command, for example: lakehouse.sh up' >&2
  usage >&2
  exit 2
fi
command -v docker >/dev/null || { echo 'Install Docker with Compose 2.27.2 or newer first.' >&2; exit 1; }
COMPOSE_VERSION="$(docker compose version --short)" || {
  echo 'Docker Compose 2.27.2 or newer is required.' >&2; exit 1;
}
# config --environment was introduced in 2.27.2. Allow Docker Desktop suffixes.
if [[ ! "$COMPOSE_VERSION" =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+)([-+].*)?$ ]] ||
    (( 10#${BASH_REMATCH[1]} < 2 ||
       (10#${BASH_REMATCH[1]} == 2 && (10#${BASH_REMATCH[2]} < 27 ||
        (10#${BASH_REMATCH[2]} == 27 && 10#${BASH_REMATCH[3]} < 2))) )); then
  echo "Docker Compose 2.27.2 or newer is required; found '$COMPOSE_VERSION'. Upgrade Docker Compose and retry." >&2
  exit 1
fi
MEMORY="$(docker info --format '{{.MemTotal}}')" || {
  echo 'Docker is not running or is inaccessible. Start Docker and retry.' >&2; exit 1;
}
COMPOSE=(docker compose --project-directory "$HERE" -f "$HERE/compose.yaml")
if [[ -f "$HERE/.env" ]]; then
  COMPOSE+=(--env-file "$HERE/.env")
fi
if [[ "$BUILD" == 1 ]]; then
  COMPOSE+=(-f "$HERE/compose.build.yaml")
fi
if [[ "$LOCAL_LLM" == 1 ]]; then
  export GATEWAY_LLM_PROVIDER=ollama GATEWAY_OLLAMA_BASE_URL=http://ollama:11434
  COMPOSE+=(--profile local-llm)
fi
case "$ACTION" in
  down) "${COMPOSE[@]}" --profile local-llm down; exit ;;
  status) "${COMPOSE[@]}" --profile local-llm ps -a; exit ;;
  logs) "${COMPOSE[@]}" --profile local-llm logs --tail=100; exit ;;
esac
if [[ "$ACTION" == up ]]; then
  if [[ ! "$MEMORY" =~ ^[0-9]+$ ]] || (( MEMORY < 7500000000 )); then
    echo 'Allocate at least 8 GB of memory to Docker (more for containerized Ollama).' >&2
    exit 1
  fi
  # Auto-select API credentials from the environment only when the user has not
  # supplied a .env configuration or an explicit provider. Never persist API keys.
  if [[ ! -f "$HERE/.env" && -z "${GATEWAY_LLM_PROVIDER:-}" ]]; then
    if [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
      export GATEWAY_LLM_PROVIDER=anthropic
    elif [[ -n "${OPENAI_API_KEY:-}" ]]; then
      export GATEWAY_LLM_PROVIDER=openai
    fi
  fi
  # Let Compose resolve .env interpolation and shell precedence. Read only the
  # provider/model; never eval or print this output because it can contain keys.
  CONFIG_ENV="$("${COMPOSE[@]}" config --environment)"
  LLM_PROVIDER=ollama
  LLM_MODEL=
  while IFS='=' read -r key value; do
    case "$key" in
      GATEWAY_LLM_PROVIDER) LLM_PROVIDER="${value:-ollama}" ;;
      GATEWAY_LLM_MODEL) LLM_MODEL="$value" ;;
    esac
  done <<< "$CONFIG_ENV"
  unset CONFIG_ENV
  if [[ "$LLM_PROVIDER" != ollama && -z "$LLM_MODEL" ]]; then
    CHAT_CONFIG_ERROR='Set GATEWAY_LLM_MODEL to your API provider model and run lakehouse.sh up again.'
  fi
  LLM_MODEL="${LLM_MODEL:-qwen3:8b}"
  if [[ "$BUILD" == 1 ]]; then
    "${COMPOSE[@]}" build spark-connect gateway
  fi
  # Start Ollama and ensure its model exists before starting the Gateway.
  if [[ "$LOCAL_LLM" == 1 ]]; then
    "${COMPOSE[@]}" up -d --wait --wait-timeout 180 ollama
    if ! "${COMPOSE[@]}" exec -T ollama ollama show "$LLM_MODEL" >/dev/null 2>&1; then
      "${COMPOSE[@]}" exec -T ollama ollama pull "$LLM_MODEL"
    fi
  fi
  if ! "${COMPOSE[@]}" up -d --wait --wait-timeout 900 gateway; then
    "${COMPOSE[@]}" ps -a >&2
    echo 'Installation failed. Run lakehouse.sh logs; if images are unpublished, use --build.' >&2
    exit 1
  fi
fi
# up already verified the data in the seed dependency; test must not reseed it.
if [[ "$ACTION" == test ]]; then
  "${COMPOSE[@]}" exec -T spark-connect python3 - --verify-only < "$HERE/scripts/seed.py"
fi
"${COMPOSE[@]}" exec -T gateway python /lakehouse/smoke-test.py
echo 'Lakehouse and MCP are ready.'
if [[ "$ACTION" == up ]]; then
  if [[ -n "$CHAT_CONFIG_ERROR" ]]; then
    echo "Chat is unavailable: $CHAT_CONFIG_ERROR" >&2
  elif ! "${COMPOSE[@]}" exec -T gateway python - < "$HERE/scripts/check-llm.py"; then
    echo 'Chat is unavailable: LLM readiness check failed (see details above).' >&2
    echo 'Configure the LLM in .env or use up --local-llm. MCP remains available.' >&2
  fi
fi
# Query resolved values without printing the environment (which can contain keys).
GATEWAY_ADDRESS="$("${COMPOSE[@]}" port gateway 8000)"
CONNECT_ADDRESS="$("${COMPOSE[@]}" port spark-connect 15002)"
printf '\nChat UI:       http://%s/ui/\n' "$GATEWAY_ADDRESS"
printf 'Connect Claude: claude mcp add --transport http hudi http://%s/mcp/\n' "$GATEWAY_ADDRESS"
printf 'Spark Connect: sc://%s\n' "$CONNECT_ADDRESS"
if [[ "$ACTION" == test ]]; then
  echo 'LLM readiness was not checked; built-in chat requires a configured LLM.'
fi
