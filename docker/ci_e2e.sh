#!/bin/sh
# E2E против api/relay/consumer под coverage; после SIGTERM — .coverage* → OUT_DIR.
set -eu

COMPOSE='docker compose -f docker-compose.yml -f docker-compose.coverage.yml'
OUT_DIR="${1:-coverage-data/e2e}"
APPS='api outbox-relay consumer'

cleanup() {
  if [ "${DONT_CLEANUP:-}" = '1' ]; then
    return
  fi
  $COMPOSE --profile coverage-report down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

$COMPOSE --profile coverage-report down --remove-orphans >/dev/null 2>&1 || true
# shellcheck disable=SC2086
$COMPOSE up --build -d --force-recreate db rabbitmq coverage-init migrate $APPS webhook-sink tests-e2e
$COMPOSE exec -T tests-e2e pytest tests/e2e -q
# Сброс parallel coverage из долгоживущих процессов
# shellcheck disable=SC2086
$COMPOSE stop $APPS

COMPOSE="$COMPOSE" ./docker/extract_coverage_data.sh "${OUT_DIR}"
