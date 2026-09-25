#!/usr/bin/env sh
set -eu

ACTION="${1:-up}"
NODES="${2:-3}"
[ "$NODES" -ge 3 ] 2>/dev/null || { echo "Whitewater requires at least 3 nodes" >&2; exit 2; }
COMPOSE_FILE="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/compose.cluster.yml"

case "$ACTION" in
  up)
    docker compose -f "$COMPOSE_FILE" up -d --build --scale "node=$NODES"
    ;;
  scale)
    docker compose -f "$COMPOSE_FILE" up -d --scale "node=$NODES" --remove-orphans
    ;;
  status)
    docker compose -f "$COMPOSE_FILE" ps
    ;;
  down)
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
    ;;
  smoke)
    ids=$(docker compose -f "$COMPOSE_FILE" ps -q node)
    count=$(printf '%s\n' "$ids" | sed '/^$/d' | wc -l | tr -d ' ')
    [ "$count" = "$NODES" ] || { echo "Expected $NODES nodes, found $count" >&2; exit 1; }
    for id in $ids; do
      seen=$(docker exec "$id" curl --fail --silent http://127.0.0.1:7070/v1/cluster/members | grep -o '"node_id"' | wc -l | tr -d ' ')
      [ "$seen" = "$NODES" ] || { echo "Node $id sees $seen members, expected $NODES" >&2; exit 1; }
    done
    echo "All $NODES nodes report complete membership."
    ;;
  *)
    echo "Usage: $0 {up|scale|status|down|smoke} [node-count]" >&2
    exit 2
    ;;
esac
