#!/bin/bash
# ============================================================================
# deploy.sh — Rolling update + automatic rollback for local deployment
# ============================================================================
# Usage: ./deploy.sh [--rollback]
#   ./deploy.sh              Deploy latest image from GHCR
#   ./deploy.sh --rollback   Rollback to previous image
# ============================================================================
set -euo pipefail

COMPOSE_FILE="source/deployment/compose.yaml"
COMPOSE_CD="source/deployment/compose.cd.yaml"
COMPOSE_OPTS="-f $COMPOSE_FILE -f $COMPOSE_CD"
SERVICE="airflow"
IMAGE="ghcr.io/mhmdwldn/ecommerce-crawler-airflow:latest"
HEALTH_URL="http://localhost:8080/health"
ROLLBACK_TAG="ghcr.io/mhmdwldn/ecommerce-crawler-airflow:rollback"
TIMEOUT=60

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[DEPLOY]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err() { echo -e "${RED}[ERROR]${NC} $1"; }

# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------
if [ "${1:-}" = "--rollback" ]; then
    log "Rolling back to previous image..."
    docker tag "$ROLLBACK_TAG" "$IMAGE" 2>/dev/null || {
        err "No rollback image found ($ROLLBACK_TAG)"
        exit 1
    }
    docker compose $COMPOSE_OPTS up -d "$SERVICE"
    log "Rollback complete"
    exit 0
fi

# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------

# 1. Save current image as rollback
log "Saving current image as rollback..."
CURRENT=$(docker inspect --format='{{.Image}}' "$SERVICE" 2>/dev/null || echo "")
if [ -n "$CURRENT" ]; then
    docker tag "$IMAGE" "$ROLLBACK_TAG" 2>/dev/null || true
    log "  Rollback image saved: $ROLLBACK_TAG"
else
    warn "  No running $SERVICE container — skipping rollback backup"
fi

# 2. Pull latest image
log "Pulling latest image..."
if ! docker pull "$IMAGE"; then
    err "Pull failed. Aborting."
    exit 1
fi

# 3. Restart service with new image
log "Restarting $SERVICE..."
docker compose $COMPOSE_OPTS up -d --no-deps "$SERVICE"

# 4. Health check — wait for service to be healthy
log "Waiting for $SERVICE to become healthy (timeout=${TIMEOUT}s)..."
START=$(date +%s)
while true; do
    if curl -s -o /dev/null -w "%{http_code}" "$HEALTH_URL" 2>/dev/null | grep -q "200\|302"; then
        log "$SERVICE is healthy!"
        docker tag "$IMAGE" "$ROLLBACK_TAG"
        log "Deploy complete: $IMAGE"
        exit 0
    fi

    ELAPSED=$(($(date +%s) - START))
    if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
        break
    fi
    sleep 5
    echo -n "."
done

# 5. Health check failed — rollback
err "Health check failed after ${TIMEOUT}s"
log "Rolling back to previous image..."

if docker image inspect "$ROLLBACK_TAG" >/dev/null 2>&1; then
    docker tag "$ROLLBACK_TAG" "$IMAGE"
    docker compose $COMPOSE_OPTS up -d "$SERVICE"
    log "Rollback complete — service restored to previous version"
else
    err "No rollback image — manual intervention required"
    err "Try: docker compose -f $COMPOSE_FILE up -d --build $SERVICE"
fi
exit 1
