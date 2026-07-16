#!/usr/bin/env bash
# start.sh — startup berurutan dengan health-check antar service
# Usage: bash start.sh
# Gunakan ini sebagai pengganti `docker compose up -d` langsung.

set -euo pipefail

COMPOSE="docker compose -f source/deployment/compose.yaml"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()   { echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $1"; }
warn()  { echo -e "${YELLOW}[$(date +%H:%M:%S)] WARN${NC} $1"; }
err()   { echo -e "${RED}[$(date +%H:%M:%S)] ERROR${NC} $1"; }

# ---------------------------------------------------------------------------
# Step 1: Zookeeper (Kafka gak bisa start sebelum ZK siap)
# ---------------------------------------------------------------------------
log "Step 1/7: Starting Zookeeper..."
$COMPOSE up -d zookeeper

log "  Waiting for Zookeeper (port 2181)..."
for i in $(seq 1 30); do
    if docker exec zookeeper bash -c "echo srvr | nc localhost 2181" 2>/dev/null | grep -q Zookeeper; then
        log "  Zookeeper ready."
        break
    fi
    [ "$i" -eq 30 ] && err "Zookeeper tidak siap setelah 30 detik." && exit 1
    sleep 1
done

# ---------------------------------------------------------------------------
# Step 2: Kafka — bersihin stale ZK node, lalu start
# ---------------------------------------------------------------------------
log "Step 2/7: Cleaning stale ZK broker node..."
# Kalau ZK volume persisten, broker ID lama bisa masih ada → NodeExists.
# Hapus dulu, aman buat fresh start maupun restart.
docker exec zookeeper zookeeper-shell localhost:2181 deleteall /brokers/ids/1 &>/dev/null || true
log "  Stale node cleaned (if any)."

log "  Starting Kafka..."
$COMPOSE up -d kafka

log "  Waiting for Kafka broker to accept connections..."
for i in $(seq 1 45); do
    if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092 &>/dev/null; then
        log "  Kafka broker ready."
        break
    fi
    [ "$i" -eq 45 ] && err "Kafka tidak siap setelah 45 detik." && exit 1
    sleep 2
done

# ---------------------------------------------------------------------------
# Step 3: Infra dasar (Postgres, MinIO, ES, ClickHouse)
# ---------------------------------------------------------------------------
log "Step 3/7: Starting core infrastructure..."
$COMPOSE up -d postgres minio minio-init elasticsearch clickhouse

log "  Waiting for Postgres..."
for i in $(seq 1 30); do
    if docker exec postgres-mart pg_isready -U mart -d mart &>/dev/null; then
        log "  Postgres ready."
        break
    fi
    [ "$i" -eq 30 ] && err "Postgres tidak siap setelah 30 detik." && exit 1
    sleep 1
done

# ---------------------------------------------------------------------------
# Step 4: DDL + Seed Asset Registry (harus sebelum DAG crawl)
# ---------------------------------------------------------------------------
log "Step 4/7: Applying Asset Registry DDL..."
cat assets/ddl/crawl_assets.sql | docker exec -i postgres-mart psql -U mart -d mart -q 2>/dev/null
log "  DDL applied."

log "  Seeding assets..."
PYTHONPATH="assets" python -m assets.seed 2>&1 | while read line; do
    log "    $line"
done

# ---------------------------------------------------------------------------
# Step 5: Bootstrap Kafka topic + ES index
# ---------------------------------------------------------------------------
log "Step 5/7: Bootstrapping Kafka topic + Elasticsearch index..."
(
    cd source
    PYTHONPATH=source python -m library.setup_infra 2>&1 | while read line; do
        log "    $line"
    done
)

# ---------------------------------------------------------------------------
# Step 6: Services pendukung (BI, monitoring, logging, vault, reverse proxy)
# ---------------------------------------------------------------------------
log "Step 6/7: Starting BI + monitoring + logging + security..."
$COMPOSE up -d kibana airflow superset metabase prometheus grafana alertmanager postgres-exporter airflow-statsd fluentbit caddy vault

# ---------------------------------------------------------------------------
# Step 7: Verifikasi semua service sehat
# ---------------------------------------------------------------------------
log "Step 7/7: Verifying all services..."
sleep 5
echo ""
log "=== Service Status ==="
docker ps --format "table {{.Names}}\t{{.Status}}" 2>/dev/null
echo ""

# Cek Kafka topic
log "=== Key Endpoints ==="
echo "  Airflow     : http://localhost:8080  (admin/admin)"
echo "  Metabase    : http://localhost:3000  (admin@tokocrawl.local / admin12345)"
echo "  Superset    : http://localhost:8088  (admin/admin)"
echo "  Grafana     : http://localhost:3001  (admin/admin)"
echo "  Kibana      : http://localhost:5601"
echo "  Prometheus  : http://localhost:9090"
echo "  Vault       : http://localhost:8200  (token: root-token-dev)"
echo "  MinIO       : http://localhost:9001  (minioadmin / minioadmin)"
echo "  Caddy proxy : http://localhost:8081"
echo ""

# Quick health report
FAILED_COUNT=$(docker ps --filter "status=exited" --format "." 2>/dev/null | wc -l)
if [ "$FAILED_COUNT" -gt 0 ]; then
    warn "Ada $FAILED_COUNT container exited — cek dengan: docker ps -a --filter 'status=exited'"
else
    log "Semua service running. Siap jalanin DAG."
fi
