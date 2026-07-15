#!/bin/bash
# ============================================================================
# backup.sh — daily backup: Postgres DDL + ClickHouse schema + MinIO data
# Run manually or via cron: 0 3 * * * /path/to/backup.sh
# ============================================================================
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-./backups}"
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
RETENTION_DAYS=7

mkdir -p "$BACKUP_DIR/$TIMESTAMP"

echo "=== Backup started: $TIMESTAMP ==="

# 1. Postgres — dump mart schema + control schema
echo "Backing up Postgres..."
docker exec postgres-mart pg_dump -U mart -d mart --schema-only \
  > "$BACKUP_DIR/$TIMESTAMP/pg_mart_schema.sql"
docker exec postgres-mart pg_dump -U mart -d mart --schema=control --schema-only \
  > "$BACKUP_DIR/$TIMESTAMP/pg_control_schema.sql"
# Also dump asset registry data (critical for pipeline operation)
docker exec postgres-mart pg_dump -U mart -d mart --schema=control --data-only \
  > "$BACKUP_DIR/$TIMESTAMP/pg_control_data.sql"

# 2. ClickHouse — dump DDL for analytics schema
echo "Backing up ClickHouse..."
docker exec clickhouse clickhouse-client --user ch_user --password ch_pass --query "
  SELECT concat('CREATE TABLE ', name, ' ENGINE=', engine, ' AS SELECT * FROM ', name, ' LIMIT 0')
  FROM system.tables WHERE database='analytics'
  FORMAT TSV
" > "$BACKUP_DIR/$TIMESTAMP/ch_ddl.sql"

# 3. MinIO — sync lakehouse to backup bucket
echo "Syncing MinIO lakehouse data..."
docker exec minio mc alias set backup http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
docker exec minio mc mirror local/lakehouse backup/lakehouse-backup 2>/dev/null || echo "  MinIO sync skipped (mc not configured)"

# 4. Write manifest
cat > "$BACKUP_DIR/$TIMESTAMP/manifest.txt" << EOF
Backup timestamp: $TIMESTAMP
Host: $(hostname)
Services: postgres, clickhouse, minio
EOF

echo "Backup complete: $BACKUP_DIR/$TIMESTAMP"

# 5. Cleanup old backups
find "$BACKUP_DIR" -maxdepth 1 -type d -mtime +$RETENTION_DAYS -exec rm -rf {} \; 2>/dev/null || true
echo "Cleaned backups older than $RETENTION_DAYS days"
