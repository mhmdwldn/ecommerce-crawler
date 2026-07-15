"""Asset registry repository — satu-satunya pintu akses ke control.crawl_assets.

Dipakai oleh:
  - Airflow DAG      : get_due_assets(), mark_success(), mark_failure()
  - Streamlit admin  : list_assets(), create_asset(), update_asset(), delete_asset()
  - seed.py          : upsert_asset()

Semua timestamp UTC (keputusan #6, PRD_40).

Config mengikuti pola project (library/config.py, pydantic-settings,
prefix TOKOPEDIA_, delimiter __): set salah satu dari
  TOKOPEDIA_CONTROL__DSN=host=...;port=...;dbname=...;user=...;password=...
atau override langsung lewat CONTROL_DSN untuk kebutuhan lokal/testing.
Kalau nanti control-plane config dipindah resmi ke library/config.py sebagai
ControlPlaneSettings, baris get_dsn() di bawah tinggal diarahkan ke situ —
pemanggil (repository functions) tidak perlu berubah.
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Any, Iterator, Optional

import psycopg2
import psycopg2.extras

# Ambang circuit breaker — FR-19 (PRD_20)
MAX_CONSECUTIVE_FAILURES = 5


def get_dsn() -> str:
    """Resolve DSN: TOKOPEDIA_CONTROL__DSN (pola project) > CONTROL_DSN (override lokal) > default dev."""
    return (
        os.getenv("TOKOPEDIA_CONTROL__DSN")
        or os.getenv("CONTROL_DSN")
        or "host=localhost port=5433 dbname=mart user=mart password=mart"
    )


DSN = get_dsn()


@contextmanager
def get_conn(dsn: str | None = None) -> Iterator[psycopg2.extensions.connection]:
    """Koneksi Postgres dengan commit/rollback otomatis."""
    conn = psycopg2.connect(dsn or DSN)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _dict_cur(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


# ---------------------------------------------------------------------------
# READ
# ---------------------------------------------------------------------------

def get_due_assets(limit: int = 50, dsn: str | None = None) -> list[dict[str, Any]]:
    """Asset yang layak di-crawl sekarang (aturan 'due' — PRD_50).

    Dipakai Airflow sebagai input dynamic task mapping: crawl.expand(asset=...).
    Sudah terurut priority ASC, lalu yang paling lama tidak di-crawl.
    """
    with get_conn(dsn) as conn, _dict_cur(conn) as cur:
        cur.execute("SELECT * FROM control.v_due_assets LIMIT %s", (limit,))
        return [dict(r) for r in cur.fetchall()]


def list_assets(
    category: Optional[str] = None,
    active_only: bool = False,
    dsn: str | None = None,
) -> list[dict[str, Any]]:
    """Semua asset, opsional difilter kategori / hanya yang aktif."""
    sql = "SELECT * FROM control.crawl_assets WHERE 1=1"
    params: list[Any] = []
    if category:
        sql += " AND category = %s"
        params.append(category)
    if active_only:
        sql += " AND is_active"
    sql += " ORDER BY priority ASC, label ASC"

    with get_conn(dsn) as conn, _dict_cur(conn) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def get_asset(asset_id: int, dsn: str | None = None) -> Optional[dict[str, Any]]:
    with get_conn(dsn) as conn, _dict_cur(conn) as cur:
        cur.execute("SELECT * FROM control.crawl_assets WHERE asset_id = %s", (asset_id,))
        row = cur.fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# WRITE — CRUD
# ---------------------------------------------------------------------------

def create_asset(
    *,
    crawl_type: str,
    payload: dict[str, Any],
    label: str,
    category: str,
    priority: int = 5,
    cadence_min: int = 60,
    platform: str = "tokopedia",
    is_active: bool = True,
    notes: str | None = None,
    dsn: str | None = None,
) -> int:
    """Tambah asset baru. Mengembalikan asset_id.

    Raise psycopg2.errors.UniqueViolation jika target (platform+type+payload) sudah ada.
    """
    with get_conn(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO control.crawl_assets
                (platform, crawl_type, payload, label, category,
                 priority, cadence_min, is_active, notes)
            VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
            RETURNING asset_id
            """,
            (platform, crawl_type, json.dumps(payload), label, category,
             priority, cadence_min, is_active, notes),
        )
        return cur.fetchone()[0]


def upsert_asset(**kwargs) -> int:
    """Seperti create_asset tapi idempotent — dipakai oleh seed.py.

    Konflik pada (platform, crawl_type, payload) → update metadata,
    TIDAK menyentuh last_crawled_at / consecutive_failures (histori runtime dijaga).
    """
    dsn = kwargs.pop("dsn", None)
    payload = kwargs["payload"]
    with get_conn(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO control.crawl_assets
                (platform, crawl_type, payload, label, category,
                 priority, cadence_min, is_active, notes)
            VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (platform, crawl_type, payload) DO UPDATE SET
                label       = EXCLUDED.label,
                category    = EXCLUDED.category,
                priority    = EXCLUDED.priority,
                cadence_min = EXCLUDED.cadence_min,
                is_active   = EXCLUDED.is_active,
                notes       = EXCLUDED.notes
            RETURNING asset_id
            """,
            (
                kwargs.get("platform", "tokopedia"),
                kwargs["crawl_type"],
                json.dumps(payload),
                kwargs["label"],
                kwargs["category"],
                kwargs.get("priority", 5),
                kwargs.get("cadence_min", 60),
                kwargs.get("is_active", True),
                kwargs.get("notes"),
            ),
        )
        return cur.fetchone()[0]


def update_asset(asset_id: int, dsn: str | None = None, **fields) -> None:
    """Update kolom mana pun yang boleh diedit manusia."""
    allowed = {"label", "category", "priority", "cadence_min",
               "is_active", "notes", "payload", "crawl_type"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return

    sets, params = [], []
    for key, value in updates.items():
        if key == "payload":
            sets.append("payload = %s::jsonb")
            params.append(json.dumps(value))
        else:
            sets.append(f"{key} = %s")
            params.append(value)
    params.append(asset_id)

    with get_conn(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE control.crawl_assets SET {', '.join(sets)} WHERE asset_id = %s",
            params,
        )


def delete_asset(asset_id: int, dsn: str | None = None) -> None:
    """Hapus permanen. Untuk menghentikan crawl sementara, lebih baik is_active=false."""
    with get_conn(dsn) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM control.crawl_assets WHERE asset_id = %s", (asset_id,))


# ---------------------------------------------------------------------------
# WRITE — status runtime (dipanggil DAG)
# ---------------------------------------------------------------------------

def mark_success(asset_id: int, dsn: str | None = None) -> None:
    """Crawl berhasil: catat waktu, reset counter kegagalan."""
    with get_conn(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE control.crawl_assets
            SET last_crawled_at = now(),
                last_status = 'success',
                consecutive_failures = 0
            WHERE asset_id = %s
            """,
            (asset_id,),
        )


def mark_failure(asset_id: int, status: str = "failed", dsn: str | None = None) -> bool:
    """Crawl gagal: naikkan counter. Circuit breaker (FR-19): >= MAX → nonaktifkan.

    Returns:
        True jika asset baru saja dinonaktifkan oleh circuit breaker.
    """
    if status not in ("failed", "blocked"):
        raise ValueError("status harus 'failed' atau 'blocked'")

    with get_conn(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE control.crawl_assets
            SET last_crawled_at = now(),
                last_status = %s,
                consecutive_failures = consecutive_failures + 1,
                is_active = CASE
                    WHEN consecutive_failures + 1 >= %s THEN false
                    ELSE is_active
                END
            WHERE asset_id = %s
            RETURNING NOT is_active AND consecutive_failures >= %s
            """,
            (status, MAX_CONSECUTIVE_FAILURES, asset_id, MAX_CONSECUTIVE_FAILURES),
        )
        row = cur.fetchone()
        return bool(row and row[0])


def reactivate(asset_id: int, dsn: str | None = None) -> None:
    """Hidupkan lagi asset yang dimatikan circuit breaker (reset counter)."""
    with get_conn(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE control.crawl_assets
            SET is_active = true, consecutive_failures = 0, last_status = NULL
            WHERE asset_id = %s
            """,
            (asset_id,),
        )
