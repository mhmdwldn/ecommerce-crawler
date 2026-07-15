"""Test asset registry (PRD_50). Butuh Postgres hidup.

Jalankan:
    CONTROL_DSN="host=localhost port=5433 dbname=mart user=mart password=mart" pytest assets/tests
"""

from __future__ import annotations

import os

import psycopg2
import pytest

from assets import repository as repo
from assets.seed import build_asset

DSN = os.getenv("CONTROL_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="CONTROL_DSN tidak diset")


@pytest.fixture
def asset():
    """Asset sementara, dibersihkan setelah test."""
    aid = repo.create_asset(
        crawl_type="search-product",
        payload={"keyword": f"__test_{os.getpid()}", "max_pages": 1},
        label="Test Asset",
        category="elektronik",
        priority=5,
        cadence_min=60,
    )
    yield aid
    repo.delete_asset(aid)


# --- due logic --------------------------------------------------------------

def test_asset_baru_langsung_due(asset):
    """last_crawled_at NULL → wajib masuk antrian."""
    assert asset in {a["asset_id"] for a in repo.get_due_assets(limit=500)}


def test_mark_success_keluarkan_dari_antrian(asset):
    """Setelah di-crawl, asset tidak due sampai cadence lewat."""
    repo.mark_success(asset)
    assert asset not in {a["asset_id"] for a in repo.get_due_assets(limit=500)}
    row = repo.get_asset(asset)
    assert row["last_status"] == "success"
    assert row["consecutive_failures"] == 0


def test_due_terurut_priority():
    due = repo.get_due_assets(limit=500)
    prios = [a["priority"] for a in due]
    assert prios == sorted(prios), "antrian harus terurut priority menaik"


# --- circuit breaker (FR-19) ------------------------------------------------

def test_circuit_breaker_menonaktifkan_setelah_batas(asset):
    for _ in range(repo.MAX_CONSECUTIVE_FAILURES - 1):
        assert repo.mark_failure(asset, "failed") is False
        assert repo.get_asset(asset)["is_active"] is True

    assert repo.mark_failure(asset, "blocked") is True  # kegagalan ke-5
    row = repo.get_asset(asset)
    assert row["is_active"] is False
    assert row["consecutive_failures"] == repo.MAX_CONSECUTIVE_FAILURES


def test_asset_nonaktif_tidak_di_crawl(asset):
    for _ in range(repo.MAX_CONSECUTIVE_FAILURES):
        repo.mark_failure(asset, "blocked")
    assert asset not in {a["asset_id"] for a in repo.get_due_assets(limit=500)}


def test_sukses_mereset_counter(asset):
    repo.mark_failure(asset, "failed")
    repo.mark_failure(asset, "failed")
    repo.mark_success(asset)
    assert repo.get_asset(asset)["consecutive_failures"] == 0


def test_reactivate(asset):
    for _ in range(repo.MAX_CONSECUTIVE_FAILURES):
        repo.mark_failure(asset, "blocked")
    repo.reactivate(asset)
    row = repo.get_asset(asset)
    assert row["is_active"] is True
    assert row["consecutive_failures"] == 0


def test_mark_failure_tolak_status_invalid(asset):
    with pytest.raises(ValueError):
        repo.mark_failure(asset, "meledak")


# --- CRUD -------------------------------------------------------------------

def test_update_asset(asset):
    repo.update_asset(asset, priority=1, cadence_min=30, label="Diubah")
    row = repo.get_asset(asset)
    assert (row["priority"], row["cadence_min"], row["label"]) == (1, 30, "Diubah")


def test_update_abaikan_kolom_terlarang(asset):
    """Kolom runtime (mis. consecutive_failures) tidak boleh diubah lewat UI."""
    repo.update_asset(asset, consecutive_failures=99, last_status="success")
    assert repo.get_asset(asset)["consecutive_failures"] == 0


def test_duplikat_ditolak(asset):
    row = repo.get_asset(asset)
    with pytest.raises(psycopg2.errors.UniqueViolation):
        repo.create_asset(
            crawl_type=row["crawl_type"], payload=row["payload"],
            label="Duplikat", category="elektronik",
        )


# --- seed -------------------------------------------------------------------

def test_build_asset_pakai_defaults():
    a = build_asset(
        {"label": "POCO F8", "keyword": "poco f8", "category": "elektronik"},
        {"platform": "tokopedia", "crawl_type": "search-product", "max_pages": 2},
    )
    assert a["payload"] == {"keyword": "poco f8", "max_pages": 2}
    assert a["priority"] == 5  # default


def test_build_asset_entri_menimpa_default():
    a = build_asset(
        {"label": "iPhone 17", "keyword": "iphone 17", "category": "elektronik", "max_pages": 3, "priority": 1},
        {"platform": "tokopedia", "crawl_type": "search-product", "max_pages": 2},
    )
    assert a["payload"]["max_pages"] == 3
    assert a["priority"] == 1


def test_build_asset_tolak_payload_kosong():
    with pytest.raises(ValueError):
        build_asset({"label": "Rusak", "category": "x"},
                    {"crawl_type": "search-product"})


def test_build_asset_tolak_crawl_type_aneh():
    with pytest.raises(ValueError):
        build_asset({"label": "X", "keyword": "y"}, {"crawl_type": "ngawur"})
