# ============================================================
# TEMPEL BAGIAN DI BAWAH INI KE AKHIR CLAUDE.md YANG SUDAH ADA.
# Jangan replace file — ini section tambahan.
# ============================================================

## Control plane: Asset Registry (module `assets/`)

Selain crawler engine (`source/`) di atas, repo ini punya **control plane** terpisah:
daftar target crawl (keyword/URL/product_id) yang harus dijalankan, disimpan di Postgres,
dikelola lewat Streamlit admin UI. Dokumen desain lengkap: sharded PRD di `docs/prd/` —
baca `PRD_50_Asset_Registry.md` sebelum menyentuh modul ini.

```
assets/
├── ddl/crawl_assets.sql   # schema Postgres (schema `control`)
├── seeds/targets.yaml     # daftar target awal, versioned — sumber kebenaran seed
├── seed.py                # upsert YAML → Postgres, idempotent (python -m assets.seed)
├── repository.py          # SATU-SATUNYA pintu tulis/baca ke control.crawl_assets
├── app.py                 # Streamlit admin CRUD (streamlit run assets/app.py)
└── tests/test_asset_registry.py
```

**Aturan keras:** semua akses ke tabel `control.crawl_assets` — dari DAG, dari script mana
pun — WAJIB lewat `assets/repository.py`. Jangan raw SQL di tempat lain. Ini mencegah logic
due/circuit-breaker punya dua sumber kebenaran.

**Kenapa terpisah dari `source/`:** `source/` = *bagaimana* cara crawl (engine, dipertahankan
Open/Closed seperti didokumentasikan di atas). `assets/` = *apa* yang di-crawl (data operasional,
berubah tiap hari tanpa deploy kode). Analogi: `source/` itu mesinnya, `assets/` itu daftar
tujuannya.

**Config:** ikut pola project ini — `pydantic-settings`, prefix `TOKOPEDIA_`, delimiter `__`.
```
TOKOPEDIA_CONTROL__DSN=host=localhost port=5433 dbname=mart user=mart password=mart
```
Saat ini `assets/repository.py` masih baca lewat `os.getenv` langsung (lihat `get_dsn()`
di file itu) sebagai jalan cepat. **TODO housekeeping:** pindahkan ke
`library/config.py` sebagai `ControlPlaneSettings` resmi (nested di settings tree yang
sudah ada), supaya satu mekanisme config untuk seluruh repo — bukan dua.

**Bootstrap:** belum diintegrasikan ke `library/setup_infra.py`. Untuk konsistensi dengan
pola bootstrap Kafka topic/ES index yang sudah ada, `assets/ddl/crawl_assets.sql` sebaiknya
dieksekusi dari situ juga (tambah satu fungsi `setup_control_plane_table()`), bukan
`psql -f` manual selamanya. Belum dikerjakan — lihat TASKS.md fase 2.5.

**Cara jalanin:**
```bash
psql <DSN> -f assets/ddl/crawl_assets.sql   # sekali di awal / setelah ubah schema
python -m assets.seed                       # sinkronkan targets.yaml → Postgres, aman diulang
streamlit run assets/app.py                 # UI CRUD (tambah/nonaktifkan keyword)
```

**Belum tersambung ke DAG.** `assets/repository.py:get_due_assets()` sudah siap dipakai
sebagai input `crawl.expand()` (Airflow dynamic task mapping), tapi
`pipeline/airflow/dags/tokopedia_products_dag.py` belum di-refactor untuk memanggilnya —
saat ini DAG masih pakai sumber keyword lama (Variable/hardcode). Lihat TASKS.md task 2.5.4.
