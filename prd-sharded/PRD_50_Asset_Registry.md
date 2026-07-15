# PRD 50 — Asset Registry & Crawl Frontier (Control Plane)
> Versi 0.6 · Baca PRD_00 dulu. Domain baru: apa yang di-crawl, bukan bagaimana.

## Konsep
Memisahkan **control plane** (daftar target yang harus di-crawl) dari **data plane** (pipeline crawl→Kafka→lakehouse→CH). Registry = "seed store", antriannya = "frontier" dalam istilah crawler.

Sebelumnya keyword di-hardcode / via Airflow Variable → tidak scalable, tidak ada histori, tidak ada prioritas.

## Storage — Postgres + JSONB (keputusan #7)
Alasan: data terstruktur & kecil (puluhan–ratusan baris), Postgres sudah ada di stack, JSONB menampung payload heterogen per crawl_type. Mongo ditolak: menambah service tanpa kebutuhan teknis nyata.

```sql
CREATE TABLE crawl_assets (
  asset_id       BIGSERIAL PRIMARY KEY,
  platform       TEXT NOT NULL DEFAULT 'tokopedia',
  crawl_type     TEXT NOT NULL,           -- search-product | product-detail | search-shop | product-reviews
  payload        JSONB NOT NULL,          -- {"keyword":"poco f8","max_pages":2} | {"url":"..."} | {"product_id":"..."}
  priority       SMALLINT NOT NULL DEFAULT 5,   -- 1 = tertinggi
  cadence_min    INT NOT NULL DEFAULT 60,       -- crawl tiap N menit
  is_active      BOOLEAN NOT NULL DEFAULT true,
  last_crawled_at TIMESTAMPTZ,            -- UTC (keputusan #6)
  last_status    TEXT,                    -- success | failed | blocked
  consecutive_failures SMALLINT NOT NULL DEFAULT 0,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_assets_due ON crawl_assets (is_active, last_crawled_at);
```

**Aturan "due":** asset layak di-crawl jika `is_active` AND (`last_crawled_at` IS NULL OR `last_crawled_at < now() - cadence_min`).
**Circuit breaker:** `consecutive_failures >= 5` → set `is_active = false` (cegah spam request ke target yang memblokir).

## Fase 1 — Pola A: Airflow Dynamic Task Mapping (P0)
```
Airflow DAG @hourly
  └─ get_due_assets()  ── query Postgres ──▶ list[asset]
        └─ crawl.expand(asset)  ── satu mapped task per asset ──▶ Kafka
              └─ update_asset_status()  ── tulis last_crawled_at, last_status
  └─ bronze → silver → dbt → clickhouse (data plane, tidak berubah)
```
Tanpa queue. Paralelisme dibatasi `max_active_tasks` agar tidak membanjiri Tokopedia.

## Fase 2 — Pola B: Beanstalkd Frontier (P2, upgrade)
```
Airflow DAG (producer)  ──push job──▶  beanstalkd tube: crawl.tokopedia
                                              │ reserve
                                     Crawler Worker (long-running, N replica)
                                              │ publish
                                          Kafka ──▶ data plane (tidak berubah)
```
**Kenapa beanstalkd, bukan Kafka:** Kafka = event log (offset-based), tidak punya per-job ack, retry-with-delay, atau bury. Beanstalkd = work queue: `reserve`/`delete`/`release(delay)`/`bury`/priority/TTR — semantik yang tepat untuk crawl frontier dengan retry.

Konsekuensi: Airflow tidak lagi menunggu hasil crawl (fire-and-forget) → butuh cara tahu kapan batch selesai (opsi: sensor pada `pipeline_runs`, atau pisahkan DAG produce & DAG transform).

**Prasyarat memulai Fase 2 (jangan lebih cepat):** Pola A sudah stabil ≥1 minggu DAN ada alasan nyata (jumlah asset > ~100, atau butuh scale-out worker, atau butuh retry semantics yang Airflow retry tidak cukup).

## Admin UI — Streamlit CRUD (BARU, v0.6 — sebelumnya Non-Goal, direvisi atas permintaan)
Non-Goal awal "UI untuk registry" dicabut: SQL manual ternyata tidak cukup nyaman untuk operasi harian (tambah/nonaktifkan keyword). Streamlit dipilih karena ringan, Python-native (konsisten dengan stack), dan cukup untuk single-user admin tool — bukan aplikasi customer-facing.

`assets/app.py` — 4 tab:
1. **Daftar** — tabel semua asset, filter kategori/aktif, penanda 🔥 untuk yang *due* saat ini.
2. **Tambah** — form sesuai `crawl_type` (payload dinamis: keyword+max_pages / url / product_id).
3. **Edit/Hapus** — ubah priority/cadence/aktif, atau hapus permanen (dengan peringatan: nonaktifkan lebih aman daripada hapus, histori tetap terjaga).
4. **Bermasalah** — daftar asset yang kena circuit breaker atau punya `consecutive_failures>0`, plus tombol reaktivasi manual.

Berjalan di atas `assets/repository.py` yang sama dengan yang dipakai Airflow — **satu-satunya jalur tulis ke registry**, jadi tidak ada logic ganda.

## Non-Goals
- ❌ Auto-discovery asset baru dari hasil crawl (mis. produk terkait) — v2.
- ❌ MongoDB.
- ❌ Multi-user auth di Streamlit — v1 diasumsikan dipakai lokal oleh 1 operator (lo sendiri). Kalau nanti di-deploy shared, wajib tambah auth dulu.
