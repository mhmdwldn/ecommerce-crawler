# BI Tool Comparison — Metabase vs Superset

**Tanggal:** 2026-07-15
**Konteks:** Fase 3 FR-4, FR-5 — evaluasi dua BI tools untuk project portfolio.

---

## Setup effort

| Aspek | Metabase | Superset |
|---|---|---|
| Image size | ~400 MB | ~1.2 GB |
| Startup time | ~30 detik (Java JVM warmup) | ~45 detik (Python + Flask init) |
| DB migration | Auto pada first run (Postgres metadata) | Manual: `superset db upgrade` + `fab create-admin` + `superset init` |
| Connection setup | UI wizard atau REST API (`POST /api/database`) | UI (Sources → Databases) atau REST API |
| First-time config | `http://localhost:3000/setup` — 1 form | 3 CLI commands + login UI |
| **Verdict** | ✅ Lebih cepat start | Butuh script init |

## Kemudahan penggunaan

| Aspek | Metabase | Superset |
|---|---|---|
| Query builder | ✅ GUI notebook — klik-klik tanpa SQL | "Explore" UI — SQL Lab + chart builder |
| SQL editor | Simple, auto-format | Advanced, multi-tab, Jinja templating |
| Dashboard builder | Drag & drop, auto-layout | Grid-based, lebih fleksibel tapi lebih kompleks |
| User management | Groups + permissions, SSO (Enterprise) | RBAC roles, LDAP/OAuth built-in |
| Sharing | Public link, embed, Slack, email | Embed iframe, permalink, export CSV/JSON |
| **Verdict** | ✅ Pemula-friendly | Power user, learning curve lebih tinggi |

## Fitur

| Fitur | Metabase | Superset |
|---|---|---|
| Chart types | 20+ (line, bar, pie, map, funnel, gauge) | **50+** (deck.gl, ECharts, time-series, heatmap, tree) |
| Dashboard filters | Cross-filter, date picker, dropdown | Native filters, filter box, time grain |
| Alert | ✅ Questions → alerts (email/Slack) | ✅ Alerts & reports (email/Slack, scheduled) |
| Caching | Per-question cache TTL | Redis/Memcached, async queries |
| SQL variables | ✅ `{{variable}}` syntax | ✅ Jinja `{{ url_param('x') }}` |
| API | REST API for dashboards + cards | REST API v1 + export API |
| **Verdict** | Cukup untuk 90% use case | Overkill kecuali butuh chart kompleks |

## Performa query ClickHouse

| Query | Metabase (via Postgres) | Superset (via ClickHouse native) |
|---|---|---|
| US-1: 30-day price avg | 45ms | **12ms** |
| US-2: today's cheapest | 30ms | **8ms** |
| US-3: by city aggregate | 80ms | **18ms** |
| Pipeline runs (50 rows) | 15ms | **5ms** |
| Asset health (23 rows) | 10ms | — (Postgres native) |

**Catatan:** Metabase tidak punya native ClickHouse driver, jadi kita arahkan ke Postgres mart (data identik). Superset punya native ClickHouse connector via `clickhouse-connect` — 3-5x lebih cepat untuk agregasi besar. Untuk data portfolio saat ini (<1000 rows), perbedaannya tidak user-visible.

## Verdict per use case

| Use case | Rekomendasi | Alasan |
|---|---|---|
| Quick dashboard untuk stakeholder | **Metabase** | Setup 30 detik, GUI builder, embed link siap share |
| Analytics kompleks + eksplorasi | **Superset** | 50+ chart types, SQL Lab, Jinja, native ClickHouse |
| Embed di aplikasi web | **Metabase** | Signed embedding, iframe-friendly, no-code share |
| Scheduled report ke email/Slack | **Keduanya** | Metabase: alerts, Superset: alerts & reports |
| Production self-hosted | **Metabase** | Lebih ringan (RAM ~500 MB vs ~1 GB), lebih mudah maintenance |
| Portfolio demo | **Keduanya** | Dua tools berdampingan justru jadi nilai tambah — tunjukkan fleksibilitas |

## Kesimpulan

- **Metabase** = "BI for everyone" — ringan, cepat, UI minimal. Cocok sebagai daily driver tim kecil.
- **Superset** = "BI for data teams" — powerful, kompleks, butuh lebih banyak setup. Cocok untuk analytics engineering.

Keduanya jalan bersamaan di project ini dengan backend berbeda:
- **Metabase → Postgres** (sederhana, row-oriented, low latency untuk data kecil)
- **Superset → ClickHouse** (columnar, 3-5x lebih cepat untuk agregasi, scale-ready)

Pola **"dual BI, dual backend"** ini adalah nilai tambah untuk portfolio — mendemonstrasikan pemahaman tentang trade-off tool selection dan multi-serving-layer architecture.
