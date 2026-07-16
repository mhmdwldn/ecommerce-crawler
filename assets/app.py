"""Asset Registry Admin — Streamlit CRUD untuk control.crawl_assets (PRD_50).

Jalankan:
    streamlit run assets/app.py

Env:
    CONTROL_DSN — DSN Postgres (default: host=localhost port=5433 dbname=mart ...)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running from any directory: add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone

import pandas as pd
import psycopg2
import streamlit as st

from assets.repository import (
    MAX_CONSECUTIVE_FAILURES,
    create_asset,
    delete_asset,
    get_due_assets,
    list_assets,
    reactivate,
    update_asset,
)

CRAWL_TYPES = ["search-product", "search-shop", "product-detail", "product-reviews"]
CATEGORIES = ["elektronik", "fashion", "rumah tangga", "olahraga", "lainnya"]

# payload yang dibutuhkan tiap crawl_type (harus konsisten dengan seed.py)
PAYLOAD_SPEC = {
    "search-product": [("keyword", "Keyword", "poco f8"), ("max_pages", "Max pages", 2)],
    "search-shop": [("keyword", "Keyword", "xiaomi"), ("max_pages", "Max pages", 2)],
    "product-detail": [("url", "URL produk", "https://www.tokopedia.com/...")],
    "product-reviews": [("product_id", "Product ID", "")],
}

st.set_page_config(page_title="Asset Registry", page_icon="🎯", layout="wide")


def payload_inputs(crawl_type: str, prefix: str, existing: dict | None = None) -> dict:
    """Render input payload sesuai crawl_type. Return dict payload."""
    existing = existing or {}
    payload: dict = {}
    for key, label, default in PAYLOAD_SPEC[crawl_type]:
        val = existing.get(key, default)
        if isinstance(default, int):
            payload[key] = st.number_input(
                label, min_value=1, max_value=10, value=int(val), key=f"{prefix}_{key}"
            )
        else:
            text = st.text_input(label, value=str(val), key=f"{prefix}_{key}")
            if text.strip():
                payload[key] = text.strip()
    return payload


def humanize_age(ts) -> str:
    if ts is None:
        return "belum pernah"
    delta = datetime.now(timezone.utc) - ts
    mins = int(delta.total_seconds() // 60)
    if mins < 60:
        return f"{mins}m lalu"
    if mins < 1440:
        return f"{mins // 60}j lalu"
    return f"{mins // 1440}h lalu"


# ---------------------------------------------------------------------------
# Header + metrik
# ---------------------------------------------------------------------------
st.title("🎯 Asset Registry")
st.caption("Control plane: daftar target yang di-crawl. Sumber kebenaran untuk Airflow (PRD_50).")

try:
    assets = list_assets()
    due = get_due_assets(limit=200)
except psycopg2.OperationalError as exc:
    st.error(f"Gagal konek Postgres. Cek CONTROL_DSN & `docker compose up postgres`.\n\n```\n{exc}\n```")
    st.stop()

active = [a for a in assets if a["is_active"]]
tripped = [a for a in assets if not a["is_active"] and a["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total asset", len(assets))
c2.metric("Aktif", len(active))
c3.metric("Due sekarang", len(due))
c4.metric("Circuit breaker", len(tripped), delta=None if not tripped else "perlu dicek")

if tripped:
    st.warning(
        f"{len(tripped)} asset dinonaktifkan otomatis setelah {MAX_CONSECUTIVE_FAILURES}x gagal beruntun. "
        "Cek tab **Bermasalah** — kemungkinan schema Tokopedia berubah atau target diblokir."
    )

tab_list, tab_add, tab_edit, tab_bad = st.tabs(["📋 Daftar", "➕ Tambah", "✏️ Edit / Hapus", "⚠️ Bermasalah"])

# ---------------------------------------------------------------------------
# Tab: Daftar
# ---------------------------------------------------------------------------
with tab_list:
    if not assets:
        st.info("Registry kosong. Jalankan `python -m assets.seed` atau tambah lewat tab ➕.")
    else:
        col_a, col_b = st.columns([1, 1])
        cat_filter = col_a.multiselect("Kategori", sorted({a["category"] for a in assets if a["category"]}))
        only_active = col_b.checkbox("Hanya yang aktif", value=False)

        rows = assets
        if cat_filter:
            rows = [a for a in rows if a["category"] in cat_filter]
        if only_active:
            rows = [a for a in rows if a["is_active"]]

        due_ids = {a["asset_id"] for a in due}
        df = pd.DataFrame([
            {
                "id": a["asset_id"],
                "label": a["label"],
                "kategori": a["category"],
                "tipe": a["crawl_type"],
                "payload": ", ".join(f"{k}={v}" for k, v in a["payload"].items()),
                "prio": a["priority"],
                "cadence": f'{a["cadence_min"]}m',
                "aktif": "✅" if a["is_active"] else "⛔",
                "due": "🔥" if a["asset_id"] in due_ids else "",
                "terakhir": humanize_age(a["last_crawled_at"]),
                "status": a["last_status"] or "—",
                "gagal": a["consecutive_failures"],
            }
            for a in rows
        ])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption("🔥 = layak di-crawl pada run Airflow berikutnya")

# ---------------------------------------------------------------------------
# Tab: Tambah
# ---------------------------------------------------------------------------
with tab_add:
    st.subheader("Tambah asset baru")
    ctype = st.selectbox("Crawl type", CRAWL_TYPES, key="add_type")

    left, right = st.columns(2)
    with left:
        label = st.text_input("Label", placeholder="POCO F8", key="add_label")
        category = st.selectbox("Kategori", CATEGORIES, key="add_cat")
        new_payload = payload_inputs(ctype, "add")
    with right:
        priority = st.slider("Priority (1 = tertinggi)", 1, 9, 5, key="add_prio")
        cadence = st.number_input("Cadence (menit)", min_value=15, max_value=10080, value=60, step=15, key="add_cad")
        is_active = st.checkbox("Aktif", value=True, key="add_active")
        notes = st.text_area("Catatan", key="add_notes", height=80)

    st.caption("💡 Makin volatil harganya (flagship baru), makin pendek cadence & tinggi priority.")

    if st.button("Simpan", type="primary", key="add_btn"):
        if not label.strip():
            st.error("Label wajib diisi.")
        elif not new_payload:
            st.error("Payload kosong.")
        else:
            try:
                aid = create_asset(
                    crawl_type=ctype, payload=new_payload, label=label.strip(),
                    category=category, priority=priority, cadence_min=int(cadence),
                    is_active=is_active, notes=notes.strip() or None,
                )
                st.success(f"Asset #{aid} '{label}' ditambahkan. Akan ikut ter-crawl di run Airflow berikutnya.")
                st.rerun()
            except psycopg2.errors.UniqueViolation:
                st.error("Target ini sudah ada di registry (platform + tipe + payload sama).")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Gagal: {exc}")

# ---------------------------------------------------------------------------
# Tab: Edit / Hapus
# ---------------------------------------------------------------------------
with tab_edit:
    if not assets:
        st.info("Belum ada asset.")
    else:
        opts = {f'#{a["asset_id"]} · {a["label"]} ({a["category"]})': a for a in assets}
        chosen = st.selectbox("Pilih asset", list(opts), key="edit_pick")
        a = opts[chosen]

        left, right = st.columns(2)
        with left:
            e_label = st.text_input("Label", value=a["label"] or "", key="e_label")
            e_cat = st.selectbox(
                "Kategori", CATEGORIES,
                index=CATEGORIES.index(a["category"]) if a["category"] in CATEGORIES else len(CATEGORIES) - 1,
                key="e_cat",
            )
            e_payload = payload_inputs(a["crawl_type"], "e", a["payload"])
        with right:
            e_prio = st.slider("Priority", 1, 9, int(a["priority"]), key="e_prio")
            e_cad = st.number_input("Cadence (menit)", min_value=15, max_value=10080,
                                    value=int(a["cadence_min"]), step=15, key="e_cad")
            e_active = st.checkbox("Aktif", value=a["is_active"], key="e_active")
            e_notes = st.text_area("Catatan", value=a["notes"] or "", key="e_notes", height=80)

        st.caption(
            f'Terakhir crawl: {humanize_age(a["last_crawled_at"])} · '
            f'status: {a["last_status"] or "—"} · gagal beruntun: {a["consecutive_failures"]}'
        )

        b1, b2 = st.columns([1, 1])
        if b1.button("Simpan perubahan", type="primary", key="e_save"):
            update_asset(
                a["asset_id"], label=e_label.strip(), category=e_cat, payload=e_payload,
                priority=e_prio, cadence_min=int(e_cad), is_active=e_active,
                notes=e_notes.strip() or None,
            )
            st.success("Tersimpan.")
            st.rerun()

        with b2.popover("🗑️ Hapus", use_container_width=True):
            st.write(f'Hapus permanen **{a["label"]}**?')
            st.caption("Untuk berhenti crawl sementara, lebih baik matikan 'Aktif' saja — histori tetap terjaga.")
            if st.button("Ya, hapus permanen", key="e_del"):
                delete_asset(a["asset_id"])
                st.success("Terhapus.")
                st.rerun()

# ---------------------------------------------------------------------------
# Tab: Bermasalah
# ---------------------------------------------------------------------------
with tab_bad:
    st.subheader("Asset bermasalah")
    st.caption(
        f"Asset dinonaktifkan otomatis setelah {MAX_CONSECUTIVE_FAILURES}x gagal beruntun "
        "(circuit breaker, FR-19) agar tidak terus-menerus menembak target yang memblokir."
    )
    problems = [a for a in assets if a["consecutive_failures"] > 0 or a["last_status"] in ("failed", "blocked")]

    if not problems:
        st.success("Tidak ada asset bermasalah. 🎉")
    else:
        for a in sorted(problems, key=lambda x: -x["consecutive_failures"]):
            with st.container(border=True):
                c1, c2, c3 = st.columns([3, 2, 1])
                c1.markdown(f'**{a["label"]}** · `{a["payload"]}`')
                c2.markdown(
                    f'status: `{a["last_status"] or "—"}` · gagal: **{a["consecutive_failures"]}x** · '
                    f'{"⛔ nonaktif" if not a["is_active"] else "✅ aktif"}'
                )
                if not a["is_active"] and c3.button("Aktifkan", key=f'react_{a["asset_id"]}'):
                    reactivate(a["asset_id"])
                    st.rerun()
