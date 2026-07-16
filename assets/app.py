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

import subprocess as _sp
from datetime import datetime, timezone

import psycopg2
import streamlit as st

from assets.repository import (
    MAX_CONSECUTIVE_FAILURES,
    create_asset,
    delete_asset,
    get_due_assets,
    list_assets,
    mark_pending,
    reactivate,
    update_asset,
)

CRAWL_TYPES = ["search-product", "search-shop", "product-detail", "product-reviews"]
CATEGORIES = ["elektronik", "fashion", "rumah tangga", "olahraga", "lainnya"]
PAGE_SIZE = 10

PAYLOAD_SPEC = {
    "search-product": [("keyword", "Keyword", "poco f8"), ("max_pages", "Max pages", 2)],
    "search-shop": [("keyword", "Keyword", "xiaomi"), ("max_pages", "Max pages", 2)],
    "product-detail": [("url", "URL produk", "https://www.tokopedia.com/...")],
    "product-reviews": [("product_id", "Product ID", "")],
}

AIRFLOW_API = "http://localhost:8080/api/v1"
AIRFLOW_AUTH = ("admin", "admin")

st.set_page_config(page_title="Asset Registry", page_icon="🎯", layout="wide")


def trigger_dag(keyword: str, max_pages: int = 2, asset_id: int | None = None) -> tuple[bool, str]:
    """Trigger Airflow DAG for a specific keyword via REST API.

    Auth credentials fixed in compose.yaml (_AIRFLOW_WWW_USER_*).

    Args:
        keyword: Search keyword to pass as dag_run.conf.keyword
        max_pages: Max pages for this run
        asset_id: If provided, mark asset as 'pending' after successful trigger

    Returns:
        (ok, message) tuple.
    """
    url = f"{AIRFLOW_API}/dags/tokopedia_retry/dagRuns"
    try:
        resp = _sp.run(
            ["curl", "-s", "-u", "admin:admin", "-X", "POST",
             "-H", "Content-Type: application/json",
             "-d", f'{{"conf":{{"keyword":"{keyword}","max_pages":{max_pages},"asset_id":{asset_id or "null"}}}}}',
             url],
            capture_output=True, text=True, timeout=10,
        )
        if resp.returncode == 0 and "dag_run_id" in resp.stdout:
            if asset_id is not None:
                mark_pending(asset_id)
            return True, f"DAG triggered — keyword={keyword}"
        return False, f"API error: {resp.stdout[:200] or resp.stderr[:200]}"
    except _sp.TimeoutExpired:
        return False, "Airflow API timeout — cek docker ps"
    except Exception as exc:
        return False, str(exc)


# =============================================================================
# Helpers
# =============================================================================

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


def humanize_age(ts: datetime | None) -> str:
    """Render timestamp as human-readable relative time.

    Args:
        ts: UTC datetime or None.

    Returns:
        Relative time string like ``"5m lalu"``, ``"2j lalu"``, or ``"belum pernah"``.
    """
    if ts is None:
        return "belum pernah"
    delta = datetime.now(timezone.utc) - ts
    mins = int(delta.total_seconds() // 60)
    if mins < 60:
        return f"{mins}m lalu"
    if mins < 1440:
        return f"{mins // 60}j lalu"
    return f"{mins // 1440}h lalu"


def render_pagination(total: int, key: str) -> tuple[int, int]:
    """Render prev/next pagination. Returns (page, start_idx)."""
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if key not in st.session_state:
        st.session_state[key] = 1
    if st.session_state[key] > total_pages:
        st.session_state[key] = total_pages
    page = st.session_state[key]
    start = (page - 1) * PAGE_SIZE

    pc1, pc2, pc3 = st.columns([1, 2, 1])
    if pc1.button("⬅️ Prev", disabled=page == 1, key=f"{key}_prev"):
        st.session_state[key] = page - 1
        st.rerun()
    pc2.markdown(
        f'<div style="text-align:center;padding-top:4px">'
        f'Halaman <b>{page}/{total_pages}</b> '
        f'({start+1}–{min(start+PAGE_SIZE, total)} dari {total})'
        f'</div>',
        unsafe_allow_html=True,
    )
    if pc3.button("Next ➡️", disabled=page >= total_pages, key=f"{key}_next"):
        st.session_state[key] = page + 1
        st.rerun()

    return page, start


# =============================================================================
# Load data
# =============================================================================

st.title("\U0001f3af Asset Registry")
st.caption(
    "Control plane: daftar target yang di-crawl. "
    "Sumber kebenaran untuk Airflow (PRD_50)."
)

try:
    assets = list_assets()
    due = get_due_assets(limit=200)
except psycopg2.OperationalError as exc:
    st.error(
        f"Gagal konek Postgres. Cek CONTROL_DSN & `docker compose up postgres`.\n\n"
        f"```\n{exc}\n```"
    )
    st.stop()

active = [a for a in assets if a["is_active"]]
tripped = [
    a for a in assets
    if not a["is_active"] and a["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES
]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total asset", len(assets))
c2.metric("Aktif", len(active))
c3.metric("Due sekarang", len(due), help="Asset yang layak di-crawl di run Airflow berikutnya")
c4.metric("Circuit breaker", len(tripped), delta=None if not tripped else "perlu dicek")

if tripped:
    st.warning(
        f"{len(tripped)} asset dinonaktifkan otomatis setelah "
        f"{MAX_CONSECUTIVE_FAILURES}x gagal beruntun. "
        "Cek tab **Bermasalah**."
    )

tab_list, tab_add, tab_edit, tab_bad = st.tabs([
    "\U0001f4cb Daftar", "➕ Tambah", "✏️ Edit / Hapus", "⚠️ Bermasalah",
])

# =============================================================================
# Tab: Daftar
# =============================================================================
with tab_list:
    if not assets:
        st.info("Registry kosong. Jalankan `python -m assets.seed` atau tambah lewat tab ➕.")
    else:
        # --- filter bar ---
        f1, f2, f3, f4, f5 = st.columns([1, 1, 1, 1, 0.8])
        cat_filter = f1.multiselect(
            "\U0001f4c2 Kategori",
            sorted({a["category"] for a in assets if a["category"]}),
            key="filt_cat",
        )
        status_filter = f2.multiselect(
            "\U0001f4ca Status",
            sorted({a["last_status"] for a in assets if a["last_status"]}),
            key="filt_status",
        )
        type_filter = f3.multiselect(
            "⚙️ Tipe",
            sorted({a["crawl_type"] for a in assets}),
            key="filt_type",
        )
        active_filter = f4.selectbox(
            "\U0001f504 Aktif",
            ["Semua", "✅ Aktif", "⛔ Nonaktif"],
            key="filt_active",
        )
        show_failed_only = f5.checkbox(
            "❌ Failed only", key="filt_failed",
        )

        rows = assets
        if cat_filter:
            rows = [a for a in rows if a["category"] in cat_filter]
        if status_filter:
            rows = [a for a in rows if a["last_status"] in status_filter]
        if type_filter:
            rows = [a for a in rows if a["crawl_type"] in type_filter]
        if active_filter == "✅ Aktif":
            rows = [a for a in rows if a["is_active"]]
        elif active_filter == "⛔ Nonaktif":
            rows = [a for a in rows if not a["is_active"]]
        if show_failed_only:
            rows = [a for a in rows if a["last_status"] in ("failed", "blocked")]

        # pagination
        page, start = render_pagination(len(rows), "dft_page")
        page_rows = rows[start:start + PAGE_SIZE]

        due_ids = {a["asset_id"] for a in due}
        # assets eligible for batch retry on current page
        retryable = [a for a in page_rows if a["last_status"] in ("failed", "blocked") and a["is_active"]]

        # --- batch action bar ---
        if retryable:
            bc1, bc2 = st.columns([1, 4])
            all_checked = bc1.checkbox("Pilih semua", key="batch_all")
            batch_label = f"🔁 Retry {len(retryable)} selected" if all_checked else "🔁 Retry 0 selected"
            if bc2.button(batch_label, type="primary", disabled=not all_checked, key="batch_retry"):
                with st.spinner(f"Triggering {len(retryable)} retries..."):
                    ok, fail = 0, 0
                    for a in retryable:
                        kw = a["payload"].get("keyword", "")
                        mp = a["payload"].get("max_pages", 2)
                        success, _ = trigger_dag(kw, int(mp), asset_id=a["asset_id"])
                        if success:
                            ok += 1
                        else:
                            fail += 1
                    if fail:
                        st.warning(f"{ok} triggered, {fail} failed")
                    else:
                        st.success(f"{ok} retries triggered")
                    st.rerun()

        # header
        hdr = st.columns([0.4, 2, 0.8, 0.6, 2.5, 0.4, 0.5, 0.3, 0.7, 0.4, 0.5, 0.3, 0.3])
        for i, h in enumerate([
            "ID", "Label", "Kategori", "Tipe", "Payload", "Prio",
            "Cadence", "Aktif", "Terakhir", "Due", "Status", "✏️", "🔁",
        ]):
            hdr[i].markdown(f"**{h}**")

        # rows
        for a in page_rows:
            row_cols = st.columns([0.4, 2, 0.8, 0.6, 2.5, 0.4, 0.5, 0.3, 0.7, 0.4, 0.5, 0.3, 0.3])
            is_retryable = a["last_status"] in ("failed", "blocked") and a["is_active"]
            row_cols[0].markdown(f"`{a['asset_id']}`")
            row_cols[1].write(str(a["label"]))
            row_cols[2].write(str(a["category"]))
            row_cols[3].write(str(a["crawl_type"]))
            payload_text = ", ".join(f"{k}={v}" for k, v in a["payload"].items())
            row_cols[4].markdown(f"<small>{payload_text}</small>", unsafe_allow_html=True)
            row_cols[5].write(str(a["priority"]))
            row_cols[6].write(f'{a["cadence_min"]}m')
            row_cols[7].write("✅" if a["is_active"] else "⛔")
            row_cols[8].write(humanize_age(a["last_crawled_at"]))
            row_cols[9].write("\U0001f525" if a["asset_id"] in due_ids else "")
            row_cols[10].write(a["last_status"] or "—")
            if row_cols[11].button("✏️", key=f"edit_btn_{a['asset_id']}"):
                st.session_state["edit_target"] = int(a["asset_id"])
                st.rerun()
            # Retry button — only for failed/blocked assets that are still active
            if is_retryable:
                kw = a["payload"].get("keyword", "")
                mp = a["payload"].get("max_pages", 2)
                if row_cols[12].button("\U0001f501", key=f"retry_{a['asset_id']}", help=f"Retry: {kw}"):
                    ok, msg = trigger_dag(kw, int(mp), asset_id=a["asset_id"])
                    if ok:
                        st.toast(f"\U0001f514 {msg}", icon="✅")
                        st.rerun()
                    else:
                        st.error(msg)
            else:
                row_cols[12].write("")

        st.caption(
            "\U0001f525 = layak di-crawl pada run Airflow berikutnya (cadence sudah lewat). "
            "✏️ = edit asset."
        )

        # due explanation
        if len(due) == 0:
            st.info(
                "Tidak ada asset yang **due** saat ini. Ini normal — artinya semua asset "
                "sudah di-crawl baru-baru ini dan belum lewat `cadence_min` masing-masing. "
                "Asset akan otomatis muncul di sini setelah cukup waktu berlalu (misal: "
                "cadence 60 menit = muncul lagi 1 jam setelah crawl terakhir).\n\n"
                "**Cara forcing:** set `cadence_min` kecil (15m) di tab Edit, "
                "atau trigger DAG manual lewat Airflow."
            )

# =============================================================================
# Tab: Tambah
# =============================================================================
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
        cadence = st.number_input(
            "Cadence (menit)", min_value=15, max_value=10080, value=60, step=15, key="add_cad",
        )
        is_active = st.checkbox("Aktif", value=True, key="add_active")
        notes = st.text_area("Catatan", key="add_notes", height=80)

    st.caption(
        "\U0001f4a1 Makin volatil harganya (flagship baru), "
        "makin pendek cadence & tinggi priority."
    )

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
                st.success(f"Asset #{aid} '{label}' ditambahkan.")
                st.rerun()
            except psycopg2.errors.UniqueViolation:
                st.error("Target ini sudah ada di registry.")
            except Exception as exc:
                st.error(f"Gagal: {exc}")

# =============================================================================
# Tab: Edit / Hapus
# =============================================================================
with tab_edit:
    if not assets:
        st.info("Belum ada asset.")
    else:
        # Pre-select from Daftar tab edit button
        pre_select = st.session_state.get("edit_target")
        asset_map = {a["asset_id"]: a for a in assets}
        default_idx = 0
        label_map = {}
        for i, a in enumerate(assets):
            key = f"#{a['asset_id']} · {a['label']} ({a['category']})"
            label_map[key] = a
            if pre_select and a["asset_id"] == pre_select:
                default_idx = i
        chosen_label = st.selectbox("Pilih asset", list(label_map), index=default_idx, key="edit_pick")
        a = label_map[chosen_label]

        left, right = st.columns(2)
        with left:
            e_label = st.text_input("Label", value=a["label"] or "", key="e_label")
            e_cat = st.selectbox(
                "Kategori", CATEGORIES,
                index=CATEGORIES.index(a["category"])
                if a["category"] in CATEGORIES else len(CATEGORIES) - 1,
                key="e_cat",
            )
            e_payload = payload_inputs(a["crawl_type"], "e", a["payload"])
        with right:
            e_prio = st.slider("Priority", 1, 9, int(a["priority"]), key="e_prio")
            e_cad = st.number_input(
                "Cadence (menit)", min_value=15, max_value=10080,
                value=int(a["cadence_min"]), step=15, key="e_cad",
            )
            e_active = st.checkbox("Aktif", value=a["is_active"], key="e_active")
            e_notes = st.text_area("Catatan", value=a["notes"] or "", key="e_notes", height=80)

        st.caption(
            f'Terakhir crawl: {humanize_age(a["last_crawled_at"])} · '
            f'status: {a["last_status"] or "—"} · '
            f'gagal beruntun: {a["consecutive_failures"]}'
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

        with b2.popover("\U0001f5d1️ Hapus", use_container_width=True):
            st.write(f'Hapus permanen **{a["label"]}**?')
            st.caption(
                "Untuk berhenti crawl sementara, lebih baik matikan 'Aktif' saja "
                "— histori tetap terjaga."
            )
            if st.button("Ya, hapus permanen", key="e_del"):
                delete_asset(a["asset_id"])
                st.success("Terhapus.")
                st.rerun()

# =============================================================================
# Tab: Bermasalah
# =============================================================================
with tab_bad:
    st.subheader("Asset bermasalah")
    st.caption(
        f"Asset dinonaktifkan otomatis setelah {MAX_CONSECUTIVE_FAILURES}x gagal beruntun "
        "(circuit breaker, FR-19) agar tidak terus-menerus menembak target yang memblokir."
    )
    problems = [
        a for a in assets
        if a["consecutive_failures"] > 0 or a["last_status"] in ("failed", "blocked")
    ]

    if not problems:
        st.success("Tidak ada asset bermasalah. \U0001f389")
    else:
        # bulk retry: all active + failed
        bulk_candidates = [
            a for a in problems
            if a["is_active"] and a["last_status"] in ("failed", "blocked")
        ]
        if bulk_candidates:
            if st.button(
                f"🔁 Retry Semua ({len(bulk_candidates)} asset)",
                type="primary", key="retry_all_bad",
            ):
                with st.spinner(f"Triggering {len(bulk_candidates)} retries..."):
                    ok, fail = 0, 0
                    for a in bulk_candidates:
                        kw = a["payload"].get("keyword", "")
                        mp = a["payload"].get("max_pages", 2)
                        success, _ = trigger_dag(kw, int(mp), asset_id=a["asset_id"])
                        if success:
                            ok += 1
                        else:
                            fail += 1
                    if fail:
                        st.warning(f"{ok} triggered, {fail} failed — cek Airflow")
                    else:
                        st.success(f"{ok} retries triggered")
                    st.rerun()

        for a in sorted(problems, key=lambda x: -x["consecutive_failures"]):
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([2.5, 2, 0.8, 0.7])
                c1.markdown(f'**{a["label"]}** · `{a["payload"]}`')
                c2.markdown(
                    f'status: `{a["last_status"] or "—"}` · '
                    f'gagal: **{a["consecutive_failures"]}x** · '
                    f'{"⛔ nonaktif" if not a["is_active"] else "✅ aktif"}'
                )
                if not a["is_active"] and c3.button(
                    "Aktifkan", key=f'react_{a["asset_id"]}'
                ):
                    reactivate(a["asset_id"])
                    st.rerun()
                if a["is_active"] and a["last_status"] in ("failed", "blocked"):
                    kw = a["payload"].get("keyword", "")
                    mp = a["payload"].get("max_pages", 2)
                    if c4.button("\U0001f501 Retry", key=f"retry_bad_{a['asset_id']}"):
                        ok, msg = trigger_dag(kw, int(mp), asset_id=a["asset_id"])
                        if ok:
                            st.toast(f"\U0001f514 {msg}", icon="✅")
                        else:
                            st.error(msg)
