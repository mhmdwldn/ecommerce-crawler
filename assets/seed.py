"""Seed asset registry dari YAML → Postgres. Idempotent (aman dijalankan berulang).

Usage:
    python -m assets.seed                       # pakai assets/seeds/targets.yaml
    python -m assets.seed --file other.yaml
    python -m assets.seed --dry-run             # tampilkan saja, jangan tulis DB
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import yaml

from assets.repository import upsert_asset

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("seed")

DEFAULT_FILE = Path(__file__).parent / "seeds" / "targets.yaml"

# field yang jadi payload JSONB, per crawl_type
PAYLOAD_FIELDS = {
    "search-product": ("keyword", "max_pages"),
    "search-shop": ("keyword", "max_pages"),
    "product-detail": ("url",),
    "product-reviews": ("product_id",),
}


def build_asset(raw: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    """Gabungkan satu entri YAML dengan defaults → argumen upsert_asset()."""
    merged = {**defaults, **raw}
    crawl_type = merged["crawl_type"]

    fields = PAYLOAD_FIELDS.get(crawl_type)
    if fields is None:
        raise ValueError(f"crawl_type tidak dikenal: {crawl_type}")

    payload = {f: merged[f] for f in fields if merged.get(f) is not None}
    if not payload:
        raise ValueError(f"payload kosong untuk '{merged.get('label')}' — butuh salah satu dari {fields}")

    return {
        "platform": merged.get("platform", "tokopedia"),
        "crawl_type": crawl_type,
        "payload": payload,
        "label": merged.get("label") or str(next(iter(payload.values()))),
        "category": merged.get("category", "uncategorized"),
        "priority": int(merged.get("priority", 5)),
        "cadence_min": int(merged.get("cadence_min", 60)),
        "is_active": bool(merged.get("is_active", True)),
        "notes": merged.get("notes"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", type=Path, default=DEFAULT_FILE)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    doc = yaml.safe_load(args.file.read_text())
    defaults = doc.get("defaults", {})
    entries = doc.get("assets", [])

    if not entries:
        log.error("Tidak ada asset di %s", args.file)
        return 1

    ok = failed = 0
    for raw in entries:
        try:
            asset = build_asset(raw, defaults)
        except ValueError as exc:
            log.error("SKIP %s — %s", raw.get("label", "?"), exc)
            failed += 1
            continue

        if args.dry_run:
            log.info("[dry-run] %-28s %-12s %s", asset["label"], asset["category"], asset["payload"])
            ok += 1
            continue

        try:
            asset_id = upsert_asset(**asset)
            log.info("upserted #%-4s %-28s %s", asset_id, asset["label"], asset["payload"])
            ok += 1
        except Exception as exc:  # noqa: BLE001
            log.error("GAGAL %s — %s", asset["label"], exc)
            failed += 1

    log.info("Selesai: %d ok, %d gagal", ok, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
