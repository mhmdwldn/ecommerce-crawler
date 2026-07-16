"""Crawl all due assets from the asset registry (Postgres control.crawl_assets).

Called by Airflow DAG as the first task instead of a fixed-keyword crawl.
Reads due assets, crawls each sequentially, reports results back to the registry.
"""

import os
import shlex
import subprocess
import sys
from pathlib import Path


def main() -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "assets"))
    from repository import get_due_assets, mark_failure, mark_success

    repo = os.getenv("REPO", "/opt/airflow/repo")
    keyword_fallback = os.getenv("CRAWL_KEYWORD", "poco f8")
    max_pages = int(os.getenv("CRAWL_MAX_PAGES", "2"))
    kafka_topic = os.getenv("KAFKA_TOPIC", "tokopedia.products.raw")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

    assets = get_due_assets(limit=50)

    if not assets:
        # Fallback: crawl default keyword so pipeline doesn't run dry
        print("No due assets — falling back to default keyword")
        cmd_args = [
            "cd", f"{repo}/source", "&&", "python", "main.py", "crawler",
            "--mode", "full", "--type", "search-product",
            "--keyword", keyword_fallback, "--max-pages", str(max_pages),
            "-d", "kafka", "-o", kafka_topic, "--bootstrap-servers", kafka_bootstrap,
        ]
        subprocess.run(" ".join(shlex.quote(str(a)) for a in cmd_args),
                        shell=True, executable="/bin/bash", check=True)
        return

    print(f"Crawling {len(assets)} due assets")
    failed_ids = []

    for asset in assets:
        asset_id = asset["asset_id"]
        payload = asset["payload"]
        label = asset.get("label", asset_id)
        crawl_type = asset.get("crawl_type", "search-product")
        asset_category = asset.get("category", "") or ""
        print(f"  [{asset_id}] {label} ({crawl_type})")

        # Build keyword from payload
        keyword = payload.get("keyword", keyword_fallback)

        # ponytail: pass registry context as CLI args so it lands in Kafka event metadata
        cmd_args = [
            "cd", f"{repo}/source", "&&", "python", "main.py", "crawler",
            "--mode", "full", "--type", crawl_type,
            "--keyword", keyword, "--max-pages", str(max_pages),
            "--asset-category", asset_category, "--asset-id", str(asset_id),
            "-d", "kafka", "-o", kafka_topic, "--bootstrap-servers", kafka_bootstrap,
        ]
        cmd = " ".join(shlex.quote(str(a)) for a in cmd_args)
        result = subprocess.run(cmd, shell=True, executable="/bin/bash",
                                capture_output=True, text=True)

        if result.returncode == 0:
            mark_success(asset_id)
            print("    OK")
        else:
            was_disabled = mark_failure(asset_id)
            tag = " DISABLED (circuit breaker)" if was_disabled else ""
            print(f"    FAIL{tag}")
            failed_ids.append(asset_id)

    if failed_ids:
        print(f"\n{len(failed_ids)} assets failed: {failed_ids}")
        # Don't fail the task — let the pipeline continue with partial data.
        # Quality checks + audit will catch the impact.


if __name__ == "__main__":
    main()
