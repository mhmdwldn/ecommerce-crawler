"""Shopee controllers — shared base for all Shopee crawler handlers."""

import json
import logging
import os
from typing import Any

from controllers import Controllers
from library.shopee_api import ShopeeAPI


class ShopeeControllers(Controllers):
    """Shared base for Shopee crawler controllers.

    Sets up the ShopeeAPI client (its own ``SHOPEE_*`` settings namespace)
    and provides helper methods for parsing job parameters (CLI args take
    precedence over job fields) and saving intermediate results.
    """

    log = logging.getLogger("shopee.controller")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Load the Shopee settings singleton from library
        from library.config import shopee_settings

        self.settings = shopee_settings
        self.api: ShopeeAPI | None = None

    async def _ensure_api(self):
        """Lazily initialise the ShopeeAPI client."""
        if self.api is None:
            cookies = self.args.get("cookies") or self.settings.cookies or None
            self.api = ShopeeAPI(self.settings, cookies=cookies)
            await self.api.start()

    async def _close_api(self):
        """Tear down the ShopeeAPI client."""
        if self.api is not None:
            await self.api.stop()
            self.api = None

    # ------------------------------------------------------------------
    # Job helpers
    # ------------------------------------------------------------------

    def parse_job_value(self, job: dict, key: str, default: Any = None) -> Any:
        """Resolve *key* with precedence: CLI args > job dict > default."""
        if self.args.get(key) not in (None, ""):
            return self.args[key]
        return job.get(key, default)

    def parse_job_keyword(self, job: dict, default: str = "") -> str:
        """Extract the search keyword from job dict or CLI args."""
        keyword = self.parse_job_value(job, "keyword", default)
        return str(keyword).strip('"').strip("'")

    def parse_job_limit(self, job: dict, default: int | None = None) -> int:
        """Extract results-per-page from job dict or CLI args."""
        default = default or self.settings.default_limit
        return int(self.parse_job_value(job, "limit", default))

    def parse_job_max_pages(self, job: dict, default: int = 1) -> int:
        """Extract max_pages from job dict or CLI args."""
        return int(self.parse_job_value(job, "max_pages", default))

    def save_to_file(self, data, path: str):
        """Save JSON-serialisable *data* to a local file."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        self.log.info("Saved to %s", path)
