"""Tokopedia controllers — shared base for all Tokopedia crawler handlers."""

import json
import logging
import os
from typing import Any

from controllers import Controllers
from library.tokopedia_api import TokopediaAPI


class TokopediaControllers(Controllers):
    """Shared base for Tokopedia crawler controllers.

    Sets up the TokopediaAPI client and provides helper methods for
    parsing job parameters (CLI args take precedence over job fields)
    and saving intermediate results.
    """

    log = logging.getLogger("tokopedia.controller")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Load settings from library
        from library.config import settings

        self.settings = settings
        self.api: TokopediaAPI | None = None

    async def _ensure_api(self):
        """Lazily initialise the TokopediaAPI client."""
        if self.api is None:
            cookies = self.args.get("cookies") or self.settings.crawler.cookies or None
            self.api = TokopediaAPI(self.settings.crawler, cookies=cookies)
            await self.api.start()

    async def _close_api(self):
        """Tear down the TokopediaAPI client."""
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

    def parse_job_rows(self, job: dict, default: int | None = None) -> int:
        """Extract results-per-page from job dict or CLI args."""
        default = default or self.settings.crawler.default_rows
        return int(self.parse_job_value(job, "rows", default))

    def parse_job_max_pages(self, job: dict, default: int = 1) -> int:
        """Extract max_pages from job dict or CLI args."""
        return int(self.parse_job_value(job, "max_pages", default))

    def save_to_file(self, data, path: str):
        """Save JSON-serialisable *data* to a local file."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        self.log.info("Saved to %s", path)
