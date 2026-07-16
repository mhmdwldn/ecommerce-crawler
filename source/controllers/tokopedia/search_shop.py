"""Tokopedia shop-search controller."""

import logging

from controllers.tokopedia import TokopediaControllers

logger = logging.getLogger(__name__)


class TokopediaSearchShop(TokopediaControllers):
    """Handler that searches Tokopedia shops and sends results to output.

    Job dict fields (also overridable via CLI):
        - ``keyword``: search query (required)
        - ``rows``: results per page (default from settings)
        - ``max_pages``: number of pages to crawl (default 1)
        - ``output_file``: optional path to save raw JSON results
    """

    log = logging.getLogger("tokopedia.search_shop")

    async def handler(self, job: dict[str, object]) -> None:
        """Execute the shop search and pipe results to the output driver.

        Args:
            job: dict with keys ``keyword`` (required), ``rows``, ``max_pages``.
                CLI args take precedence over job values.
        """
        keyword = self.parse_job_keyword(job)
        rows = self.parse_job_rows(job)
        max_pages = self.parse_job_max_pages(job)
        output_file = self.args.get("output_file") or job.get("output_file")

        self.log.info(
            "Searching Tokopedia shops: keyword=%r rows=%d max_pages=%d",
            keyword, rows, max_pages,
        )

        await self._ensure_api()

        try:
            docs: list[dict] | None = [] if output_file else None
            count = 0
            async for event in self.api.search_shops(
                keyword=keyword,
                max_pages=max_pages,
                rows=rows,
            ):
                doc = event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                doc_json = event.payload.model_dump_json(by_alias=True, exclude_none=True)

                if docs is not None:
                    docs.append(doc)
                count += 1

                self.log.info("  [shop_id=%s] %s", doc.get("shop_id", "?"),
                              str(doc.get("shop_name", ""))[:80])
                self.send_output(doc_json)

            if output_file and docs:
                self.save_to_file(docs, output_file)

            self.log.info("Search complete — %d shops scraped for %r", count, keyword)

        finally:
            await self._close_api()

    async def scrape_to_json(self, job: dict[str, object]) -> list[dict[str, object]]:
        """Scrape and return raw shop dicts — no output driver involved.

        Args:
            job: dict with keys ``keyword`` (required), ``rows``, ``max_pages``.

        Returns:
            List of raw shop dicts (TokopediaShop.model_dump).
        """
        keyword = self.parse_job_keyword(job)
        rows = self.parse_job_rows(job)
        max_pages = self.parse_job_max_pages(job)

        docs: list[dict] = []
        await self._ensure_api()
        try:
            async for event in self.api.search_shops(
                keyword=keyword,
                max_pages=max_pages,
                rows=rows,
            ):
                docs.append(
                    event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                )
        finally:
            await self._close_api()

        return docs
