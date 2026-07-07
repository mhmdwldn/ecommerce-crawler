"""Shopee product-search controller."""

import logging

from controllers.shopee import ShopeeControllers

logger = logging.getLogger(__name__)


class ShopeeSearchProduct(ShopeeControllers):
    """Handler that searches Shopee products and sends results to output.

    Job dict fields (also overridable via CLI):
        - ``keyword``: search query (required unless ``match_id`` is given)
        - ``match_id``: category ID for category listing (PAGE_CATEGORY)
        - ``limit``: results per page (default from settings)
        - ``max_pages``: number of pages to crawl (default 1)
        - ``output_file``: optional path to save raw JSON results
    """

    log = logging.getLogger("shopee.search_product")

    async def handler(self, job: dict):
        """Execute the product search and pipe results to the output driver."""
        keyword = self.parse_job_keyword(job)
        match_id = str(self.parse_job_value(job, "match_id", "") or "")
        limit = self.parse_job_limit(job)
        max_pages = self.parse_job_max_pages(job)
        output_file = self.args.get("output_file") or job.get("output_file")

        self.log.info(
            "Searching Shopee products: keyword=%r match_id=%r limit=%d max_pages=%d",
            keyword, match_id, limit, max_pages,
        )

        await self._ensure_api()

        try:
            # Only accumulate the full list when we'll need to save to file
            docs: list[dict] | None = [] if output_file else None
            count = 0
            async for event in self.api.search_products(
                keyword=keyword,
                match_id=match_id,
                max_pages=max_pages,
                limit=limit,
            ):
                doc = event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                doc_json = event.payload.model_dump_json(by_alias=True, exclude_none=True)

                if docs is not None:
                    docs.append(doc)
                count += 1

                self.log.info("  [item_id=%s] %s", doc.get("itemid", doc.get("id", "?")),
                              str(doc.get("name", ""))[:80])
                self.send_output(doc_json)

            if output_file and docs:
                self.save_to_file(docs, output_file)

            query_label = keyword or f"category:{match_id}"
            self.log.info("Search complete — %d products scraped for %r", count, query_label)

        finally:
            await self._close_api()

    async def scrape_to_json(self, job: dict) -> list[dict]:
        """Scrape and return raw dicts — no output driver involved."""
        keyword = self.parse_job_keyword(job)
        match_id = str(self.parse_job_value(job, "match_id", "") or "")
        limit = self.parse_job_limit(job)
        max_pages = self.parse_job_max_pages(job)

        docs: list[dict] = []
        await self._ensure_api()
        try:
            async for event in self.api.search_products(
                keyword=keyword,
                match_id=match_id,
                max_pages=max_pages,
                limit=limit,
            ):
                docs.append(
                    event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                )
        finally:
            await self._close_api()

        return docs
