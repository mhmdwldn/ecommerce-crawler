"""Tokopedia product-search controller."""

import json
import logging

from controllers.tokopedia import TokopediaControllers

logger = logging.getLogger(__name__)


class TokopediaSearchProduct(TokopediaControllers):
    """Handler that searches Tokopedia products and sends results to output.

    Job dict fields (also overridable via CLI):
        - ``keyword``: search query (required)
        - ``rows``: results per page (default from settings)
        - ``max_pages``: number of pages to crawl (default 1)
        - ``output_file``: optional path to save raw JSON results
    """

    log = logging.getLogger("tokopedia.search_product")

    async def handler(self, job: dict):
        """Execute the product search and pipe results to the output driver."""
        keyword = self.parse_job_keyword(job)
        rows = self.parse_job_rows(job)
        max_pages = self.parse_job_max_pages(job)
        output_file = self.args.get("output_file") or job.get("output_file")
        asset_category = job.get("asset_category", "")
        asset_id = job.get("asset_id", "")

        self.log.info(
            "Searching Tokopedia products: keyword=%r rows=%d max_pages=%d",
            keyword, rows, max_pages,
        )

        await self._ensure_api()

        try:
            # Only accumulate the full list when we'll need to save to file
            docs: list[dict] | None = [] if output_file else None
            count = 0
            async for event in self.api.search_products(
                keyword=keyword,
                max_pages=max_pages,
                rows=rows,
                context_metadata={"asset_category": asset_category, "asset_id": str(asset_id)},
            ):
                # Merge metadata into product dict so it flows through Kafka -> bronze -> silver
                doc = event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                if event.metadata:
                    doc["search_keyword"] = event.metadata.get("keyword", keyword)
                    doc["asset_category"] = event.metadata.get("asset_category", "")
                    doc["asset_id"] = event.metadata.get("asset_id", "")
                doc_json = json.dumps(doc, ensure_ascii=False, default=str)

                if docs is not None:
                    docs.append(doc)
                count += 1

                self.log.info("  [product_id=%s] %s", doc.get("id", "?"),
                              str(doc.get("name", ""))[:80])
                self.send_output(doc_json)

            if output_file and docs:
                self.save_to_file(docs, output_file)

            self.log.info("Search complete — %d products scraped for %r", count, keyword)

        finally:
            await self._close_api()

    async def scrape_to_json(self, job: dict) -> list[dict]:
        """Scrape and return raw dicts — no output driver involved."""
        keyword = self.parse_job_keyword(job)
        rows = self.parse_job_rows(job)
        max_pages = self.parse_job_max_pages(job)

        docs: list[dict] = []
        await self._ensure_api()
        try:
            async for event in self.api.search_products(
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
