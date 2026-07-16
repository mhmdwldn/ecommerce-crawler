"""Tokopedia product-reviews controller."""

import logging

from controllers.tokopedia import TokopediaControllers

logger = logging.getLogger(__name__)


class TokopediaProductReviews(TokopediaControllers):
    """Handler that crawls paginated reviews of a product.

    Job dict fields (also overridable via CLI):
        - ``product_id``: numeric product ID (required)
        - ``limit``: reviews per page (default 10)
        - ``max_pages``: number of pages to crawl (default 1)
        - ``sort_by``: sort expression (default 'informative_score desc')
        - ``filter_by``: filter expression (default '')
        - ``output_file``: optional path to save raw JSON results
    """

    log = logging.getLogger("tokopedia.product_reviews")

    async def handler(self, job: dict[str, object]) -> None:
        """Crawl paginated reviews and pipe them to the output driver.

        Args:
            job: dict with keys ``product_id`` (required), ``limit``, ``max_pages``,
                ``sort_by``, ``filter_by``. CLI args take precedence.
        """
        product_id = str(self.parse_job_value(job, "product_id", ""))
        limit = int(self.parse_job_value(job, "limit", 10))
        max_pages = self.parse_job_max_pages(job)
        sort_by = self.parse_job_value(job, "sort_by", "informative_score desc")
        filter_by = self.parse_job_value(job, "filter_by", "")
        output_file = self.args.get("output_file") or job.get("output_file")

        self.log.info(
            "Crawling Tokopedia reviews: product_id=%r limit=%d max_pages=%d",
            product_id, limit, max_pages,
        )

        await self._ensure_api()

        try:
            docs: list[dict] | None = [] if output_file else None
            count = 0
            async for event in self.api.get_product_reviews(
                product_id=product_id,
                max_pages=max_pages,
                limit=limit,
                sort_by=sort_by,
                filter_by=filter_by,
            ):
                doc = event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                doc_json = event.payload.model_dump_json(by_alias=True, exclude_none=True)

                if docs is not None:
                    docs.append(doc)
                count += 1

                self.log.info("  [review_id=%s] rating=%s", doc.get("id", "?"),
                              doc.get("productRating", "?"))
                self.send_output(doc_json)

            if output_file and docs:
                self.save_to_file(docs, output_file)

            self.log.info("Crawl complete — %d reviews scraped for product %r",
                          count, product_id)

        finally:
            await self._close_api()

    async def scrape_to_json(self, job: dict[str, object]) -> list[dict[str, object]]:
        """Scrape and return raw review dicts — no output driver involved.

        Args:
            job: dict with keys ``product_id`` (required), ``limit``, ``max_pages``,
                ``sort_by``, ``filter_by``.

        Returns:
            List of raw review dicts (TokopediaReview.model_dump).
        """
        product_id = str(self.parse_job_value(job, "product_id", ""))
        limit = int(self.parse_job_value(job, "limit", 10))
        max_pages = self.parse_job_max_pages(job)
        sort_by = self.parse_job_value(job, "sort_by", "informative_score desc")
        filter_by = self.parse_job_value(job, "filter_by", "")

        docs: list[dict] = []
        await self._ensure_api()
        try:
            async for event in self.api.get_product_reviews(
                product_id=product_id,
                max_pages=max_pages,
                limit=limit,
                sort_by=sort_by,
                filter_by=filter_by,
            ):
                docs.append(
                    event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                )
        finally:
            await self._close_api()

        return docs
