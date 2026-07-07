"""Tokopedia product-detail (PDP) controller."""

import logging

from controllers.tokopedia import TokopediaControllers

logger = logging.getLogger(__name__)


class TokopediaProductDetail(TokopediaControllers):
    """Handler that fetches a single product detail page (PDP).

    Job dict fields (also overridable via CLI):
        - ``url``: full product URL (https://www.tokopedia.com/<shop>/<key>)
        - ``product_key`` + ``shop_domain``: alternative to ``url``
        - ``output_file``: optional path to save raw JSON results
    """

    log = logging.getLogger("tokopedia.product_detail")

    async def handler(self, job: dict):
        """Fetch the product detail and pipe it to the output driver."""
        url = self.parse_job_value(job, "url")
        product_key = self.parse_job_value(job, "product_key")
        shop_domain = self.parse_job_value(job, "shop_domain")
        output_file = self.args.get("output_file") or job.get("output_file")

        self.log.info(
            "Fetching Tokopedia product detail: url=%r shop=%r key=%r",
            url, shop_domain, product_key,
        )

        await self._ensure_api()

        try:
            event = await self.api.get_product_detail(
                url=url,
                product_key=product_key,
                shop_domain=shop_domain,
            )
            if event is None:
                self.log.warning("Product not found")
                return

            doc = event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
            doc_json = event.payload.model_dump_json(by_alias=True, exclude_none=True)

            self.log.info("  [product_id=%s] %s", doc.get("id", "?"),
                          str(doc.get("name", ""))[:80])
            self.send_output(doc_json)

            if output_file:
                self.save_to_file(doc, output_file)

            self.log.info("Product detail scraped (id=%s)", doc.get("id", "?"))

        finally:
            await self._close_api()

    async def scrape_to_json(self, job: dict) -> list[dict]:
        """Scrape and return raw dicts — no output driver involved."""
        url = self.parse_job_value(job, "url")
        product_key = self.parse_job_value(job, "product_key")
        shop_domain = self.parse_job_value(job, "shop_domain")

        await self._ensure_api()
        try:
            event = await self.api.get_product_detail(
                url=url,
                product_key=product_key,
                shop_domain=shop_domain,
            )
        finally:
            await self._close_api()

        if event is None:
            return []
        return [event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)]
