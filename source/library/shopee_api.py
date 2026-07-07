"""
Shopee API client for the public v4 search REST API (shopee.co.id).

Provides an async method to search products. Used by controllers as the
HTTP data-access layer. Handles rate limiting, retries, response parsing,
and Shopee's anti-bot quirks (session cookies + rotating header tokens).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncIterator, Optional

import httpx

from exception.exception import ErrorRequestException, RateLimitExceeded
from library.config import ShopeeCrawlerSettings
from library.schemas import (
    KafkaEvent,
    ShopeeProduct,
    ShopeeSearchProductRequest,
)

logger = logging.getLogger(__name__)


class ShopeeAPI:
    """Async HTTP client for the Shopee v4 search API.

    Example::

        api = ShopeeAPI(shopee_settings)
        async with api:
            async for event in api.search_products("sepatu lari pria"):
                print(event.payload.name)
    """

    def __init__(
        self, settings: ShopeeCrawlerSettings, cookies: str | None = None
    ) -> None:
        self._settings = settings
        self._cookies_override = cookies
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_delay: float = (
            1.0 / settings.rate_limit_rps if settings.rate_limit_rps > 0 else 0.0
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Create the async HTTP client."""
        cookie_source = self._cookies_override or self._settings.cookies
        cookies = None
        if cookie_source:
            cookies = dict(
                pair.split("=", 1)
                for pair in cookie_source.split("; ")
                if "=" in pair
            )

        self._client = httpx.AsyncClient(
            base_url=self._settings.base_url,
            timeout=httpx.Timeout(self._settings.request_timeout),
            headers=self._default_headers(cookies),
            cookies=cookies,
            follow_redirects=True,
            proxy=self._settings.proxy_url,
        )
        logger.info(
            "ShopeeAPI client created (base_url=%s, cookies=%s)",
            self._settings.base_url, "yes" if cookies else "no",
        )

    async def stop(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("ShopeeAPI client stopped")

    async def __aenter__(self) -> ShopeeAPI:
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # Public API — product search
    # ------------------------------------------------------------------

    async def search_products(
        self,
        keyword: str = "",
        max_pages: int = 1,
        limit: int | None = None,
        page: int = 1,
        match_id: str = "",
        scenario: str | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[KafkaEvent]:
        """Paginate through product search results.

        Pass *keyword* for a keyword search, or *match_id* (with
        ``scenario="PAGE_CATEGORY"``) for a category listing.

        Yields:
            :class:`KafkaEvent` with a :class:`ShopeeProduct` payload.
        """
        limit = limit or self._settings.default_limit
        request = ShopeeSearchProductRequest(
            keyword=keyword,
            page=page,
            limit=limit,
            by=kwargs.get("by", "relevancy"),
            order=kwargs.get("order", "desc"),
            match_id=match_id,
            scenario=scenario or ("PAGE_CATEGORY" if match_id else "PAGE_GLOBAL_SEARCH"),
        )

        query_label = keyword or f"category:{match_id}"
        for _ in range(max_pages):
            data = await self._execute(request.to_params())
            items = data.get("items") or []

            if not items:
                logger.info("No more products for %s page=%d", query_label, request.page)
                break

            for raw in items:
                product = self._parse_item(raw)
                if product is None:
                    continue
                yield self._to_event(
                    product,
                    metadata={"query": query_label, "page": request.page},
                )

            # Shopee echoes "nomore" when the result set is exhausted.
            if data.get("nomore"):
                break
            request = request.model_copy(update={"page": request.page + 1})

    # ------------------------------------------------------------------
    # Internal — HTTP plumbing
    # ------------------------------------------------------------------

    async def _execute(self, params: dict[str, str]) -> dict[str, Any]:
        """GET the search endpoint with retries; return the parsed JSON body."""
        assert self._client is not None, "HTTP client not initialised — call start()"

        max_retries = max(self._settings.max_retries, 1)
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                await self._throttle()
                resp = await self._client.get(
                    self._settings.search_endpoint, params=params
                )
                if resp.status_code == 429:
                    raise RateLimitExceeded("Too Many Requests on search_items")
                resp.raise_for_status()
                body = resp.json()
                return self._unwrap(body)
            except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError) as exc:
                last_exc = exc
                wait = self._settings.retry_backoff ** attempt
                logger.warning(
                    "Search attempt %d/%d failed (%s). Retrying in %.1fs ...",
                    attempt, max_retries, exc, wait,
                )
                await asyncio.sleep(wait)

        logger.error("All %d search attempts failed.", max_retries)
        raise last_exc  # type: ignore[misc]

    @staticmethod
    def _unwrap(body: Any) -> dict[str, Any]:
        """Validate the search envelope and return it.

        Raises:
            ErrorRequestException: when Shopee signals a non-zero error code
                or returns a non-JSON-object body (anti-bot HTML interstitial).
        """
        if not isinstance(body, dict):
            raise ErrorRequestException(
                f"Unexpected Shopee response shape: {type(body).__name__} "
                "(likely an anti-bot block — refresh cookies/tokens)"
            )
        error = body.get("error")
        if error:
            raise ErrorRequestException(
                f"Shopee API error code={error}: {body.get('error_msg') or ''}".strip()
            )
        return body

    @staticmethod
    def _parse_item(raw: dict[str, Any]) -> Optional[ShopeeProduct]:
        """Parse one search row (``{item_basic: {...}}``) into a ShopeeProduct."""
        basic = raw.get("item_basic") or raw
        if not isinstance(basic, dict) or not basic.get("itemid"):
            return None
        product = ShopeeProduct.model_validate(basic)
        if product.shop_id and product.id:
            slug = (product.name or "product").replace(" ", "-")
            product.url = f"https://shopee.co.id/{slug}-i.{product.shop_id}.{product.id}"
        return product

    async def _throttle(self) -> None:
        """Enforce the configured requests-per-second rate limit."""
        if self._rate_delay > 0:
            await asyncio.sleep(self._rate_delay)

    def _default_headers(self, cookies: dict[str, str] | None) -> dict[str, str]:
        """Build default HTTP headers matching captured browser traffic.

        The CSRF token is echoed from the ``csrftoken`` cookie when present;
        rotating anti-bot tokens (``x-sap-sec`` etc.) come from
        ``settings.extra_headers``.
        """
        s = self._settings
        headers = {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
            "Content-Type": "application/json",
            "Referer": f"{s.base_url}/search",
            "User-Agent": s.user_agent,
            "X-Api-Source": s.api_source,
            "X-Requested-With": "XMLHttpRequest",
            "X-Shopee-Language": s.language,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        if cookies and cookies.get("csrftoken"):
            headers["X-Csrftoken"] = cookies["csrftoken"]
        headers.update(s.extra_headers)
        return headers

    @staticmethod
    def _to_event(
        product: ShopeeProduct, metadata: Optional[dict[str, Any]] = None
    ) -> KafkaEvent:
        """Wrap a parsed product in a KafkaEvent envelope."""
        return KafkaEvent(
            event_type="shopee.product.scraped",
            source="shopee-crawler",
            payload=product,
            metadata=metadata or {},
        )
