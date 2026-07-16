"""
Tokopedia API client for the gql.tokopedia.com GraphQL gateway.

Provides async methods for the four supported operations:
  - search_products    (SearchProductV5Query)
  - search_shops       (AceSearchShopQuery)
  - get_product_detail (PDPMainInfo)
  - get_product_reviews(productReviewList)

Used by controllers as the HTTP data-access layer. Handles rate limiting,
retries, GraphQL error unwrapping, and response parsing into Pydantic models.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncIterator, Optional

import httpx

from exception.exception import ErrorRequestException, RateLimitExceeded
from library import graphql_queries
from library.config import TokopediaCrawlerSettings
from library.schemas import (
    EventType,
    GraphQLRequest,
    KafkaEvent,
    TokopediaDocument,
    TokopediaProduct,
    TokopediaProductDetail,
    TokopediaProductDetailRequest,
    TokopediaProductReviewsRequest,
    TokopediaReview,
    TokopediaSearchProductRequest,
    TokopediaSearchShopRequest,
    TokopediaShop,
)

logger = logging.getLogger(__name__)


class TokopediaAPI:
    """Async HTTP client for the Tokopedia GraphQL gateway.

    Example::

        api = TokopediaAPI(settings.crawler)
        async with api:
            async for event in api.search_products("poco f8"):
                print(event.payload.name)
    """

    def __init__(
        self, settings: TokopediaCrawlerSettings, cookies: str | None = None
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
            headers=self._default_headers(),
            cookies=cookies,
            follow_redirects=True,
            proxy=self._settings.proxy_url,
        )
        logger.info(
            "TokopediaAPI client created (base_url=%s, cookies=%s)",
            self._settings.base_url, "yes" if cookies else "no",
        )

    async def stop(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("TokopediaAPI client stopped")

    async def __aenter__(self) -> TokopediaAPI:
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # Public API — product search
    # ------------------------------------------------------------------

    async def search_products(
        self,
        keyword: str,
        max_pages: int = 1,
        rows: int | None = None,
        page: int = 1,
        context_metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[KafkaEvent]:
        """Paginate through product search results for *keyword*.

        Args:
            context_metadata: Optional dict merged into each event's metadata
                (e.g. asset_category, asset_id from the registry).

        Yields:
            :class:`KafkaEvent` with a :class:`TokopediaProduct` payload.
        """
        rows = rows or self._settings.default_rows
        request = TokopediaSearchProductRequest(
            keyword=keyword,
            page=page,
            rows=rows,
            unique_id=kwargs.get("unique_id") or self._settings.unique_id or uuid_hex(),
            user_district_id=self._settings.user_district_id,
            user_city_id=self._settings.user_city_id,
        )

        for _ in range(max_pages):
            data = await self._execute(
                self._settings.search_product_endpoint,
                request.operation_name,
                request.to_variables(),
                graphql_queries.SEARCH_PRODUCT_QUERY,
            )
            products = (
                (data.get("searchProductV5") or {}).get("data") or {}
            ).get("products") or []

            if not products:
                logger.info("No more products for keyword=%r page=%d", keyword, request.page)
                break

            for raw in products:
                product = TokopediaProduct.model_validate(raw)
                yield self._to_event(
                    product,
                    event_type=EventType.PRODUCT_SCRAPED,
                    metadata=self._build_metadata(
                        {"keyword": keyword, "page": request.page},
                        context_metadata,
                    ),
                )

            request = request.model_copy(update={"page": request.page + 1})

    # ------------------------------------------------------------------
    # Public API — shop search
    # ------------------------------------------------------------------

    async def search_shops(
        self,
        keyword: str,
        max_pages: int = 1,
        rows: int | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[KafkaEvent]:
        """Paginate through shop search results for *keyword*.

        Yields:
            :class:`KafkaEvent` with a :class:`TokopediaShop` payload.
        """
        rows = rows or self._settings.default_rows
        request = TokopediaSearchShopRequest(
            keyword=keyword,
            rows=rows,
            start=kwargs.get("start", 0),
            user_district_id=self._settings.user_district_id,
            user_city_id=self._settings.user_city_id,
        )

        for _ in range(max_pages):
            data = await self._execute(
                self._settings.search_shop_endpoint,
                request.operation_name,
                request.to_variables(),
                graphql_queries.SEARCH_SHOP_QUERY,
            )
            shops = (data.get("aceSearchShop") or {}).get("shops") or []

            if not shops:
                logger.info("No more shops for keyword=%r start=%d", keyword, request.start)
                break

            for raw in shops:
                shop = TokopediaShop.model_validate(raw)
                yield self._to_event(
                    shop,
                    event_type=EventType.SHOP_SCRAPED,
                    metadata={"keyword": keyword, "start": request.start},
                )

            request = request.model_copy(update={"start": request.start + rows})

    # ------------------------------------------------------------------
    # Public API — product detail
    # ------------------------------------------------------------------

    async def get_product_detail(
        self,
        product_key: str | None = None,
        shop_domain: str | None = None,
        url: str | None = None,
        **kwargs: Any,
    ) -> Optional[KafkaEvent]:
        """Fetch a single product detail page (PDP).

        Address the product either by ``url`` or by the
        ``shop_domain`` + ``product_key`` pair.

        Returns:
            :class:`KafkaEvent` with a :class:`TokopediaProductDetail`
            payload, or ``None`` when the product was not found.
        """
        if url:
            request = TokopediaProductDetailRequest.from_url(
                url,
                district_id=self._settings.user_district_id,
                city_id=self._settings.user_city_id,
            )
        elif product_key and shop_domain:
            request = TokopediaProductDetailRequest(
                product_key=product_key,
                shop_domain=shop_domain,
                district_id=self._settings.user_district_id,
                city_id=self._settings.user_city_id,
            )
        else:
            raise ValueError("Provide either url= or product_key= and shop_domain=")

        data = await self._execute(
            self._settings.product_detail_endpoint,
            request.operation_name,
            request.to_variables(),
            graphql_queries.PRODUCT_DETAIL_QUERY,
        )
        pdp = data.get("pdpMainInfo") or {}
        basic_info = ((pdp.get("data") or {}).get("basicInfo")) or {}
        if not basic_info:
            logger.warning(
                "No product detail for shop=%r key=%r",
                request.shop_domain, request.product_key,
            )
            return None

        detail_doc = dict(basic_info)
        self._merge_pdp_components(detail_doc, pdp.get("components") or [])

        detail = TokopediaProductDetail.model_validate(detail_doc)
        return self._to_event(
            detail,
            event_type=EventType.PRODUCT_DETAIL_SCRAPED,
            metadata={
                "shop_domain": request.shop_domain,
                "product_key": request.product_key,
            },
        )

    # ------------------------------------------------------------------
    # Public API — product reviews
    # ------------------------------------------------------------------

    async def get_product_reviews(
        self,
        product_id: str,
        max_pages: int = 1,
        limit: int = 10,
        sort_by: str = "informative_score desc",
        filter_by: str = "",
        **kwargs: Any,
    ) -> AsyncIterator[KafkaEvent]:
        """Paginate through reviews of *product_id*.

        Yields:
            :class:`KafkaEvent` with a :class:`TokopediaReview` payload.
        """
        request = TokopediaProductReviewsRequest(
            product_id=product_id,
            page=kwargs.get("page", 1),
            limit=limit,
            sort_by=sort_by,
            filter_by=filter_by,
        )

        for _ in range(max_pages):
            data = await self._execute(
                self._settings.product_reviews_endpoint,
                request.operation_name,
                request.to_variables(),
                graphql_queries.PRODUCT_REVIEWS_QUERY,
            )
            review_list = data.get("productrevGetProductReviewList") or {}
            reviews = review_list.get("list") or []

            if not reviews:
                logger.info("No more reviews for product_id=%r page=%d", product_id, request.page)
                break

            for raw in reviews:
                review = TokopediaReview.model_validate(
                    {**raw, "product_id": product_id}
                )
                yield self._to_event(
                    review,
                    event_type=EventType.REVIEW_SCRAPED,
                    metadata={"product_id": product_id, "page": request.page},
                )

            if not review_list.get("hasNext"):
                break
            request = request.model_copy(update={"page": request.page + 1})

    # ------------------------------------------------------------------
    # Internal — HTTP & GraphQL plumbing
    # ------------------------------------------------------------------

    async def _execute(
        self,
        endpoint: str,
        operation_name: str,
        variables: dict[str, Any],
        query: str,
    ) -> dict[str, Any]:
        """POST one GraphQL operation with retries; return its ``data`` dict."""
        assert self._client is not None, "HTTP client not initialised — call start()"

        payload = GraphQLRequest(
            operation_name=operation_name,
            variables=variables,
            query=query,
        ).to_payload()

        max_retries = max(self._settings.max_retries, 1)
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                await self._throttle()
                resp = await self._client.post(endpoint, json=payload)
                if resp.status_code == 429:
                    raise RateLimitExceeded(f"Too Many Requests on {endpoint}")
                resp.raise_for_status()
                return self._unwrap(resp.json(), operation_name)
            except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError) as exc:
                last_exc = exc
                wait = self._settings.retry_backoff ** attempt
                logger.warning(
                    "Request attempt %d/%d failed (%s): %s. Retrying in %.1fs ...",
                    attempt, max_retries, endpoint, exc, wait,
                )
                await asyncio.sleep(wait)

        logger.error("All %d request attempts failed for %s.", max_retries, endpoint)
        raise last_exc  # type: ignore[misc]

    @staticmethod
    def _unwrap(body: Any, operation_name: str) -> dict[str, Any]:
        """Unwrap a (possibly batched) GraphQL response into its ``data`` dict.

        Raises:
            ErrorRequestException: when the gateway returned GraphQL errors.
        """
        element = body[0] if isinstance(body, list) and body else body
        if not isinstance(element, dict):
            raise ErrorRequestException(
                f"Unexpected GraphQL response shape for {operation_name}: {type(body).__name__}"
            )
        if element.get("errors"):
            messages = "; ".join(
                str(err.get("message", err)) for err in element["errors"]
            )
            raise ErrorRequestException(f"GraphQL error on {operation_name}: {messages}")
        return element.get("data") or {}

    @staticmethod
    def _merge_pdp_components(doc: dict[str, Any], components: list[dict[str, Any]]) -> None:
        """Merge name/price/stock/media from PDP layout components into *doc*.

        The PDP response splits product data across layout components; the
        ``ProductHighlight`` fragment carries name/price/stock and the
        ``ProductMedia`` fragment carries the media gallery.
        """
        for component in components:
            for entry in component.get("data") or []:
                if not isinstance(entry, dict):
                    continue
                if "price" in entry and "name" in entry:
                    doc.setdefault("name", entry.get("name", ""))
                    doc["price"] = entry.get("price") or {}
                    doc["stock"] = entry.get("stock") or {}
                elif "media" in entry and entry.get("media"):
                    doc["media"] = entry["media"]

    async def _throttle(self) -> None:
        """Enforce the configured requests-per-second rate limit."""
        if self._rate_delay > 0:
            await asyncio.sleep(self._rate_delay)

    def _default_headers(self) -> dict[str, str]:
        """Build default HTTP headers matching captured browser traffic."""
        s = self._settings
        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": s.site_url,
            "Referer": f"{s.site_url}/",
            "User-Agent": s.user_agent,
            "X-Device": s.x_device,
            "X-Source": s.x_source,
            "X-Tkpd-Lite-Service": s.lite_service,
            "X-Version": s.x_version,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }
        if s.device_id:
            headers["Bd-Device-Id"] = s.device_id
        return headers

    @staticmethod
    def _to_event(
        document: TokopediaDocument,
        event_type: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> KafkaEvent:
        """Wrap a parsed document in a KafkaEvent envelope."""
        return KafkaEvent(
            event_type=event_type,
            payload=document,
            metadata=metadata or {},
        )

    @staticmethod
    def _build_metadata(
        base: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Merge base + extra metadata. Single place for all API metadata injection."""
        if extra:
            base.update(extra)
        return base


def uuid_hex() -> str:
    """Return a fresh 32-char hex UUID (visitor unique_id fallback)."""
    from uuid import uuid4

    return uuid4().hex
