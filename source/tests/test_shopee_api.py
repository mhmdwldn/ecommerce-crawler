"""Tests for library/shopee_api.py — ShopeeAPI client.

The fixture payloads are trimmed copies of a real
``/api/v4/search/search_items`` response captured during development, so the
parsing path is exercised against the actual Shopee schema.
"""

from __future__ import annotations

import copy

import pytest
from pytest_mock import MockerFixture

from exception.exception import ErrorRequestException
from library.config import ShopeeCrawlerSettings
from library.schemas import KafkaEvent, ShopeeProduct, ShopeeSearchProductRequest
from library.shopee_api import ShopeeAPI


# ---------------------------------------------------------------------------
# Fixtures (trimmed from a real Shopee response)
# ---------------------------------------------------------------------------


@pytest.fixture
def shopee_settings() -> ShopeeCrawlerSettings:
    return ShopeeCrawlerSettings(
        base_url="https://shopee.co.id",
        rate_limit_rps=100.0,
        request_timeout=5.0,
        max_retries=2,
        cookies="csrftoken=abc123; SPC_F=xyz",
    )


@pytest.fixture
def real_item_basic() -> dict:
    return {
        "itemid": 55362062936,
        "shopid": 765981374,
        "name": "Bantalan Busa Earcup Earpad Headphones Foam Sony - WH CH710N",
        "price": 6000000000,
        "price_min": 6000000000,
        "price_max": 6000000000,
        "currency": "IDR",
        "stock": 1,
        "sold": 0,
        "historical_sold": 12,
        "liked_count": 3,
        "shop_location": "Jakarta Barat",
        "image": "sg-11134201-823op-mpcmbrs0syz17c",
        "shop_name": "Earcup_Marketing",
        "item_rating": {"rating_star": 4.5, "rating_count": [0, 0, 0, 0, 0, 0]},
    }


@pytest.fixture
def search_response(real_item_basic: dict) -> dict:
    return {
        "error": None,
        "error_msg": None,
        "total_count": 1,
        "nomore": False,
        "items": [{"itemid": 55362062936, "shopid": 765981374, "item_basic": real_item_basic}],
    }


def _mock_response(mocker: MockerFixture, body) -> object:
    resp = mocker.MagicMock()
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    resp.json.return_value = body
    return resp


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestShopeeProduct:
    def test_parse_real_item(self, real_item_basic: dict) -> None:
        product = ShopeeProduct.model_validate(real_item_basic)
        assert product.id == "55362062936"
        assert product.shop_id == "765981374"
        assert product.price == 6_000_000_000
        assert product.price_idr == 60_000.0          # Shopee scales price by 1e5
        assert product.historical_sold == 12
        assert product.rating == 4.5                  # lifted from item_rating.rating_star
        assert product.shop_location == "Jakarta Barat"

    def test_zero_rating(self, real_item_basic: dict) -> None:
        real_item_basic["item_rating"] = {"rating_star": 0}
        assert ShopeeProduct.model_validate(real_item_basic).rating == 0.0


class TestShopeeSearchProductRequest:
    def test_keyword_params(self) -> None:
        params = ShopeeSearchProductRequest(keyword="sepatu lari pria", limit=20).to_params()
        assert params["keyword"] == "sepatu lari pria"
        assert params["scenario"] == "PAGE_GLOBAL_SEARCH"
        assert params["limit"] == "20"
        assert "match_id" not in params

    def test_category_params(self) -> None:
        params = ShopeeSearchProductRequest(
            match_id="11044364", scenario="PAGE_CATEGORY", by="ctime"
        ).to_params()
        assert params["match_id"] == "11044364"
        assert params["scenario"] == "PAGE_CATEGORY"
        assert "keyword" not in params

    def test_paging_offset(self) -> None:
        params = ShopeeSearchProductRequest(keyword="x", page=3, limit=60).to_params()
        assert params["newest"] == "120"

    def test_requires_keyword_or_match(self) -> None:
        with pytest.raises(ValueError):
            ShopeeSearchProductRequest()


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_context_manager(self, shopee_settings) -> None:
        async with ShopeeAPI(shopee_settings) as api:
            assert api._client is not None
        assert api._client is None

    def test_default_headers_echo_csrf(self, shopee_settings) -> None:
        api = ShopeeAPI(shopee_settings)
        headers = api._default_headers({"csrftoken": "abc123"})
        assert headers["X-Csrftoken"] == "abc123"
        assert headers["X-Api-Source"] == "pc"
        assert headers["X-Shopee-Language"] == "id"

    def test_extra_headers_merged(self) -> None:
        settings = ShopeeCrawlerSettings(extra_headers={"x-sap-sec": "TOKEN"})
        api = ShopeeAPI(settings)
        assert api._default_headers(None)["x-sap-sec"] == "TOKEN"


class TestSearchProducts:
    @pytest.mark.asyncio
    async def test_yields_kafka_events(
        self, mocker: MockerFixture, shopee_settings, search_response: dict
    ) -> None:
        api = ShopeeAPI(shopee_settings)
        mock_client = mocker.AsyncMock()
        mock_client.get.return_value = _mock_response(mocker, search_response)
        api._client = mock_client

        events = [e async for e in api.search_products(keyword="sepatu lari pria", max_pages=1)]
        assert len(events) == 1
        assert isinstance(events[0], KafkaEvent)
        assert events[0].event_type == "shopee.product.scraped"
        assert events[0].source == "shopee-crawler"
        assert events[0].payload.name.startswith("Bantalan Busa")
        assert events[0].payload.url.endswith("i.765981374.55362062936")

    @pytest.mark.asyncio
    async def test_stops_on_nomore(
        self, mocker: MockerFixture, shopee_settings, search_response: dict
    ) -> None:
        api = ShopeeAPI(shopee_settings)
        nomore = copy.deepcopy(search_response)
        nomore["nomore"] = True

        mock_client = mocker.AsyncMock()
        mock_client.get = mocker.AsyncMock(
            side_effect=[_mock_response(mocker, nomore), _mock_response(mocker, search_response)]
        )
        api._client = mock_client

        events = [e async for e in api.search_products(keyword="x", max_pages=5)]
        assert len(events) == 1
        assert mock_client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_category_mode(
        self, mocker: MockerFixture, shopee_settings, search_response: dict
    ) -> None:
        api = ShopeeAPI(shopee_settings)
        mock_client = mocker.AsyncMock()
        mock_client.get.return_value = _mock_response(mocker, search_response)
        api._client = mock_client

        events = [
            e async for e in api.search_products(match_id="11044364", max_pages=1)
        ]
        assert len(events) == 1
        assert events[0].metadata["query"] == "category:11044364"


class TestAntiBotHandling:
    @pytest.mark.asyncio
    async def test_error_code_raises(
        self, mocker: MockerFixture, shopee_settings
    ) -> None:
        """A non-zero ``error`` (e.g. 90309999 anti-bot block) must raise."""
        api = ShopeeAPI(shopee_settings)
        blocked = {"error": 90309999, "error_msg": "", "items": None}
        mock_client = mocker.AsyncMock()
        mock_client.get.return_value = _mock_response(mocker, blocked)
        api._client = mock_client

        with pytest.raises(ErrorRequestException, match="90309999"):
            _ = [e async for e in api.search_products(keyword="x", max_pages=1)]

    def test_unwrap_rejects_non_dict(self) -> None:
        with pytest.raises(ErrorRequestException, match="anti-bot"):
            ShopeeAPI._unwrap("<html>blocked</html>")
