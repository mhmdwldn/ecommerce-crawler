"""Tests for library/tokopedia_api.py — TokopediaAPI client."""

from __future__ import annotations

import copy

import pytest
from pytest_mock import MockerFixture

from exception.exception import ErrorRequestException
from library.schemas import KafkaEvent, TokopediaProductDetail
from library.tokopedia_api import TokopediaAPI


def _mock_response(mocker: MockerFixture, body) -> object:
    """Build a fake httpx response with the given JSON body."""
    resp = mocker.MagicMock()
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    resp.json.return_value = body
    return resp


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_creates_client(self, crawler_settings) -> None:
        api = TokopediaAPI(crawler_settings)
        await api.start()
        assert api._client is not None
        await api.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_client(self, crawler_settings) -> None:
        api = TokopediaAPI(crawler_settings)
        await api.start()
        await api.stop()
        assert api._client is None

    @pytest.mark.asyncio
    async def test_async_context_manager(self, crawler_settings) -> None:
        async with TokopediaAPI(crawler_settings) as api:
            assert api._client is not None
        assert api._client is None


class TestSearchProducts:
    @pytest.mark.asyncio
    async def test_yields_kafka_events(
        self, mocker: MockerFixture, crawler_settings, sample_search_product_response: list
    ) -> None:
        api = TokopediaAPI(crawler_settings)
        mock_client = mocker.AsyncMock()
        mock_client.post.return_value = _mock_response(mocker, sample_search_product_response)
        api._client = mock_client

        events = [e async for e in api.search_products(keyword="poco f8", max_pages=1)]
        assert len(events) == 1
        assert isinstance(events[0], KafkaEvent)
        assert events[0].event_type == "tokopedia.product.scraped"
        assert events[0].payload.name == "POCO F8 Pro 12/512GB"
        assert events[0].metadata == {"keyword": "poco f8", "page": 1}

    @pytest.mark.asyncio
    async def test_paginates(
        self, mocker: MockerFixture, crawler_settings, sample_search_product_response: list
    ) -> None:
        api = TokopediaAPI(crawler_settings)
        call_count = 0

        async def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return _mock_response(mocker, copy.deepcopy(sample_search_product_response))

        mock_client = mocker.AsyncMock()
        mock_client.post = mocker.AsyncMock(side_effect=side_effect)
        api._client = mock_client

        events = [e async for e in api.search_products(keyword="poco f8", max_pages=2)]
        assert len(events) == 2
        assert call_count == 2
        # Page number advanced between requests
        assert events[0].metadata["page"] == 1
        assert events[1].metadata["page"] == 2

    @pytest.mark.asyncio
    async def test_stops_when_no_products(
        self, mocker: MockerFixture, crawler_settings, sample_search_product_response: list
    ) -> None:
        api = TokopediaAPI(crawler_settings)
        empty = copy.deepcopy(sample_search_product_response)
        empty[0]["data"]["searchProductV5"]["data"]["products"] = []

        responses = [
            _mock_response(mocker, sample_search_product_response),
            _mock_response(mocker, empty),
        ]
        mock_client = mocker.AsyncMock()
        mock_client.post = mocker.AsyncMock(side_effect=responses)
        api._client = mock_client

        events = [e async for e in api.search_products(keyword="poco f8", max_pages=5)]
        assert len(events) == 1


class TestSearchShops:
    @pytest.mark.asyncio
    async def test_yields_kafka_events(
        self, mocker: MockerFixture, crawler_settings, sample_search_shop_response: list
    ) -> None:
        api = TokopediaAPI(crawler_settings)
        mock_client = mocker.AsyncMock()
        mock_client.post.return_value = _mock_response(mocker, sample_search_shop_response)
        api._client = mock_client

        events = [e async for e in api.search_shops(keyword="xiaomi", max_pages=1)]
        assert len(events) == 1
        assert events[0].event_type == "tokopedia.shop.scraped"
        assert events[0].payload.shop_name == "Xiaomi Official Store"


class TestProductDetail:
    @pytest.mark.asyncio
    async def test_merges_components_into_document(
        self, mocker: MockerFixture, crawler_settings, sample_pdp_response: list
    ) -> None:
        api = TokopediaAPI(crawler_settings)
        mock_client = mocker.AsyncMock()
        mock_client.post.return_value = _mock_response(mocker, sample_pdp_response)
        api._client = mock_client

        event = await api.get_product_detail(product_key="poco-f8-pro", shop_domain="xiaomi")
        assert event is not None
        assert isinstance(event.payload, TokopediaProductDetail)
        assert event.payload.id == "111222"
        assert event.payload.name == "POCO F8 Pro 12/512GB"          # from ProductHighlight
        assert event.payload.price["priceFmt"] == "Rp7.999.000"      # from ProductHighlight
        assert event.payload.media[0]["urlOriginal"].endswith("poco-f8.jpg")  # from ProductMedia
        assert event.payload.tx_stats.count_sold == 150

    @pytest.mark.asyncio
    async def test_accepts_url(
        self, mocker: MockerFixture, crawler_settings, sample_pdp_response: list
    ) -> None:
        api = TokopediaAPI(crawler_settings)
        mock_client = mocker.AsyncMock()
        mock_client.post.return_value = _mock_response(mocker, sample_pdp_response)
        api._client = mock_client

        event = await api.get_product_detail(url="https://www.tokopedia.com/xiaomi/poco-f8-pro")
        assert event is not None
        assert event.metadata["shop_domain"] == "xiaomi"

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(
        self, mocker: MockerFixture, crawler_settings
    ) -> None:
        api = TokopediaAPI(crawler_settings)
        body = [{"data": {"pdpMainInfo": {"data": {"basicInfo": None}}}}]
        mock_client = mocker.AsyncMock()
        mock_client.post.return_value = _mock_response(mocker, body)
        api._client = mock_client

        event = await api.get_product_detail(product_key="x", shop_domain="y")
        assert event is None

    @pytest.mark.asyncio
    async def test_requires_address(self, crawler_settings) -> None:
        api = TokopediaAPI(crawler_settings)
        with pytest.raises(ValueError):
            await api.get_product_detail()


class TestProductReviews:
    @pytest.mark.asyncio
    async def test_yields_kafka_events(
        self, mocker: MockerFixture, crawler_settings, sample_reviews_response: list
    ) -> None:
        api = TokopediaAPI(crawler_settings)
        mock_client = mocker.AsyncMock()
        mock_client.post.return_value = _mock_response(mocker, sample_reviews_response)
        api._client = mock_client

        events = [
            e async for e in api.get_product_reviews(product_id="102988772766", max_pages=3)
        ]
        # hasNext=False — must stop after the first page despite max_pages=3
        assert len(events) == 1
        assert events[0].event_type == "tokopedia.review.scraped"
        assert events[0].payload.product_id == "102988772766"
        assert events[0].payload.rating == 5

    @pytest.mark.asyncio
    async def test_paginates_while_has_next(
        self, mocker: MockerFixture, crawler_settings, sample_reviews_response: list
    ) -> None:
        api = TokopediaAPI(crawler_settings)
        page1 = copy.deepcopy(sample_reviews_response)
        page1[0]["data"]["productrevGetProductReviewList"]["hasNext"] = True

        responses = [
            _mock_response(mocker, page1),
            _mock_response(mocker, sample_reviews_response),
        ]
        mock_client = mocker.AsyncMock()
        mock_client.post = mocker.AsyncMock(side_effect=responses)
        api._client = mock_client

        events = [
            e async for e in api.get_product_reviews(product_id="102988772766", max_pages=5)
        ]
        assert len(events) == 2
        assert events[1].metadata["page"] == 2


class TestGraphQLPlumbing:
    def test_unwrap_batched_response(self) -> None:
        data = TokopediaAPI._unwrap([{"data": {"x": 1}}], "Op")
        assert data == {"x": 1}

    def test_unwrap_plain_response(self) -> None:
        data = TokopediaAPI._unwrap({"data": {"x": 1}}, "Op")
        assert data == {"x": 1}

    def test_unwrap_raises_on_graphql_errors(self) -> None:
        with pytest.raises(ErrorRequestException, match="something broke"):
            TokopediaAPI._unwrap([{"errors": [{"message": "something broke"}]}], "Op")

    def test_default_headers(self, crawler_settings) -> None:
        api = TokopediaAPI(crawler_settings)
        headers = api._default_headers()
        assert headers["Content-Type"] == "application/json"
        assert headers["Origin"] == "https://www.tokopedia.com"
        assert headers["X-Source"] == "tokopedia-lite"
        assert headers["X-Tkpd-Lite-Service"] == "zeus"
        assert "Bd-Device-Id" not in headers  # empty device_id is omitted
