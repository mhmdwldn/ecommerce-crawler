"""Tests for controllers/tokopedia/* — crawler controllers."""

from __future__ import annotations

import pytest
from pytest_mock import MockerFixture

from controllers.tokopedia.product_reviews import TokopediaProductReviews
from controllers.tokopedia.search_product import TokopediaSearchProduct


def _patch_api_lifecycle(mocker: MockerFixture) -> None:
    mocker.patch("controllers.tokopedia.TokopediaAPI.start", new_callable=mocker.AsyncMock)
    mocker.patch("controllers.tokopedia.TokopediaAPI.stop", new_callable=mocker.AsyncMock)


def _mock_event(mocker: MockerFixture, doc: dict):
    event = mocker.MagicMock()
    event.payload.model_dump.return_value = doc
    event.payload.model_dump_json.return_value = '{"mock": true}'
    return event


class TestTokopediaSearchProductController:
    @pytest.mark.asyncio
    async def test_scrape_to_json(self, mocker: MockerFixture) -> None:
        """scrape_to_json() should return a list of raw product dicts."""
        mock_event = _mock_event(mocker, {"id": "test123", "name": "Test Product"})

        async def mock_search(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.tokopedia.TokopediaAPI.search_products",
            side_effect=mock_search,
        )
        _patch_api_lifecycle(mocker)

        ctl = TokopediaSearchProduct(keyword="test", rows=10, max_pages=1)
        docs = await ctl.scrape_to_json({"keyword": "test"})
        assert len(docs) == 1
        assert docs[0]["id"] == "test123"

    @pytest.mark.asyncio
    async def test_handler_sends_output(self, mocker: MockerFixture) -> None:
        """handler() should call send_output for each product."""
        mock_event = _mock_event(mocker, {"id": "x"})

        async def mock_search(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.tokopedia.TokopediaAPI.search_products",
            side_effect=mock_search,
        )
        _patch_api_lifecycle(mocker)

        send_spy = mocker.patch.object(TokopediaSearchProduct, "send_output")

        ctl = TokopediaSearchProduct(keyword="test", destination="std", rows=5, max_pages=1)
        await ctl.handler({"keyword": "test"})

        assert send_spy.call_count >= 1

    @pytest.mark.asyncio
    async def test_cli_args_override_job(self, mocker: MockerFixture) -> None:
        """CLI kwargs take precedence over job-dict values."""
        captured: dict = {}

        async def mock_search(*args, **kwargs):
            captured.update(kwargs)
            return
            yield  # pragma: no cover — makes this an async generator

        mocker.patch(
            "controllers.tokopedia.TokopediaAPI.search_products",
            side_effect=mock_search,
        )
        _patch_api_lifecycle(mocker)

        ctl = TokopediaSearchProduct(keyword="cli-keyword", rows=7, max_pages=2)
        await ctl.scrape_to_json({"keyword": "job-keyword", "rows": 99})

        assert captured["keyword"] == "cli-keyword"
        assert captured["rows"] == 7
        assert captured["max_pages"] == 2


class TestTokopediaProductReviewsController:
    @pytest.mark.asyncio
    async def test_scrape_to_json(self, mocker: MockerFixture) -> None:
        mock_event = _mock_event(mocker, {"id": "rev1", "productRating": 5})

        async def mock_reviews(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.tokopedia.TokopediaAPI.get_product_reviews",
            side_effect=mock_reviews,
        )
        _patch_api_lifecycle(mocker)

        ctl = TokopediaProductReviews(product_id="102988772766", max_pages=1)
        docs = await ctl.scrape_to_json({"product_id": "102988772766"})
        assert len(docs) == 1
        assert docs[0]["id"] == "rev1"
