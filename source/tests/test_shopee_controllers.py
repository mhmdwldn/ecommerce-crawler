"""Tests for controllers/shopee/* — Shopee crawler controllers."""

from __future__ import annotations

import pytest
from pytest_mock import MockerFixture

from controllers.shopee.search_product import ShopeeSearchProduct


def _patch_api_lifecycle(mocker: MockerFixture) -> None:
    mocker.patch("controllers.shopee.ShopeeAPI.start", new_callable=mocker.AsyncMock)
    mocker.patch("controllers.shopee.ShopeeAPI.stop", new_callable=mocker.AsyncMock)


def _mock_event(mocker: MockerFixture, doc: dict):
    event = mocker.MagicMock()
    event.payload.model_dump.return_value = doc
    event.payload.model_dump_json.return_value = '{"mock": true}'
    return event


class TestShopeeSearchProductController:
    @pytest.mark.asyncio
    async def test_scrape_to_json(self, mocker: MockerFixture) -> None:
        """scrape_to_json() should return a list of raw product dicts."""
        mock_event = _mock_event(mocker, {"itemid": "55362062936", "name": "Earpad"})

        async def mock_search(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.shopee.ShopeeAPI.search_products",
            side_effect=mock_search,
        )
        _patch_api_lifecycle(mocker)

        ctl = ShopeeSearchProduct(keyword="sepatu lari pria", limit=10, max_pages=1)
        docs = await ctl.scrape_to_json({"keyword": "sepatu lari pria"})
        assert len(docs) == 1
        assert docs[0]["itemid"] == "55362062936"

    @pytest.mark.asyncio
    async def test_handler_sends_output(self, mocker: MockerFixture) -> None:
        """handler() should call send_output for each product."""
        mock_event = _mock_event(mocker, {"itemid": "x"})

        async def mock_search(*args, **kwargs):
            yield mock_event

        mocker.patch(
            "controllers.shopee.ShopeeAPI.search_products",
            side_effect=mock_search,
        )
        _patch_api_lifecycle(mocker)

        send_spy = mocker.patch.object(ShopeeSearchProduct, "send_output")

        ctl = ShopeeSearchProduct(keyword="sepatu", destination="std", limit=5, max_pages=1)
        await ctl.handler({"keyword": "sepatu"})

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
            "controllers.shopee.ShopeeAPI.search_products",
            side_effect=mock_search,
        )
        _patch_api_lifecycle(mocker)

        ctl = ShopeeSearchProduct(keyword="cli-keyword", limit=7, max_pages=2)
        await ctl.scrape_to_json({"keyword": "job-keyword", "limit": 99})

        assert captured["keyword"] == "cli-keyword"
        assert captured["limit"] == 7
        assert captured["max_pages"] == 2

    @pytest.mark.asyncio
    async def test_category_mode(self, mocker: MockerFixture) -> None:
        """match_id job field drives category mode."""
        captured: dict = {}

        async def mock_search(*args, **kwargs):
            captured.update(kwargs)
            return
            yield  # pragma: no cover

        mocker.patch(
            "controllers.shopee.ShopeeAPI.search_products",
            side_effect=mock_search,
        )
        _patch_api_lifecycle(mocker)

        ctl = ShopeeSearchProduct(max_pages=1)
        await ctl.scrape_to_json({"match_id": "11044364"})

        assert captured["match_id"] == "11044364"
        assert captured["keyword"] == ""
