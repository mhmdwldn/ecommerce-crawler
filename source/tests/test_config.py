"""Tests for library/config.py — Pydantic BaseSettings."""

from __future__ import annotations

import pytest

from library.config import (
    ElasticsearchSettings,
    KafkaSettings,
    Settings,
    TokopediaCrawlerSettings,
)


class TestKafkaSettings:
    def test_defaults(self) -> None:
        ks = KafkaSettings()
        assert ks.bootstrap_servers == "localhost:9092"
        assert ks.topic == "tokopedia.products.raw"
        assert ks.client_id == "tokopedia-crawler"

    def test_override_via_init(self) -> None:
        ks = KafkaSettings(bootstrap_servers="kafka:29092", topic="custom.topic")
        assert ks.bootstrap_servers == "kafka:29092"
        assert ks.topic == "custom.topic"


class TestElasticsearchSettings:
    def test_defaults(self) -> None:
        es = ElasticsearchSettings()
        assert es.hosts == ["http://localhost:9200"]
        assert es.index_name == "tokopedia_products"
        assert es.max_retries == 3


class TestTokopediaCrawlerSettings:
    def test_defaults(self) -> None:
        cs = TokopediaCrawlerSettings()
        assert cs.base_url == "https://gql.tokopedia.com"
        assert cs.site_url == "https://www.tokopedia.com"
        assert cs.search_product_endpoint == "/graphql/SearchProductV5Query"
        assert cs.search_shop_endpoint == "/graphql/AceSearchShopQuery"
        assert cs.product_detail_endpoint == "/graphql/PDPMainInfo"
        assert cs.product_reviews_endpoint == "/graphql/productReviewList"
        assert cs.rate_limit_rps == 5.0

    def test_rows_bounds(self) -> None:
        with pytest.raises(Exception):
            TokopediaCrawlerSettings(default_rows=0)
        with pytest.raises(Exception):
            TokopediaCrawlerSettings(default_rows=1000)


class TestRootSettings:
    def test_nested_settings_created(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("library.config.Path.exists", lambda _self: False)
        settings = Settings()
        assert isinstance(settings.kafka, KafkaSettings)
        assert isinstance(settings.elasticsearch, ElasticsearchSettings)
        assert isinstance(settings.crawler, TokopediaCrawlerSettings)
