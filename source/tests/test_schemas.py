"""Tests for library/schemas.py — Pydantic v2 models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from library.schemas import (
    GraphQLRequest,
    KafkaEvent,
    TokopediaProduct,
    TokopediaProductDetailRequest,
    TokopediaProductReviewsRequest,
    TokopediaReview,
    TokopediaSearchProductRequest,
    TokopediaSearchShopRequest,
    TokopediaShop,
)


class TestGraphQLRequest:
    def test_payload_is_batched_list(self) -> None:
        req = GraphQLRequest(operation_name="Op", variables={"a": 1}, query="query Op { x }")
        payload = req.to_payload()
        assert isinstance(payload, list)
        assert payload[0]["operationName"] == "Op"
        assert payload[0]["variables"] == {"a": 1}


class TestSearchProductRequest:
    def test_valid_request(self, sample_search_request: TokopediaSearchProductRequest) -> None:
        assert sample_search_request.keyword == "poco f8"
        assert sample_search_request.rows == 20

    def test_missing_keyword_raises(self) -> None:
        with pytest.raises(ValidationError):
            TokopediaSearchProductRequest()

    def test_blank_keyword_raises(self) -> None:
        with pytest.raises(ValidationError):
            TokopediaSearchProductRequest(keyword="")

    def test_rows_out_of_bounds(self) -> None:
        with pytest.raises(ValidationError):
            TokopediaSearchProductRequest(keyword="x", rows=0)
        with pytest.raises(ValidationError):
            TokopediaSearchProductRequest(keyword="x", rows=1000)

    def test_to_params_encodes_keyword_and_paging(
        self, sample_search_request: TokopediaSearchProductRequest
    ) -> None:
        params = sample_search_request.to_params()
        assert "q=poco+f8" in params
        assert "rows=20" in params
        assert "page=1" in params
        assert "start=0" in params
        assert "unique_id=abc123" in params
        assert "user_cityId=176" in params

    def test_start_derives_from_page(self) -> None:
        req = TokopediaSearchProductRequest(keyword="x", page=3, rows=20)
        assert "start=40" in req.to_params()

    def test_to_variables_wraps_params(
        self, sample_search_request: TokopediaSearchProductRequest
    ) -> None:
        variables = sample_search_request.to_variables()
        assert set(variables) == {"params"}


class TestSearchShopRequest:
    def test_to_params(self) -> None:
        req = TokopediaSearchShopRequest(keyword="xiaomi", rows=30, start=30)
        params = req.to_params()
        assert "q=xiaomi" in params
        assert "rows=30" in params
        assert "start=30" in params

    def test_missing_keyword_raises(self) -> None:
        with pytest.raises(ValidationError):
            TokopediaSearchShopRequest()


class TestProductDetailRequest:
    def test_to_variables(self) -> None:
        req = TokopediaProductDetailRequest(
            product_key="poco-f8-pro", shop_domain="xiaomi",
            district_id="2274", city_id="176",
        )
        variables = req.to_variables()
        assert variables["productKey"] == "poco-f8-pro"
        assert variables["shopDomain"] == "xiaomi"
        assert variables["userLocation"]["districtID"] == "2274"

    def test_from_url(self) -> None:
        req = TokopediaProductDetailRequest.from_url(
            "https://www.tokopedia.com/xiaomi/poco-f8-pro?src=search"
        )
        assert req.shop_domain == "xiaomi"
        assert req.product_key == "poco-f8-pro"

    def test_from_url_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            TokopediaProductDetailRequest.from_url("https://www.tokopedia.com/xiaomi")


class TestProductReviewsRequest:
    def test_to_variables(self) -> None:
        req = TokopediaProductReviewsRequest(product_id="102988772766", page=2, limit=10)
        variables = req.to_variables()
        assert variables["productID"] == "102988772766"
        assert variables["page"] == 2
        assert variables["limit"] == 10
        assert variables["sortBy"] == "informative_score desc"

    def test_missing_product_id_raises(self) -> None:
        with pytest.raises(ValidationError):
            TokopediaProductReviewsRequest()


class TestTokopediaProduct:
    def test_parse_from_dict(self, sample_product_dict: dict) -> None:
        product = TokopediaProduct.model_validate(sample_product_dict)
        assert product.id == "123456"
        assert product.name == "POCO F8 Pro 12/512GB"
        assert product.price.number == 7999000
        assert product.price.discount_percentage == 11
        assert product.shop.name == "Xiaomi Official Store"

    def test_rating_string_coercion(self, sample_product: TokopediaProduct) -> None:
        assert sample_product.rating == 4.9

    def test_rating_empty_string_defaults_zero(self) -> None:
        product = TokopediaProduct.model_validate({"id": "1", "rating": ""})
        assert product.rating == 0.0

    def test_minimal_product(self) -> None:
        product = TokopediaProduct.model_validate({"id": 123})
        assert product.id == "123"


class TestTokopediaShop:
    def test_parse_from_dict(self, sample_shop_dict: dict) -> None:
        shop = TokopediaShop.model_validate(sample_shop_dict)
        assert shop.shop_id == "99"
        assert shop.shop_name == "Xiaomi Official Store"
        assert shop.is_official is True
        assert len(shop.products) == 1
        assert shop.products[0].price_format == "Rp7.999.000"

    def test_null_products(self, sample_shop_dict: dict) -> None:
        sample_shop_dict["products"] = None
        shop = TokopediaShop.model_validate(sample_shop_dict)
        assert shop.products == []


class TestTokopediaReview:
    def test_parse_from_dict(self, sample_review_dict: dict) -> None:
        review = TokopediaReview.model_validate(sample_review_dict)
        assert review.id == "9876543"
        assert review.rating == 5
        assert review.user.full_name == "Budi"
        assert review.total_like == 3
        assert review.response.message == "Terima kasih!"

    def test_null_attachments_coerced(self, sample_review_dict: dict) -> None:
        review = TokopediaReview.model_validate(sample_review_dict)
        assert review.image_attachments == []
        assert review.video_attachments == []

    def test_null_response_coerced(self, sample_review_dict: dict) -> None:
        sample_review_dict["reviewResponse"] = None
        review = TokopediaReview.model_validate(sample_review_dict)
        assert review.response.message == ""


class TestKafkaEvent:
    def test_create_event(self, sample_product: TokopediaProduct) -> None:
        event = KafkaEvent(
            event_type="tokopedia.product.scraped",
            payload=sample_product,
            metadata={"keyword": "poco f8"},
        )
        assert event.event_type == "tokopedia.product.scraped"
        assert len(event.event_id) == 32
        assert event.payload == sample_product
        assert event.source == "tokopedia-crawler"

    def test_extra_fields_forbidden(self, sample_product: TokopediaProduct) -> None:
        with pytest.raises(ValidationError):
            KafkaEvent(payload=sample_product, unknown_field="oops")
