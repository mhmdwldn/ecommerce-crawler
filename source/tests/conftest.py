"""Shared pytest fixtures for the Tokopedia crawler test suite."""

from __future__ import annotations

import pytest

from library.config import (
    ElasticsearchSettings,
    KafkaSettings,
    TokopediaCrawlerSettings,
)
from library.schemas import (
    KafkaEvent,
    TokopediaProduct,
    TokopediaReview,
    TokopediaSearchProductRequest,
    TokopediaShop,
)


# ---------------------------------------------------------------------------
# Sample data builders
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_search_request() -> TokopediaSearchProductRequest:
    return TokopediaSearchProductRequest(
        keyword="poco f8",
        page=1,
        rows=20,
        unique_id="abc123",
        user_city_id="176",
        user_district_id="2274",
    )


@pytest.fixture
def sample_product_dict() -> dict:
    return {
        "oldID": 123456,
        "id": "123456",
        "name": "POCO F8 Pro 12/512GB",
        "url": "https://www.tokopedia.com/xiaomi/poco-f8-pro",
        "applink": "tokopedia://product/123456",
        "mediaURL": {
            "image": "https://images.tokopedia.net/poco-f8.jpg",
            "image300": "https://images.tokopedia.net/poco-f8-300.jpg",
        },
        "shop": {
            "id": "99",
            "name": "Xiaomi Official Store",
            "url": "https://www.tokopedia.com/xiaomi",
            "city": "Jakarta Pusat",
            "tier": 2,
        },
        "price": {
            "text": "Rp7.999.000",
            "number": 7999000,
            "original": "Rp8.999.000",
            "discountPercentage": 11,
        },
        "category": {"id": "24", "name": "Handphone"},
        "rating": "4.9",
        "wishlist": False,
    }


@pytest.fixture
def sample_product(sample_product_dict: dict) -> TokopediaProduct:
    return TokopediaProduct.model_validate(sample_product_dict)


@pytest.fixture
def sample_shop_dict() -> dict:
    return {
        "old_shop_id": 99,
        "shop_id": "99",
        "shop_name": "Xiaomi Official Store",
        "shop_domain": "xiaomi",
        "shop_location": "Jakarta Pusat",
        "shop_tag_line": "Official store",
        "shop_url": "https://www.tokopedia.com/xiaomi",
        "shop_image": "https://images.tokopedia.net/xiaomi.jpg",
        "reputation_score": "100",
        "shop_total_favorite": 50000,
        "shop_gold_shop": True,
        "is_official": True,
        "is_pm_pro": False,
        "products": [
            {
                "id": "123456",
                "name": "POCO F8 Pro",
                "url": "https://www.tokopedia.com/xiaomi/poco-f8-pro",
                "price_format": "Rp7.999.000",
                "image_url": "https://images.tokopedia.net/poco-f8.jpg",
            }
        ],
    }


@pytest.fixture
def sample_review_dict() -> dict:
    return {
        "id": "9876543",
        "variantName": "Black",
        "message": "Barang bagus, pengiriman cepat!",
        "productRating": 5,
        "reviewCreateTime": "3 minggu lalu",
        "reviewCreateTimestamp": "2026-05-20T10:00:00",
        "isAnonymous": False,
        "imageAttachments": None,
        "videoAttachments": None,
        "reviewResponse": {"message": "Terima kasih!", "createTime": "2 minggu lalu"},
        "user": {
            "userID": "555",
            "fullName": "Budi",
            "image": "https://images.tokopedia.net/budi.jpg",
            "url": "https://www.tokopedia.com/people/555",
        },
        "likeDislike": {"totalLike": 3, "likeStatus": 0},
        "badRatingReasonFmt": "",
    }


@pytest.fixture
def sample_kafka_event(sample_product: TokopediaProduct) -> KafkaEvent:
    return KafkaEvent(
        event_type="tokopedia.product.scraped",
        payload=sample_product,
        metadata={"keyword": "poco f8"},
    )


# ---------------------------------------------------------------------------
# GraphQL response envelopes (batched list shape, as served by the gateway)
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_search_product_response(sample_product_dict: dict) -> list:
    return [
        {
            "data": {
                "searchProductV5": {
                    "header": {"totalData": 1, "responseCode": 0},
                    "data": {
                        "totalDataText": "1",
                        "products": [sample_product_dict],
                    },
                }
            }
        }
    ]


@pytest.fixture
def sample_search_shop_response(sample_shop_dict: dict) -> list:
    return [
        {
            "data": {
                "aceSearchShop": {
                    "shops": [sample_shop_dict],
                    "header": {"total_data": 1, "response_code": "0"},
                }
            }
        }
    ]


@pytest.fixture
def sample_reviews_response(sample_review_dict: dict) -> list:
    return [
        {
            "data": {
                "productrevGetProductReviewList": {
                    "productID": "102988772766",
                    "list": [sample_review_dict],
                    "shop": {"shopID": "99", "name": "Xiaomi Official Store"},
                    "hasNext": False,
                    "totalReviews": 1,
                }
            }
        }
    ]


@pytest.fixture
def sample_pdp_response() -> list:
    return [
        {
            "data": {
                "pdpMainInfo": {
                    "requestID": "req-1",
                    "data": {
                        "layoutName": "default",
                        "basicInfo": {
                            "id": "111222",
                            "productID": "111222",
                            "alias": "poco-f8-pro",
                            "shopID": "99",
                            "shopName": "Xiaomi Official Store",
                            "url": "https://www.tokopedia.com/xiaomi/poco-f8-pro",
                            "minOrder": 1,
                            "maxOrder": 5,
                            "weight": 500,
                            "weightUnit": "gram",
                            "condition": 1,
                            "status": "ACTIVE",
                            "defaultMediaURL": "https://images.tokopedia.net/poco-f8.jpg",
                            "category": {"id": "24", "name": "Handphone"},
                            "txStats": {"countSold": 150, "itemSoldFmt": "150+"},
                            "stats": {"countReview": 40, "countView": 9000, "rating": 4.9},
                        },
                    },
                    "components": [
                        {
                            "name": "product_content",
                            "type": "data",
                            "position": 1,
                            "data": [
                                {
                                    "name": "POCO F8 Pro 12/512GB",
                                    "price": {
                                        "value": 7999000,
                                        "currency": "IDR",
                                        "priceFmt": "Rp7.999.000",
                                    },
                                    "stock": {"value": 10, "useStock": True},
                                }
                            ],
                        },
                        {
                            "name": "product_media",
                            "type": "data",
                            "position": 2,
                            "data": [
                                {
                                    "media": [
                                        {
                                            "type": "image",
                                            "urlOriginal": "https://images.tokopedia.net/poco-f8.jpg",
                                        }
                                    ]
                                }
                            ],
                        },
                    ],
                }
            }
        }
    ]


# ---------------------------------------------------------------------------
# Settings fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers="test-kafka:9092",
        topic="test.topic",
    )


@pytest.fixture
def es_settings() -> ElasticsearchSettings:
    return ElasticsearchSettings(
        hosts=["http://test-es:9200"],
        index_name="test_index",
    )


@pytest.fixture
def crawler_settings() -> TokopediaCrawlerSettings:
    return TokopediaCrawlerSettings(
        base_url="https://gql.tokopedia.com",
        rate_limit_rps=100.0,
        request_timeout=5.0,
        max_retries=2,
        unique_id="test-unique-id",
    )
