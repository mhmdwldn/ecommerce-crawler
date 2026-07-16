"""
Pydantic v2 data schemas for the Tokopedia crawler pipeline.

Defines:
  - GraphQLRequest:                  generic GraphQL wire envelope
  - Tokopedia*Request models:        typed payload builders per endpoint
  - TokopediaProduct / Shop /
    ProductDetail / Review:          parsed documents emitted by the pipeline
  - KafkaEvent:                      event envelope published to Kafka
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Union
from urllib.parse import urlencode, urlparse
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class EventType(str, Enum):
    """Discriminator for Kafka event routing — single source of truth. (str, Enum) = StrEnum for Python 3.10+."""
    PRODUCT_SCRAPED = "tokopedia.product.scraped"
    SHOP_SCRAPED = "tokopedia.shop.scraped"
    PRODUCT_DETAIL_SCRAPED = "tokopedia.product_detail.scraped"
    REVIEW_SCRAPED = "tokopedia.review.scraped"

# ---------------------------------------------------------------------------
# GraphQL wire envelope
# ---------------------------------------------------------------------------


class GraphQLRequest(BaseModel):
    """A single GraphQL operation as sent to gql.tokopedia.com.

    The gateway expects a JSON *list* of these objects (batched protocol),
    so use :meth:`to_payload` when posting.
    """

    operation_name: str = Field(..., alias="operationName")
    variables: dict[str, Any] = Field(default_factory=dict)
    query: str = Field(...)

    model_config = ConfigDict(populate_by_name=True)

    def to_payload(self) -> list[dict[str, Any]]:
        """Return the batched JSON body expected by the gateway."""
        return [self.model_dump(by_alias=True)]


# ---------------------------------------------------------------------------
# Request schemas (one per crawler type)
# ---------------------------------------------------------------------------


class TokopediaSearchProductRequest(BaseModel):
    """Request parameters for ``SearchProductV5Query`` (product search)."""

    operation_name: ClassVar[str] = "SearchProductV5Query"

    keyword: str = Field(..., min_length=1, max_length=500, description="Search query")
    page: int = Field(default=1, ge=1)
    rows: int = Field(default=20, ge=1, le=100)
    ob: str = Field(default="23", description="Sort order (23 = most relevant)")
    unique_id: str = Field(
        default_factory=lambda: uuid4().hex,
        description="Non-login visitor UUID",
    )
    user_district_id: str = Field(default="2274", description="Location bias: district ID")
    user_city_id: str = Field(default="176", description="Location bias: city ID")
    safe_search: bool = Field(default=False)
    related: bool = Field(default=True)

    def to_params(self) -> str:
        """Encode the request as the ``params`` query-string variable."""
        start = (self.page - 1) * self.rows
        fields = {
            "device": "desktop",
            "enter_method": "normal_search",
            "l_name": "sre",
            "navsource": "",
            "ob": self.ob,
            "page": str(self.page),
            "q": self.keyword,
            "related": str(self.related).lower(),
            "rows": str(self.rows),
            "safe_search": str(self.safe_search).lower(),
            "scheme": "https",
            "show_adult": "false",
            "source": "search",
            "st": "product",
            "start": str(start),
            "topads_bucket": "true",
            "unique_id": self.unique_id,
            "user_cityId": self.user_city_id,
            "user_districtId": self.user_district_id,
            "user_id": "",
        }
        return urlencode(fields)

    def to_variables(self) -> dict[str, Any]:
        """Return the GraphQL ``variables`` object."""
        return {"params": self.to_params()}


class TokopediaSearchShopRequest(BaseModel):
    """Request parameters for ``AceSearchShopQuery`` (shop search)."""

    operation_name: ClassVar[str] = "AceSearchShopQuery"

    keyword: str = Field(..., min_length=1, max_length=500, description="Search query")
    rows: int = Field(default=20, ge=1, le=100)
    start: int = Field(default=0, ge=0)
    user_district_id: str = Field(default="2274", description="Location bias: district ID")
    user_city_id: str = Field(default="176", description="Location bias: city ID")

    def to_params(self) -> str:
        """Encode the request as the ``params`` query-string variable."""
        fields = {
            "q": self.keyword,
            "rows": str(self.rows),
            "start": str(self.start),
            "user_cityId": self.user_city_id,
            "user_districtId": self.user_district_id,
            "user_id": "0",
        }
        return urlencode(fields)

    def to_variables(self) -> dict[str, Any]:
        """Return the GraphQL ``variables`` object."""
        return {"params": self.to_params()}


class TokopediaProductDetailRequest(BaseModel):
    """Request parameters for ``PDPMainInfo`` (product detail page).

    A product is addressed by ``shop_domain`` + ``product_key`` — the two
    path segments of a product URL::

        https://www.tokopedia.com/{shop_domain}/{product_key}
    """

    operation_name: ClassVar[str] = "PDPMainInfo"

    product_key: str = Field(..., min_length=1, description="Product URL slug")
    shop_domain: str = Field(..., min_length=1, description="Shop URL slug")
    district_id: str = Field(default="", description="Location bias: district ID")
    city_id: str = Field(default="", description="Location bias: city ID")
    source: str = Field(default="P1", description="PDP layout source identifier")

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> TokopediaProductDetailRequest:
        """Build a request from a full product URL.

        Raises:
            ValueError: if the URL does not contain ``/{shop}/{product}``.
        """
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        if len(parts) < 2:
            raise ValueError(
                f"Cannot parse product URL {url!r} — expected "
                "https://www.tokopedia.com/<shop-domain>/<product-key>"
            )
        return cls(shop_domain=parts[0], product_key=parts[1], **kwargs)

    def to_variables(self) -> dict[str, Any]:
        """Return the GraphQL ``variables`` object."""
        return {
            "productKey": self.product_key,
            "shopDomain": self.shop_domain,
            "layoutID": "",
            "extraPayload": "",
            "queryParam": "",
            "source": self.source,
            "userLocation": {
                "addressID": "",
                "districtID": self.district_id,
                "postalCode": "",
                "latlon": "",
                "cityID": self.city_id,
            },
        }


class TokopediaProductReviewsRequest(BaseModel):
    """Request parameters for ``productReviewList`` (paginated reviews)."""

    operation_name: ClassVar[str] = "productReviewList"

    product_id: str = Field(..., min_length=1, description="Numeric product ID")
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=10, ge=1, le=50)
    sort_by: str = Field(default="informative_score desc", description="Sort expression")
    filter_by: str = Field(default="", description="Filter expression (e.g. 'rating=5')")

    def to_variables(self) -> dict[str, Any]:
        """Return the GraphQL ``variables`` object."""
        return {
            "productID": self.product_id,
            "page": self.page,
            "limit": self.limit,
            "sortBy": self.sort_by,
            "filterBy": self.filter_by,
        }


# ---------------------------------------------------------------------------
# Document schemas — product search
# ---------------------------------------------------------------------------


class TokopediaPrice(BaseModel):
    """Price block on a search-result product."""

    text: str = Field(default="")
    number: int = Field(default=0, ge=0)
    original: str = Field(default="")
    discount_percentage: int = Field(default=0, alias="discountPercentage")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TokopediaMediaURL(BaseModel):
    """Image/video URLs of a search-result product."""

    image: str = Field(default="")
    image300: str = Field(default="")
    video_custom: str = Field(default="", alias="videoCustom")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TokopediaProductShop(BaseModel):
    """Shop summary embedded in a search-result product."""

    id: str = Field(default="")
    name: str = Field(default="")
    url: str = Field(default="")
    city: str = Field(default="")
    tier: int = Field(default=0)

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TokopediaProduct(BaseModel):
    """A single product as returned by ``SearchProductV5Query``."""

    id: str = Field(..., description="Product ID (string form)")
    name: str = Field(default="")
    url: str = Field(default="")
    applink: str = Field(default="")
    media_url: TokopediaMediaURL = Field(default_factory=TokopediaMediaURL, alias="mediaURL")
    shop: TokopediaProductShop = Field(default_factory=TokopediaProductShop)
    price: TokopediaPrice = Field(default_factory=TokopediaPrice)
    category: dict[str, Any] = Field(default_factory=dict)
    rating: float = Field(default=0.0)
    wishlist: bool = Field(default=False)

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @field_validator("id", mode="before")
    @classmethod
    def coerce_id(cls, v: Any) -> str:
        return str(v)

    @field_validator("rating", mode="before")
    @classmethod
    def coerce_rating(cls, v: Any) -> float:
        if v in (None, ""):
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0


# ---------------------------------------------------------------------------
# Document schemas — shop search
# ---------------------------------------------------------------------------


class TokopediaShopProduct(BaseModel):
    """Mini product preview embedded in a shop search result."""

    id: str = Field(default="")
    name: str = Field(default="")
    url: str = Field(default="")
    price_format: str = Field(default="")
    image_url: str = Field(default="")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TokopediaShop(BaseModel):
    """A single shop as returned by ``AceSearchShopQuery``."""

    shop_id: str = Field(default="")
    shop_name: str = Field(default="")
    shop_domain: str = Field(default="")
    shop_location: str = Field(default="")
    shop_tag_line: str = Field(default="")
    shop_description: str = Field(default="")
    shop_url: str = Field(default="")
    shop_image: str = Field(default="")
    reputation_score: str = Field(default="")
    shop_total_favorite: int = Field(default=0)
    shop_gold_shop: bool = Field(default=False)
    is_official: bool = Field(default=False)
    is_pm_pro: bool = Field(default=False)
    products: list[TokopediaShopProduct] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @field_validator("shop_id", "reputation_score", mode="before")
    @classmethod
    def coerce_str(cls, v: Any) -> str:
        return "" if v is None else str(v)

    @field_validator("products", mode="before")
    @classmethod
    def coerce_products(cls, v: Any) -> Any:
        return [] if v is None else v


# ---------------------------------------------------------------------------
# Document schemas — product detail (PDP)
# ---------------------------------------------------------------------------


class TokopediaTxStats(BaseModel):
    """Transaction statistics from PDP basicInfo."""

    transaction_success: int = Field(default=0, alias="transactionSuccess")
    count_sold: int = Field(default=0, alias="countSold")
    item_sold_fmt: str = Field(default="", alias="itemSoldFmt")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TokopediaProductStats(BaseModel):
    """Engagement statistics from PDP basicInfo."""

    count_view: int = Field(default=0, alias="countView")
    count_review: int = Field(default=0, alias="countReview")
    count_talk: int = Field(default=0, alias="countTalk")
    rating: float = Field(default=0.0)

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @field_validator("rating", mode="before")
    @classmethod
    def coerce_rating(cls, v: Any) -> float:
        if v in (None, ""):
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0


class TokopediaProductDetail(BaseModel):
    """Product detail document built from ``PDPMainInfo``.

    Combines ``basicInfo`` with the name/price/stock blocks that the PDP
    layout serves through its ``ProductHighlight`` component (injected by
    the API client before validation).
    """

    id: str = Field(..., description="Product ID")
    alias: str = Field(default="")
    name: str = Field(default="", description="From the ProductHighlight component")
    url: str = Field(default="")
    shop_id: str = Field(default="", alias="shopID")
    shop_name: str = Field(default="", alias="shopName")
    min_order: int = Field(default=1, alias="minOrder")
    max_order: int = Field(default=0, alias="maxOrder")
    weight: int = Field(default=0)
    weight_unit: str = Field(default="", alias="weightUnit")
    condition: int = Field(default=0)
    status: str = Field(default="")
    default_media_url: str = Field(default="", alias="defaultMediaURL")
    category: dict[str, Any] = Field(default_factory=dict)
    tx_stats: TokopediaTxStats = Field(default_factory=TokopediaTxStats, alias="txStats")
    stats: TokopediaProductStats = Field(default_factory=TokopediaProductStats)
    price: dict[str, Any] = Field(
        default_factory=dict, description="From the ProductHighlight component"
    )
    stock: dict[str, Any] = Field(
        default_factory=dict, description="From the ProductHighlight component"
    )
    media: list[dict[str, Any]] = Field(
        default_factory=list, description="From the ProductMedia component"
    )

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @field_validator("id", "shop_id", mode="before")
    @classmethod
    def coerce_str(cls, v: Any) -> str:
        return "" if v is None else str(v)


# ---------------------------------------------------------------------------
# Document schemas — product reviews
# ---------------------------------------------------------------------------


class TokopediaReviewer(BaseModel):
    """Review author."""

    user_id: str = Field(default="", alias="userID")
    full_name: str = Field(default="", alias="fullName")
    image: str = Field(default="")
    url: str = Field(default="")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TokopediaReviewResponse(BaseModel):
    """Seller reply attached to a review."""

    message: str = Field(default="")
    create_time: str = Field(default="", alias="createTime")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TokopediaReview(BaseModel):
    """A single product review as returned by ``productReviewList``."""

    id: str = Field(..., description="Feedback ID")
    product_id: str = Field(default="", description="Injected from the request context")
    message: str = Field(default="")
    rating: int = Field(default=0, alias="productRating", ge=0, le=5)
    variant_name: str = Field(default="", alias="variantName")
    review_time: str = Field(default="", alias="reviewCreateTime")
    review_timestamp: str = Field(default="", alias="reviewCreateTimestamp")
    is_anonymous: bool = Field(default=False, alias="isAnonymous")
    user: TokopediaReviewer = Field(default_factory=TokopediaReviewer)
    response: TokopediaReviewResponse = Field(
        default_factory=TokopediaReviewResponse, alias="reviewResponse"
    )
    total_like: int = Field(default=0)
    image_attachments: list[dict[str, Any]] = Field(
        default_factory=list, alias="imageAttachments"
    )
    video_attachments: list[dict[str, Any]] = Field(
        default_factory=list, alias="videoAttachments"
    )
    bad_rating_reason: str = Field(default="", alias="badRatingReasonFmt")

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @field_validator("id", mode="before")
    @classmethod
    def coerce_id(cls, v: Any) -> str:
        return str(v)

    @field_validator("response", mode="before")
    @classmethod
    def coerce_response(cls, v: Any) -> Any:
        return TokopediaReviewResponse() if v is None else v

    @field_validator("image_attachments", "video_attachments", mode="before")
    @classmethod
    def coerce_attachments(cls, v: Any) -> Any:
        return [] if v is None else v

    @model_validator(mode="before")
    @classmethod
    def flatten_like_dislike(cls, data: Any) -> Any:
        """Lift ``likeDislike.totalLike`` into the flat ``total_like`` field."""
        if isinstance(data, dict):
            like = data.get("likeDislike")
            if isinstance(like, dict) and "total_like" not in data:
                data = {**data, "total_like": like.get("totalLike", 0)}
        return data


# ---------------------------------------------------------------------------
# Kafka event envelope
# ---------------------------------------------------------------------------

TokopediaDocument = Union[
    TokopediaProduct,
    TokopediaShop,
    TokopediaProductDetail,
    TokopediaReview,
]


class KafkaEvent(BaseModel):
    """Standardised event envelope for Kafka messages."""

    event_id: str = Field(default_factory=lambda: uuid4().hex)
    event_type: str = Field(default="tokopedia.document.scraped")
    source: str = Field(default="tokopedia-crawler")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    payload: TokopediaDocument = Field(...)
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
