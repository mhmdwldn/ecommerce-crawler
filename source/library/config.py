"""
Configuration module for the Tokopedia scraper pipeline.

All settings are loaded via Pydantic BaseSettings, supporting:
  - Environment variables (prefixed with TOKOPEDIA_)
  - YAML configuration file
  - .env / dotenv files
  - Direct initialisation overrides

Zero hardcoded values — every tunable is defined here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import YamlConfigSettingsSource


class KafkaSettings(BaseSettings):
    """Apache Kafka connection and producer configuration."""

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka broker addresses",
    )
    topic: str = Field(
        default="tokopedia.products.raw",
        description="Default Kafka topic for scraped Tokopedia documents",
    )
    client_id: str = Field(
        default="tokopedia-crawler",
        description="Kafka client identifier",
    )
    acks: str = Field(
        default="all",
        description="Producer acknowledgment level: 0, 1, or 'all'",
    )
    compression_type: Optional[str] = Field(
        default="gzip",
        description="Compression codec: gzip, snappy, lz4, zstd, or None",
    )
    max_request_size: int = Field(
        default=1_048_576,
        description="Maximum request size in bytes (default 1 MB)",
    )
    linger_ms: int = Field(
        default=10,
        description="Artificial delay in ms to batch outgoing messages",
    )
    request_timeout_ms: int = Field(
        default=30_000,
        description="Kafka producer request timeout in ms",
    )


class ElasticsearchSettings(BaseSettings):
    """Elasticsearch connection and indexing configuration."""

    hosts: list[str] = Field(
        default=["http://localhost:9200"],
        description="List of Elasticsearch node URLs",
    )
    index_name: str = Field(
        default="tokopedia_products",
        description="Default target Elasticsearch index",
    )
    api_key: Optional[str] = Field(
        default=None,
        description="Optional API key for Elasticsearch authentication",
    )
    username: Optional[str] = Field(
        default=None,
        description="Basic-auth username",
    )
    password: Optional[str] = Field(
        default=None,
        description="Basic-auth password",
    )
    request_timeout: int = Field(
        default=30,
        description="ES client request timeout in seconds",
    )
    max_retries: int = Field(
        default=3,
        description="Number of retries on transient failures",
    )


class CrawlerSettings(BaseSettings):
    """Generic crawler HTTP configuration (platform-agnostic)."""

    request_timeout: float = Field(
        default=30.0,
        description="HTTP request timeout in seconds",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum retry attempts on transient HTTP errors",
    )
    retry_backoff: float = Field(
        default=2.0,
        description="Exponential backoff multiplier for retries",
    )
    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/149.0.0.0 Safari/537.36"
        ),
        description="Default User-Agent header for HTTP requests",
    )
    rate_limit_rps: float = Field(
        default=5.0,
        description="Maximum requests per second (per crawler instance)",
    )
    proxy_url: Optional[str] = Field(
        default=None,
        description="Optional HTTP/SOCKS proxy URL",
    )


class TokopediaCrawlerSettings(CrawlerSettings):
    """Tokopedia-specific crawler configuration (GraphQL gateway)."""

    base_url: str = Field(
        default="https://gql.tokopedia.com",
        description="Base URL of the Tokopedia GraphQL gateway",
    )
    site_url: str = Field(
        default="https://www.tokopedia.com",
        description="Public site URL — used for Origin/Referer headers",
    )
    search_product_endpoint: str = Field(
        default="/graphql/SearchProductV5Query",
        description="GraphQL endpoint for product search",
    )
    search_shop_endpoint: str = Field(
        default="/graphql/AceSearchShopQuery",
        description="GraphQL endpoint for shop search",
    )
    product_detail_endpoint: str = Field(
        default="/graphql/PDPMainInfo",
        description="GraphQL endpoint for product detail pages (PDP)",
    )
    product_reviews_endpoint: str = Field(
        default="/graphql/productReviewList",
        description="GraphQL endpoint for product reviews",
    )
    x_version: str = Field(
        default="a3540b9",
        description="Tokopedia frontend build hash sent as the x-version header "
                    "(rotate when Tokopedia ships a new web build)",
    )
    x_source: str = Field(
        default="tokopedia-lite",
        description="x-source header value expected by the lite gateway",
    )
    x_device: str = Field(
        default="desktop",
        description="x-device header value",
    )
    lite_service: str = Field(
        default="zeus",
        description="x-tkpd-lite-service header value",
    )
    device_id: str = Field(
        default="",
        description="bd-device-id header value; leave empty to omit the header",
    )
    cookies: str = Field(
        default="",
        description="Optional cookie string for session-bound endpoints "
                    "(e.g. _SID_Tokopedia_=...; bm_sz=...)",
    )
    unique_id: str = Field(
        default="",
        description="Non-login UUID used in search params; "
                    "leave empty to generate a random one per session",
    )
    user_district_id: str = Field(
        default="2274",
        description="user_districtId search param — location bias for results",
    )
    user_city_id: str = Field(
        default="176",
        description="user_cityId search param — location bias for results",
    )
    default_rows: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Default number of results per page",
    )


class ShopeeCrawlerSettings(CrawlerSettings):
    """Shopee-specific crawler configuration (v4 REST API).

    Lives in its own env namespace (``SHOPEE_*``) since this module hosts
    multiple e-commerce crawlers side by side.
    """

    model_config = SettingsConfigDict(
        env_prefix="SHOPEE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    base_url: str = Field(
        default="https://shopee.co.id",
        description="Shopee site / API base URL",
    )
    search_endpoint: str = Field(
        default="/api/v4/search/search_items",
        description="Product search endpoint path",
    )
    language: str = Field(
        default="id",
        description="x-shopee-language header value",
    )
    api_source: str = Field(
        default="pc",
        description="x-api-source header value",
    )
    cookies: str = Field(
        default="",
        description="Session cookie string (SPC_F=...; csrftoken=...); "
                    "Shopee usually rejects fully anonymous API calls",
    )
    extra_headers: dict[str, str] = Field(
        default_factory=dict,
        description="Extra request headers (e.g. rotating anti-bot tokens "
                    "like x-sap-sec / af-ac-enc-dat) captured from a browser",
    )
    default_limit: int = Field(
        default=60,
        ge=1,
        le=100,
        description="Default number of results per page",
    )


class Settings(BaseSettings):
    """Root settings aggregating all sub-configurations."""

    model_config = SettingsConfigDict(
        env_prefix="TOKOPEDIA_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        yaml_file="../config.yaml",
        yaml_config_section="tokopedia_crawler",
        case_sensitive=False,
    )

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    elasticsearch: ElasticsearchSettings = Field(default_factory=ElasticsearchSettings)
    crawler: TokopediaCrawlerSettings = Field(default_factory=TokopediaCrawlerSettings)

    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        """Customise the settings source priority.

        Priority (highest first):
          1. Constructor / init kwargs
          2. Environment variables
          3. YAML config file (if present)
          4. .env / dotenv files
          5. File secrets
        """
        yaml_path = cls._resolve_yaml_path(settings_cls)
        sources = [
            init_settings,
            env_settings,
        ]
        if yaml_path and yaml_path.exists():
            section = settings_cls.model_config.get("yaml_config_section")
            sources.append(
                YamlConfigSettingsSource(
                    settings_cls,
                    yaml_file=str(yaml_path),
                    yaml_config_section=section,
                )
            )
        sources.extend([dotenv_settings, file_secret_settings])
        return tuple(sources)

    @staticmethod
    def _resolve_yaml_path(settings_cls: type[BaseSettings]) -> Path | None:
        """Resolve the YAML config path — checks multiple locations."""
        yaml_file = settings_cls.model_config.get("yaml_file", "../config.yaml")
        candidates = [
            Path(yaml_file),                          # relative to CWD
            Path(__file__).resolve().parent.parent.parent / "config.yaml",  # project root
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0]  # return primary path even if missing (will be skipped)


# Singleton settings instances — import these throughout the application.
settings = Settings()
shopee_settings = ShopeeCrawlerSettings()
