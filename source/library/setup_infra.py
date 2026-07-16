#!/usr/bin/env python3
"""
Infrastructure setup — create Kafka topics & Elasticsearch indices (fully async).

Usage:
    python library/setup_infra.py                  # create default topic + index
    python library/setup_infra.py --dry-run        # show what would be created
    python library/setup_infra.py --delete         # delete and recreate
    python library/setup_infra.py --health         # connectivity check only

Reads configuration from ``config.yaml`` (or TOKOPEDIA_* env vars) via
``library.config.settings``.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError, TopicAlreadyExistsError
from elasticsearch import AsyncElasticsearch

from library.config import settings

logger = logging.getLogger("setup_infra")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)


def _es_client() -> AsyncElasticsearch:
    """Build an AsyncElasticsearch client from settings."""
    es = settings.elasticsearch
    return AsyncElasticsearch(
        hosts=es.hosts,
        api_key=es.api_key,
        basic_auth=(es.username, es.password) if es.username and es.password else None,
        request_timeout=es.request_timeout,
    )


# ============================================================================
# Kafka
# ============================================================================

async def create_kafka_topic(
    topic: str | None = None,
    num_partitions: int = 3,
    replication_factor: int = 1,
    dry_run: bool = False,
    delete_first: bool = False,
) -> bool:
    """Create a Kafka topic.

    Args:
        topic: Topic name (defaults to ``settings.kafka.topic``).
        num_partitions: Number of partitions.
        replication_factor: Replication factor.
        dry_run: If True, only print what would be done.
        delete_first: If True, delete the topic before recreating.

    Returns:
        True on success.
    """
    topic = topic or settings.kafka.topic
    broker = settings.kafka.bootstrap_servers

    logger.info("Connecting to Kafka broker: %s", broker)

    admin = AIOKafkaAdminClient(bootstrap_servers=broker, client_id="setup-infra")
    try:
        await admin.start()
    except KafkaError as e:
        logger.error("Failed to connect to Kafka: %s", e)
        return False

    try:
        existing = await admin.list_topics()
        logger.info("Existing topics: %s", existing)

        if topic in existing:
            if delete_first:
                logger.info("Deleting existing topic: %s", topic)
                if not dry_run:
                    await admin.delete_topics([topic])
                    await asyncio.sleep(1)
            else:
                logger.info("Topic '%s' already exists — skipping", topic)
                return True

        if dry_run:
            logger.info("[DRY-RUN] Would create topic: %s (partitions=%d, rf=%d)",
                        topic, num_partitions, replication_factor)
            return True

        try:
            new_topic = NewTopic(
                name=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            await admin.create_topics([new_topic])
            logger.info("[OK] Kafka topic created: %s (partitions=%d)", topic, num_partitions)
        except TopicAlreadyExistsError:
            logger.info("Topic '%s' already exists", topic)
            # Try to increase partitions if current count is less than target
            try:
                desc = await admin.describe_topics([topic])
                current_partitions = len(desc[0]["partitions"])
                if current_partitions < num_partitions:
                    await admin.create_partitions({topic: num_partitions})
                    logger.info("Topic '%s' partitions increased: %d -> %d",
                                topic, current_partitions, num_partitions)
            except Exception:
                pass  # partition increase is best-effort, not critical
        except KafkaError as e:
            logger.error("Failed to create topic: %s", e)
            return False

        return True
    finally:
        await admin.close()


# ============================================================================
# Elasticsearch
# ============================================================================

ES_INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            # product / product-detail documents
            "id": {"type": "keyword"},
            "name": {"type": "text", "analyzer": "standard"},
            "url": {"type": "keyword"},
            "rating": {"type": "float"},
            "price": {
                "properties": {
                    "text": {"type": "keyword"},
                    "number": {"type": "long"},
                    "discountPercentage": {"type": "integer"},
                }
            },
            "shop": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text"},
                    "city": {"type": "keyword"},
                    "tier": {"type": "integer"},
                }
            },
            "category": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "breadcrumb": {"type": "keyword"},
                }
            },
            # product-reviews documents
            "product_id": {"type": "keyword"},
            "message": {"type": "text"},
            "productRating": {"type": "integer"},
            "reviewCreateTimestamp": {"type": "keyword"},
            # shop documents
            "shop_id": {"type": "keyword"},
            "shop_name": {"type": "text"},
            "shop_location": {"type": "keyword"},
            "shop_domain": {"type": "keyword"},
        }
    },
}


async def create_elasticsearch_index(
    index: str | None = None,
    dry_run: bool = False,
    delete_first: bool = False,
) -> bool:
    """Create an Elasticsearch index with optimised mappings.

    Args:
        index: Index name (defaults to ``settings.elasticsearch.index_name``).
        dry_run: If True, only print what would be done.
        delete_first: If True, delete the index before recreating.

    Returns:
        True on success.
    """
    index = index or settings.elasticsearch.index_name

    logger.info("Connecting to Elasticsearch: %s", settings.elasticsearch.hosts)

    es = _es_client()
    try:
        try:
            info = await es.info()
            logger.info("ES cluster: %s (version %s)",
                        info["cluster_name"], info["version"]["number"])
        except Exception as e:
            logger.error("Failed to connect to Elasticsearch: %s", e)
            return False

        exists = await es.indices.exists(index=index)
        if exists:
            if delete_first:
                logger.info("Deleting existing index: %s", index)
                if not dry_run:
                    await es.indices.delete(index=index)
            else:
                logger.info("Index '%s' already exists — skipping", index)
                return True

        if dry_run:
            logger.info("[DRY-RUN] Would create index: %s", index)
            return True

        try:
            await es.indices.create(
                index=index,
                settings=ES_INDEX_MAPPING["settings"],
                mappings=ES_INDEX_MAPPING["mappings"],
            )
            logger.info("[OK] Elasticsearch index created: %s", index)
        except Exception as e:
            logger.error("Failed to create index: %s", e)
            return False

        return True
    finally:
        await es.close()


# ============================================================================
# Health check
# ============================================================================

async def health_check() -> dict[str, str]:
    """Quick connectivity check for Kafka + Elasticsearch."""
    status: dict[str, str] = {"kafka": "...", "elasticsearch": "..."}

    # Kafka
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        client_id="health-check",
        request_timeout_ms=5000,
    )
    try:
        await admin.start()
        topics = await admin.list_topics()
        status["kafka"] = f"[OK] connected ({len(topics)} topics)"
    except Exception as e:
        status["kafka"] = f"[FAIL] {e}"
    finally:
        try:
            await admin.close()
        except Exception:
            pass

    # Elasticsearch
    es = _es_client()
    try:
        info = await es.info()
        status["elasticsearch"] = f"[OK] connected (v{info['version']['number']})"
    except Exception as e:
        status["elasticsearch"] = f"[FAIL] {e}"
    finally:
        await es.close()

    return status


# ============================================================================
# CLI
# ============================================================================

async def _main(args: argparse.Namespace) -> int:
    """Async entry point for the CLI."""
    if args.health:
        report = await health_check()
        for svc, stat in report.items():
            print(f"  {svc}: {stat}")
        return 0

    print("=" * 60)
    print("Tokopedia Crawler — Infrastructure Setup")
    print("=" * 60)

    # Health check first
    report = await health_check()
    for svc, stat in report.items():
        print(f"  {svc}: {stat}")

    if "[FAIL]" in report["kafka"] or "[FAIL]" in report["elasticsearch"]:
        logger.error("One or more services are unreachable. Aborting.")
        return 1

    print()

    ok_kafka = await create_kafka_topic(
        topic=args.topic,
        dry_run=args.dry_run,
        delete_first=args.delete,
    )
    ok_es = await create_elasticsearch_index(
        index=args.index,
        dry_run=args.dry_run,
        delete_first=args.delete,
    )

    print()
    if ok_kafka and ok_es:
        print("[OK] All infrastructure ready.")
        return 0
    print("⚠ Some steps failed — check logs above.")
    return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Kafka topics & Elasticsearch indices")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--delete", action="store_true", help="Delete and recreate")
    parser.add_argument("--topic", type=str, default=None, help="Kafka topic name")
    parser.add_argument("--index", type=str, default=None, help="ES index name")
    parser.add_argument("--health", action="store_true", help="Only health check")
    args = parser.parse_args()

    sys.exit(asyncio.run(_main(args)))
