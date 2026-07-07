#!/usr/bin/env python3
"""
E-Commerce End-to-End Crawler — CLI Entry Point
===============================================

Follows the template-crawler pattern: argparse CLI -> Controllers -> Input/Output drivers.
Multi-marketplace: pick the storefront with ``--platform`` (tokopedia | shopee).

Usage:
    # Tokopedia — scrape products to stdout
    python main.py crawler --platform tokopedia --mode scrape --type search-product --keyword "poco f8"

    # Tokopedia — product detail by URL
    python main.py crawler --platform tokopedia --mode scrape --type product-detail \\
        --url "https://www.tokopedia.com/xiaomi/poco-f8-pro"

    # Shopee — keyword search (needs SHOPEE_COOKIES / SHOPEE_EXTRA_HEADERS in env)
    python main.py crawler --platform shopee --mode scrape --type search-product --keyword "sepatu lari pria"

    # Shopee — category listing
    python main.py crawler --platform shopee --mode scrape --type search-product --match-id 11044364

    # Tokopedia — search + publish to Kafka
    python main.py crawler --platform tokopedia --mode full --type search-product --keyword "poco f8" \\
        -d kafka -o tokopedia.products.raw --bootstrap-servers localhost:9092
"""

import argparse
import asyncio
import logging
import sys

# Controller registry: platform -> { --type value: (module path, class name, required args) }
CONTROLLER_REGISTRY: dict[str, dict[str, tuple[str, str, list[str]]]] = {
    "tokopedia": {
        "search-product": ("controllers.tokopedia.search_product", "TokopediaSearchProduct", ["keyword"]),
        "search-shop": ("controllers.tokopedia.search_shop", "TokopediaSearchShop", ["keyword"]),
        "product-detail": ("controllers.tokopedia.product_detail", "TokopediaProductDetail", []),
        "product-reviews": ("controllers.tokopedia.product_reviews", "TokopediaProductReviews", ["product_id"]),
    },
    "shopee": {
        "search-product": ("controllers.shopee.search_product", "ShopeeSearchProduct", []),
    },
}

# Union of every --type across platforms (argparse choices); per-platform
# validity is checked in resolve_controller.
ALL_TYPES = sorted({t for types in CONTROLLER_REGISTRY.values() for t in types})


def resolve_controller(args: argparse.Namespace, log: logging.Logger):
    """Import and validate the controller class for ``--platform`` + ``--type``."""
    platform_types = CONTROLLER_REGISTRY.get(args.platform)
    if platform_types is None:
        log.error("Unknown --platform %s. Use: %s", args.platform, ", ".join(CONTROLLER_REGISTRY))
        sys.exit(1)

    entry = platform_types.get(args.type)
    if entry is None:
        log.error("--type %s is not supported for --platform %s. Available: %s",
                  args.type, args.platform, ", ".join(platform_types))
        sys.exit(1)

    module_path, class_name, required = entry

    for field in required:
        if not getattr(args, field, None):
            log.error("--%s is required for --platform %s --type %s",
                      field.replace("_", "-"), args.platform, args.type)
            sys.exit(1)

    # Per-type cross-field validation
    if args.platform == "tokopedia" and args.type == "product-detail" and not (
        args.url or (args.product_key and args.shop_domain)
    ):
        log.error("--type product-detail requires --url or --product-key + --shop-domain")
        sys.exit(1)

    if args.platform == "shopee" and args.type == "search-product" and not (
        args.keyword or args.match_id
    ):
        log.error("Shopee search-product requires --keyword or --match-id")
        sys.exit(1)

    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)


if __name__ == "__main__":
    argp = argparse.ArgumentParser(
        description="E-Commerce End-to-End Crawler (Tokopedia / Shopee)",
    )

    argp.add_argument("-c", "--config", dest="config", type=str, default="config.yaml")
    argp.add_argument("-s", "--source", dest="source", type=str, default=None)
    argp.add_argument("-d", "--destination", dest="destination", type=str, default=None)
    argp.add_argument("-i", "--input", dest="input", type=str, default=None)
    argp.add_argument("-o", "--output", dest="output", type=str, default=None)

    # Kafka
    argp.add_argument("--bootstrap-servers", dest="bootstrap_servers", type=str, default=None)
    # Elasticsearch
    argp.add_argument("--elasticsearch-hosts", dest="elasticsearch_hosts", type=str, default=None)

    # --- Subcommands ---
    argp_sub = argp.add_subparsers(title="action", dest="which", help="-h / --help to see usage")

    argp_crawler = argp_sub.add_parser("crawler", help="Run the crawler")
    argp_crawler.add_argument("--platform", dest="platform", type=str, default="tokopedia",
                              choices=list(CONTROLLER_REGISTRY),
                              help="Marketplace: tokopedia | shopee")
    argp_crawler.add_argument("--mode", dest="mode", type=str, default="scrape",
                              choices=["scrape", "full"],
                              help="scrape: JSON only | full: crawl + output driver")
    argp_crawler.add_argument("--type", dest="type", type=str, default="search-product",
                              choices=ALL_TYPES,
                              help="search-product | search-shop | product-detail | product-reviews "
                                   "(availability depends on --platform)")
    argp_crawler.add_argument("--keyword", dest="keyword", type=str, default=None,
                              help="Search keyword (for search-product / search-shop)")
    argp_crawler.add_argument("--match-id", dest="match_id", type=str, default=None,
                              help="Shopee category ID (for --platform shopee --type search-product)")
    argp_crawler.add_argument("--url", dest="url", type=str, default=None,
                              help="Product URL (for product-detail)")
    argp_crawler.add_argument("--product-key", dest="product_key", type=str, default=None,
                              help="Product URL slug (for product-detail)")
    argp_crawler.add_argument("--shop-domain", dest="shop_domain", type=str, default=None,
                              help="Shop URL slug (for product-detail)")
    argp_crawler.add_argument("--product-id", dest="product_id", type=str, default=None,
                              help="Numeric product ID (for product-reviews)")
    argp_crawler.add_argument("--rows", dest="rows", type=int, default=None,
                              help="Results per page (search types)")
    argp_crawler.add_argument("--limit", dest="limit", type=int, default=None,
                              help="Page size: Tokopedia reviews per page, or Shopee results per page")
    argp_crawler.add_argument("--max-pages", dest="max_pages", type=int, default=1)
    argp_crawler.add_argument("--sort-by", dest="sort_by", type=str, default=None,
                              help="Review sort expression (product-reviews)")
    argp_crawler.add_argument("--filter-by", dest="filter_by", type=str, default=None,
                              help="Review filter expression (product-reviews)")
    argp_crawler.add_argument("--cookies", dest="cookies", type=str, default=None,
                              help="Cookie string for session-bound endpoints")
    argp_crawler.add_argument("-o", "--output", dest="output_file", type=str, default=None,
                              help="Save JSON to file (scrape mode) or output destination name (full mode)")
    argp_crawler.add_argument("--pretty", action="store_true", default=False,
                              help="Pretty-print JSON output")
    argp_crawler.add_argument("--log-level", dest="log_level", type=str, default="INFO")
    # Output-driver flags (duplicated from parent so they work after 'crawler' too)
    argp_crawler.add_argument("-d", "--destination", dest="destination_crawler", type=str, default=None,
                              help="Output driver: kafka | elasticsearch | file | std")
    argp_crawler.add_argument("--bootstrap-servers", dest="bootstrap_servers_crawler", type=str, default=None,
                              help="Kafka broker list")
    argp_crawler.add_argument("--elasticsearch-hosts", dest="elasticsearch_hosts_crawler", type=str, default=None,
                              help="ES host URL")

    args = argp.parse_args()

    # Merge: crawler subparser values take precedence over parent parser defaults
    if getattr(args, "bootstrap_servers_crawler", None):
        args.bootstrap_servers = args.bootstrap_servers_crawler
    if getattr(args, "elasticsearch_hosts_crawler", None):
        args.elasticsearch_hosts = args.elasticsearch_hosts_crawler
    if getattr(args, "destination_crawler", None):
        args.destination = args.destination_crawler

    # --- Setup logging ---
    log_level = getattr(args, "log_level", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("main")

    if args.which != "crawler":
        argp.print_help()
        sys.exit(1)

    controller_cls = resolve_controller(args, log)
    job = {
        key: getattr(args, key)
        for key in ("keyword", "url", "product_key", "shop_domain", "product_id", "match_id")
        if getattr(args, key, None)
    }

    # ================================================================
    # Mode: scrape (no output driver — just JSON to stdout/file)
    # ================================================================
    if args.mode == "scrape":
        import json as _json
        import os as _os

        output_path = args.output_file or args.output
        indent = 2 if args.pretty else None

        ctl = controller_cls(**vars(args))
        docs = asyncio.run(ctl.scrape_to_json(job))

        if output_path:
            _os.makedirs(_os.path.dirname(output_path) or ".", exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                _json.dump(docs, f, ensure_ascii=False, indent=indent, default=str)
            log.info("Saved %d documents -> %s", len(docs), output_path)
            print(f"Saved {len(docs)} documents to {output_path}")
        else:
            text = _json.dumps(docs, ensure_ascii=False, indent=indent, default=str)
            try:
                print(text)
            except UnicodeEncodeError:
                print(_json.dumps(docs, ensure_ascii=True, indent=indent, default=str))

        log.info("Scraped %d documents (platform=%s type=%s)", len(docs), args.platform, args.type)

    # ================================================================
    # Mode: full (crawl + output driver: Kafka / ES / file / std)
    # ================================================================
    elif args.mode == "full":
        if not args.destination:
            log.error("--destination / -d is required for full mode")
            sys.exit(1)

        # Merge: crawler subparser -o takes precedence over parent -o
        if args.output_file and not args.output:
            args.output = args.output_file

        ctl = controller_cls(**vars(args))
        asyncio.run(ctl.main())
