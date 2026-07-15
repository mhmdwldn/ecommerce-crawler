-- Asset Registry (control plane) — PRD_50
-- Jalankan di Postgres yang sama dengan mart, schema terpisah.

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.crawl_assets (
    asset_id             BIGSERIAL PRIMARY KEY,
    platform             TEXT        NOT NULL DEFAULT 'tokopedia',
    crawl_type           TEXT        NOT NULL
        CHECK (crawl_type IN ('search-product','search-shop','product-detail','product-reviews')),
    payload              JSONB       NOT NULL,
    label                TEXT,                                  -- nama manusiawi, mis. "POCO F8"
    category             TEXT,                                  -- elektronik | fashion | ...
    priority             SMALLINT    NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 9),
    cadence_min          INT         NOT NULL DEFAULT 60 CHECK (cadence_min >= 15),
    is_active            BOOLEAN     NOT NULL DEFAULT true,
    last_crawled_at      TIMESTAMPTZ,                           -- UTC (keputusan #6)
    last_status          TEXT CHECK (last_status IN ('success','failed','blocked')),
    consecutive_failures SMALLINT    NOT NULL DEFAULT 0,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- satu target unik per platform+type+payload (bikin seed idempotent)
    CONSTRAINT uq_asset UNIQUE (platform, crawl_type, payload)
);

CREATE INDEX IF NOT EXISTS idx_assets_due
    ON control.crawl_assets (is_active, last_crawled_at, priority);

-- auto-update updated_at
CREATE OR REPLACE FUNCTION control.touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_touch_assets ON control.crawl_assets;
CREATE TRIGGER trg_touch_assets
    BEFORE UPDATE ON control.crawl_assets
    FOR EACH ROW EXECUTE FUNCTION control.touch_updated_at();

-- View: asset yang layak di-crawl sekarang (aturan "due" — PRD_50)
CREATE OR REPLACE VIEW control.v_due_assets AS
SELECT *
FROM control.crawl_assets
WHERE is_active
  AND (last_crawled_at IS NULL
       OR last_crawled_at < now() - (cadence_min || ' minutes')::interval)
ORDER BY priority ASC, last_crawled_at ASC NULLS FIRST;
