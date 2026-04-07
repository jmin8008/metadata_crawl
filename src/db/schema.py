"""PostgreSQL DDL for the metadata crawl database.

10+ normalised tables with JSONB for variable attributes.
Provides both the DDL string and a helper to execute it.
"""

from __future__ import annotations

DDL = """
-- ============================================================
-- GEO tables
-- ============================================================

CREATE TABLE IF NOT EXISTS geo_series (
    id              BIGSERIAL PRIMARY KEY,
    accession       VARCHAR(20) NOT NULL UNIQUE,
    title           TEXT,
    summary         TEXT,
    overall_design  TEXT,
    experiment_type VARCHAR(100),
    contributors    JSONB       DEFAULT '[]',
    pubmed_ids      JSONB       DEFAULT '[]',
    submission_date TIMESTAMPTZ,
    last_update_date TIMESTAMPTZ,
    release_date    TIMESTAMPTZ,
    relations       JSONB       DEFAULT '{}',
    supplementary   JSONB       DEFAULT '[]',
    extra           JSONB       DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS geo_samples (
    id                  BIGSERIAL PRIMARY KEY,
    accession           VARCHAR(20) NOT NULL UNIQUE,
    title               TEXT,
    sample_type         VARCHAR(100),
    source_name         VARCHAR(500),
    organism            VARCHAR(200),
    taxid               VARCHAR(20),
    characteristics     JSONB       DEFAULT '{}',
    treatment_protocol  TEXT,
    extract_protocol    TEXT,
    label               VARCHAR(100),
    molecule            VARCHAR(100),
    platform_ref        VARCHAR(20),
    series_refs         JSONB       DEFAULT '[]',
    relations           JSONB       DEFAULT '{}',
    extra               JSONB       DEFAULT '{}',
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS geo_platforms (
    id                   BIGSERIAL PRIMARY KEY,
    accession            VARCHAR(20) NOT NULL UNIQUE,
    title                TEXT,
    technology           VARCHAR(100),
    distribution         VARCHAR(50),
    organism             VARCHAR(200),
    taxid                VARCHAR(20),
    manufacturer         VARCHAR(500),
    manufacture_protocol TEXT,
    extra                JSONB       DEFAULT '{}',
    created_at           TIMESTAMPTZ DEFAULT now(),
    updated_at           TIMESTAMPTZ DEFAULT now()
);

-- ============================================================
-- SRA tables
-- ============================================================

CREATE TABLE IF NOT EXISTS sra_studies (
    id            BIGSERIAL PRIMARY KEY,
    accession     VARCHAR(20) NOT NULL UNIQUE,
    alias         VARCHAR(200),
    center_name   VARCHAR(200),
    title         TEXT,
    abstract      TEXT,
    study_type    VARCHAR(100),
    external_ids  JSONB       DEFAULT '{}',
    extra         JSONB       DEFAULT '{}',
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sra_samples (
    id              BIGSERIAL PRIMARY KEY,
    accession       VARCHAR(20) NOT NULL UNIQUE,
    alias           VARCHAR(200),
    title           TEXT,
    taxon_id        VARCHAR(20),
    scientific_name VARCHAR(200),
    attributes      JSONB       DEFAULT '{}',
    external_ids    JSONB       DEFAULT '{}',
    extra           JSONB       DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sra_experiments (
    id               BIGSERIAL PRIMARY KEY,
    accession        VARCHAR(20) NOT NULL UNIQUE,
    alias            VARCHAR(200),
    title            TEXT,
    study_ref        VARCHAR(20),
    sample_ref       VARCHAR(20),
    strategy         VARCHAR(100),
    source           VARCHAR(100),
    selection        VARCHAR(100),
    layout           VARCHAR(20),
    instrument_model VARCHAR(200),
    extra            JSONB       DEFAULT '{}',
    created_at       TIMESTAMPTZ DEFAULT now(),
    updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sra_runs (
    id              BIGSERIAL PRIMARY KEY,
    accession       VARCHAR(20) NOT NULL UNIQUE,
    alias           VARCHAR(200),
    experiment_ref  VARCHAR(20),
    total_spots     BIGINT,
    total_bases     BIGINT,
    size_bytes      BIGINT,
    avg_length      INTEGER,
    sra_files       JSONB       DEFAULT '[]',
    extra           JSONB       DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- ============================================================
-- Cross-reference tables
-- ============================================================

CREATE TABLE IF NOT EXISTS biosamples (
    id              BIGSERIAL PRIMARY KEY,
    accession       VARCHAR(30) NOT NULL UNIQUE,
    taxon_id        VARCHAR(20),
    organism        VARCHAR(200),
    attributes      JSONB       DEFAULT '{}',
    sra_sample_ref  VARCHAR(20),
    extra           JSONB       DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bioprojects (
    id              BIGSERIAL PRIMARY KEY,
    accession       VARCHAR(30) NOT NULL UNIQUE,
    title           TEXT,
    description     TEXT,
    sra_study_ref   VARCHAR(20),
    geo_series_ref  VARCHAR(20),
    extra           JSONB       DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS id_mappings (
    id          BIGSERIAL PRIMARY KEY,
    source_db   VARCHAR(30) NOT NULL,
    source_id   VARCHAR(50) NOT NULL,
    target_db   VARCHAR(30) NOT NULL,
    target_id   VARCHAR(50) NOT NULL,
    link_type   VARCHAR(30) DEFAULT 'parsed',
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (source_db, source_id, target_db, target_id)
);

-- ============================================================
-- Pipeline state / checkpoint
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    id              BIGSERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(50)  NOT NULL,
    last_accession  VARCHAR(50),
    total_processed BIGINT       DEFAULT 0,
    total_errors    BIGINT       DEFAULT 0,
    metadata        JSONB        DEFAULT '{}',
    updated_at      TIMESTAMPTZ  DEFAULT now()
);

-- ============================================================
-- QC / audit
-- ============================================================

CREATE TABLE IF NOT EXISTS qc_reports (
    id              BIGSERIAL PRIMARY KEY,
    report_date     TIMESTAMPTZ  DEFAULT now(),
    table_name      VARCHAR(50)  NOT NULL,
    total_rows      BIGINT,
    null_counts     JSONB        DEFAULT '{}',
    error_count     BIGINT       DEFAULT 0,
    details         JSONB        DEFAULT '{}',
    created_at      TIMESTAMPTZ  DEFAULT now()
);

-- ============================================================
-- Indexes
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_geo_series_pubmed  ON geo_series USING GIN (pubmed_ids);
CREATE INDEX IF NOT EXISTS idx_geo_samples_chars  ON geo_samples USING GIN (characteristics);
CREATE INDEX IF NOT EXISTS idx_sra_samples_attrs  ON sra_samples USING GIN (attributes);
CREATE INDEX IF NOT EXISTS idx_sra_experiments_study ON sra_experiments (study_ref);
CREATE INDEX IF NOT EXISTS idx_sra_experiments_sample ON sra_experiments (sample_ref);
CREATE INDEX IF NOT EXISTS idx_sra_runs_experiment ON sra_runs (experiment_ref);
CREATE INDEX IF NOT EXISTS idx_id_mappings_source ON id_mappings (source_db, source_id);
CREATE INDEX IF NOT EXISTS idx_id_mappings_target ON id_mappings (target_db, target_id);
CREATE INDEX IF NOT EXISTS idx_biosamples_sra     ON biosamples (sra_sample_ref);
CREATE INDEX IF NOT EXISTS idx_bioprojects_sra    ON bioprojects (sra_study_ref);
CREATE INDEX IF NOT EXISTS idx_bioprojects_geo    ON bioprojects (geo_series_ref);
"""


async def create_schema(conn) -> None:
    """Execute DDL on an async psycopg connection."""
    await conn.execute(DDL)


def get_ddl() -> str:
    """Return the raw DDL string (useful for migrations / scripts)."""
    return DDL
