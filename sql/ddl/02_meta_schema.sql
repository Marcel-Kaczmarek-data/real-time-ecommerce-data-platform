-- Ingestion control plane: watermarks, file registry and pipeline run log.
-- Kept in PostgreSQL (transactional, row-level updates) rather than Delta.

CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.ingestion_watermarks (
  source_name       TEXT PRIMARY KEY,
  watermark_column  TEXT NOT NULL,
  watermark_value   TIMESTAMPTZ NOT NULL,
  pending_value     TIMESTAMPTZ,
  rows_last_run     BIGINT NOT NULL DEFAULT 0,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.file_registry (
  file_id           BIGSERIAL PRIMARY KEY,
  source_name       TEXT NOT NULL,
  object_key        TEXT NOT NULL,
  checksum_sha256   CHAR(64) NOT NULL,
  size_bytes        BIGINT NOT NULL,
  file_format       TEXT NOT NULL,
  status            TEXT NOT NULL CHECK (status IN ('DISCOVERED', 'PROCESSING', 'LOADED', 'FAILED', 'DUPLICATE')),
  rows_loaded       BIGINT,
  rows_quarantined  BIGINT,
  error_message     TEXT,
  batch_id          TEXT,
  discovered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at      TIMESTAMPTZ,
  UNIQUE (source_name, object_key)
);
CREATE INDEX IF NOT EXISTS ix_file_registry_status ON meta.file_registry (source_name, status);
CREATE INDEX IF NOT EXISTS ix_file_registry_checksum ON meta.file_registry (source_name, checksum_sha256);

CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
  run_id            TEXT PRIMARY KEY,
  pipeline_name     TEXT NOT NULL,
  status            TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
  started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at       TIMESTAMPTZ,
  rows_in           BIGINT,
  rows_out          BIGINT,
  rows_rejected     BIGINT,
  details           JSONB,
  error_message     TEXT
);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_name ON meta.pipeline_runs (pipeline_name, started_at DESC);

-- Serving schema for Power BI (populated by pipelines/gold/publish_serving.py).
CREATE SCHEMA IF NOT EXISTS analytics;
