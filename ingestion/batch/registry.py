"""Ingestion control plane in PostgreSQL (``meta`` schema).

* Watermarks are two-phase. ``stage_watermark`` records the candidate value
  when an extract is written. ``commit_watermark`` promotes it only after the
  Bronze load succeeded, so a failed load re-extracts the same window.
* The file registry gives duplicate-file prevention (same checksum, same
  source), status tracking and an audit trail per file.
* ``pipeline_runs`` logs every run with row counts and errors.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass
class FileRecord:
    file_id: int
    source_name: str
    object_key: str
    checksum: str
    status: str


class Registry:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    # ------------------------------------------------------------ watermarks
    def get_watermark(self, source: str) -> datetime:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT watermark_value FROM meta.ingestion_watermarks WHERE source_name = %s", (source,)
            )
            row = cur.fetchone()
        return row[0] if row else EPOCH

    def stage_watermark(self, source: str, column: str, value: datetime) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO meta.ingestion_watermarks (source_name, watermark_column, watermark_value, pending_value)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (source_name) DO UPDATE SET pending_value = EXCLUDED.pending_value, updated_at = now()""",
                (source, column, EPOCH, value),
            )
        self.conn.commit()

    def commit_watermark(self, source: str, rows: int) -> datetime | None:
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE meta.ingestion_watermarks
                   SET watermark_value = GREATEST(watermark_value, COALESCE(pending_value, watermark_value)),
                       pending_value = NULL, rows_last_run = %s, updated_at = now()
                   WHERE source_name = %s RETURNING watermark_value""",
                (rows, source),
            )
            row = cur.fetchone()
        self.conn.commit()
        return row[0] if row else None

    # --------------------------------------------------------- file registry
    def known_keys(self, source: str) -> set[str]:
        with self.conn.cursor() as cur:
            # FAILED files are retried when a corrected file is dropped under the same key.
            cur.execute(
                "SELECT object_key FROM meta.file_registry WHERE source_name = %s AND status IN ('LOADED', 'DUPLICATE')",
                (source,),
            )
            return {r[0] for r in cur.fetchall()}

    def register_file(self, source: str, key: str, checksum: str, size: int, fmt: str) -> FileRecord:
        """Insert (or fetch) the registry row; marks duplicates of already-loaded content."""
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT 1 FROM meta.file_registry
                   WHERE source_name = %s AND checksum_sha256 = %s AND status = 'LOADED' AND object_key <> %s""",
                (source, checksum, key),
            )
            duplicate = cur.fetchone() is not None
            cur.execute(
                """INSERT INTO meta.file_registry (source_name, object_key, checksum_sha256, size_bytes, file_format, status)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (source_name, object_key) DO UPDATE SET checksum_sha256 = EXCLUDED.checksum_sha256
                   RETURNING file_id, status""",
                (source, key, checksum, size, fmt, "DUPLICATE" if duplicate else "DISCOVERED"),
            )
            file_id, status = cur.fetchone()
        self.conn.commit()
        return FileRecord(file_id, source, key, checksum, status)

    def set_file_status(
        self,
        file_id: int,
        status: str,
        rows_loaded: int | None = None,
        rows_quarantined: int | None = None,
        error: str | None = None,
        batch_id: str | None = None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE meta.file_registry SET status = %s, rows_loaded = %s, rows_quarantined = %s,
                          error_message = %s, batch_id = COALESCE(%s, batch_id), processed_at = now()
                   WHERE file_id = %s""",
                (status, rows_loaded, rows_quarantined, (error or "")[:4000] or None, batch_id, file_id),
            )
        self.conn.commit()

    # --------------------------------------------------------- pipeline runs
    def start_run(self, run_id: str, pipeline: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO meta.pipeline_runs (run_id, pipeline_name, status) VALUES (%s, %s, 'RUNNING') "
                "ON CONFLICT (run_id) DO UPDATE SET status = 'RUNNING', started_at = now()",
                (run_id, pipeline),
            )
        self.conn.commit()

    def finish_run(
        self,
        run_id: str,
        status: str,
        rows_in: int = 0,
        rows_out: int = 0,
        rows_rejected: int = 0,
        details: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE meta.pipeline_runs SET status = %s, finished_at = now(), rows_in = %s, rows_out = %s,
                          rows_rejected = %s, details = %s, error_message = %s WHERE run_id = %s""",
                (
                    status,
                    rows_in,
                    rows_out,
                    rows_rejected,
                    json.dumps(details or {}, default=str),
                    (error or "")[:4000] or None,
                    run_id,
                ),
            )
        self.conn.commit()
