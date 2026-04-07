"""Async batch writer for PostgreSQL using psycopg (v3) async API.

Provides an ``AsyncDBWriter`` that buffers records and flushes them
in batch UPSERT statements.  When PostgreSQL is unavailable, a
``SQLiteWriter`` provides a real persistent fallback that stores
data to a local SQLite file.  ``MockAsyncDBWriter`` is for tests only.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional, Sequence

from src.config import Settings
from src.db.models import (
    BioProject,
    BioSample,
    GEOPlatform,
    GEOSample,
    GEOSeries,
    IDMapping,
    SRAExperiment,
    SRARun,
    SRASample,
    SRAStudy,
)

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────

def _json(v: Any) -> str:
    """Serialize a value to JSON string for JSONB columns."""
    if isinstance(v, str):
        return v
    return json.dumps(v, ensure_ascii=False, default=str)


# ── SQL templates ─────────────────────────────────────────────

_UPSERT_SQL: dict[str, str] = {
    "geo_series": """
        INSERT INTO geo_series (accession, title, summary, overall_design,
            experiment_type, contributors, pubmed_ids, submission_date,
            last_update_date, release_date, relations, supplementary, extra)
        VALUES (%(accession)s, %(title)s, %(summary)s, %(overall_design)s,
            %(experiment_type)s, %(contributors)s, %(pubmed_ids)s,
            %(submission_date)s, %(last_update_date)s, %(release_date)s,
            %(relations)s, %(supplementary)s, %(extra)s)
        ON CONFLICT (accession) DO UPDATE SET
            title=EXCLUDED.title, summary=EXCLUDED.summary,
            overall_design=EXCLUDED.overall_design,
            experiment_type=EXCLUDED.experiment_type,
            contributors=EXCLUDED.contributors,
            pubmed_ids=EXCLUDED.pubmed_ids,
            submission_date=EXCLUDED.submission_date,
            last_update_date=EXCLUDED.last_update_date,
            release_date=EXCLUDED.release_date,
            relations=EXCLUDED.relations,
            supplementary=EXCLUDED.supplementary,
            extra=EXCLUDED.extra,
            updated_at=now()
    """,
    "geo_samples": """
        INSERT INTO geo_samples (accession, title, sample_type, source_name,
            organism, taxid, characteristics, treatment_protocol,
            extract_protocol, label, molecule, platform_ref, series_refs,
            relations, extra)
        VALUES (%(accession)s, %(title)s, %(sample_type)s, %(source_name)s,
            %(organism)s, %(taxid)s, %(characteristics)s,
            %(treatment_protocol)s, %(extract_protocol)s, %(label)s,
            %(molecule)s, %(platform_ref)s, %(series_refs)s,
            %(relations)s, %(extra)s)
        ON CONFLICT (accession) DO UPDATE SET
            title=EXCLUDED.title, sample_type=EXCLUDED.sample_type,
            source_name=EXCLUDED.source_name, organism=EXCLUDED.organism,
            taxid=EXCLUDED.taxid, characteristics=EXCLUDED.characteristics,
            treatment_protocol=EXCLUDED.treatment_protocol,
            extract_protocol=EXCLUDED.extract_protocol,
            label=EXCLUDED.label, molecule=EXCLUDED.molecule,
            platform_ref=EXCLUDED.platform_ref,
            series_refs=EXCLUDED.series_refs,
            relations=EXCLUDED.relations, extra=EXCLUDED.extra,
            updated_at=now()
    """,
    "geo_platforms": """
        INSERT INTO geo_platforms (accession, title, technology, distribution,
            organism, taxid, manufacturer, manufacture_protocol, extra)
        VALUES (%(accession)s, %(title)s, %(technology)s, %(distribution)s,
            %(organism)s, %(taxid)s, %(manufacturer)s,
            %(manufacture_protocol)s, %(extra)s)
        ON CONFLICT (accession) DO UPDATE SET
            title=EXCLUDED.title, technology=EXCLUDED.technology,
            distribution=EXCLUDED.distribution, organism=EXCLUDED.organism,
            taxid=EXCLUDED.taxid, manufacturer=EXCLUDED.manufacturer,
            manufacture_protocol=EXCLUDED.manufacture_protocol,
            extra=EXCLUDED.extra, updated_at=now()
    """,
    "sra_studies": """
        INSERT INTO sra_studies (accession, alias, center_name, title,
            abstract, study_type, external_ids, extra)
        VALUES (%(accession)s, %(alias)s, %(center_name)s, %(title)s,
            %(abstract)s, %(study_type)s, %(external_ids)s, %(extra)s)
        ON CONFLICT (accession) DO UPDATE SET
            alias=EXCLUDED.alias, center_name=EXCLUDED.center_name,
            title=EXCLUDED.title, abstract=EXCLUDED.abstract,
            study_type=EXCLUDED.study_type, external_ids=EXCLUDED.external_ids,
            extra=EXCLUDED.extra, updated_at=now()
    """,
    "sra_samples": """
        INSERT INTO sra_samples (accession, alias, title, taxon_id,
            scientific_name, attributes, external_ids, extra)
        VALUES (%(accession)s, %(alias)s, %(title)s, %(taxon_id)s,
            %(scientific_name)s, %(attributes)s, %(external_ids)s, %(extra)s)
        ON CONFLICT (accession) DO UPDATE SET
            alias=EXCLUDED.alias, title=EXCLUDED.title,
            taxon_id=EXCLUDED.taxon_id,
            scientific_name=EXCLUDED.scientific_name,
            attributes=EXCLUDED.attributes,
            external_ids=EXCLUDED.external_ids,
            extra=EXCLUDED.extra, updated_at=now()
    """,
    "sra_experiments": """
        INSERT INTO sra_experiments (accession, alias, title, study_ref,
            sample_ref, strategy, source, selection, layout,
            instrument_model, extra)
        VALUES (%(accession)s, %(alias)s, %(title)s, %(study_ref)s,
            %(sample_ref)s, %(strategy)s, %(source)s, %(selection)s,
            %(layout)s, %(instrument_model)s, %(extra)s)
        ON CONFLICT (accession) DO UPDATE SET
            alias=EXCLUDED.alias, title=EXCLUDED.title,
            study_ref=EXCLUDED.study_ref, sample_ref=EXCLUDED.sample_ref,
            strategy=EXCLUDED.strategy, source=EXCLUDED.source,
            selection=EXCLUDED.selection, layout=EXCLUDED.layout,
            instrument_model=EXCLUDED.instrument_model,
            extra=EXCLUDED.extra, updated_at=now()
    """,
    "sra_runs": """
        INSERT INTO sra_runs (accession, alias, experiment_ref, total_spots,
            total_bases, size_bytes, avg_length, sra_files, extra)
        VALUES (%(accession)s, %(alias)s, %(experiment_ref)s,
            %(total_spots)s, %(total_bases)s, %(size)s,
            %(avg_length)s, %(sra_files)s, %(extra)s)
        ON CONFLICT (accession) DO UPDATE SET
            alias=EXCLUDED.alias, experiment_ref=EXCLUDED.experiment_ref,
            total_spots=EXCLUDED.total_spots, total_bases=EXCLUDED.total_bases,
            size_bytes=EXCLUDED.size_bytes, avg_length=EXCLUDED.avg_length,
            sra_files=EXCLUDED.sra_files, extra=EXCLUDED.extra,
            updated_at=now()
    """,
    "biosamples": """
        INSERT INTO biosamples (accession, taxon_id, organism, attributes,
            sra_sample_ref, extra)
        VALUES (%(accession)s, %(taxon_id)s, %(organism)s, %(attributes)s,
            %(sra_sample_ref)s, %(extra)s)
        ON CONFLICT (accession) DO UPDATE SET
            taxon_id=EXCLUDED.taxon_id, organism=EXCLUDED.organism,
            attributes=EXCLUDED.attributes,
            sra_sample_ref=EXCLUDED.sra_sample_ref,
            extra=EXCLUDED.extra, updated_at=now()
    """,
    "bioprojects": """
        INSERT INTO bioprojects (accession, title, description,
            sra_study_ref, geo_series_ref, extra)
        VALUES (%(accession)s, %(title)s, %(description)s,
            %(sra_study_ref)s, %(geo_series_ref)s, %(extra)s)
        ON CONFLICT (accession) DO UPDATE SET
            title=EXCLUDED.title, description=EXCLUDED.description,
            sra_study_ref=EXCLUDED.sra_study_ref,
            geo_series_ref=EXCLUDED.geo_series_ref,
            extra=EXCLUDED.extra, updated_at=now()
    """,
    "id_mappings": """
        INSERT INTO id_mappings (source_db, source_id, target_db, target_id, link_type)
        VALUES (%(source_db)s, %(source_id)s, %(target_db)s, %(target_id)s, %(link_type)s)
        ON CONFLICT (source_db, source_id, target_db, target_id) DO NOTHING
    """,
}


# ── Async DB Writer ───────────────────────────────────────────


class AsyncDBWriter:
    """Buffered async batch writer for PostgreSQL."""

    def __init__(self, pool, batch_size: int = 500) -> None:
        self._pool = pool
        self._batch_size = batch_size
        self._buffers: dict[str, list[dict[str, Any]]] = {k: [] for k in _UPSERT_SQL}
        self._total_written: dict[str, int] = {k: 0 for k in _UPSERT_SQL}

    async def write(self, table: str, record: dict[str, Any]) -> None:
        """Buffer a record; auto-flush when batch_size is reached."""
        if table not in self._buffers:
            raise ValueError(f"Unknown table: {table}")
        # Serialize JSONB fields
        rec = _prepare_record(table, record)
        self._buffers[table].append(rec)
        if len(self._buffers[table]) >= self._batch_size:
            await self.flush(table)

    async def flush(self, table: Optional[str] = None) -> None:
        """Flush one or all table buffers to the database."""
        tables = [table] if table else list(self._buffers.keys())
        for t in tables:
            buf = self._buffers[t]
            if not buf:
                continue
            sql = _UPSERT_SQL[t]
            async with self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    for rec in buf:
                        await cur.execute(sql, rec)
                await conn.commit()
            self._total_written[t] += len(buf)
            logger.debug("Flushed %d records to %s", len(buf), t)
            self._buffers[t] = []

    async def flush_all(self) -> None:
        await self.flush()

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._total_written)


# ── Mock writer for testing ───────────────────────────────────


class MockAsyncDBWriter:
    """In-memory mock that mimics AsyncDBWriter for tests."""

    def __init__(self) -> None:
        self._buffers: dict[str, list[dict[str, Any]]] = {}
        self._total_written: dict[str, int] = {}

    async def write(self, table: str, record: dict[str, Any]) -> None:
        self._buffers.setdefault(table, []).append(record)
        self._total_written[table] = self._total_written.get(table, 0) + 1

    async def flush(self, table: Optional[str] = None) -> None:
        pass  # no-op for mock

    async def flush_all(self) -> None:
        pass

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._total_written)

    def get_records(self, table: str) -> list[dict[str, Any]]:
        return self._buffers.get(table, [])


# ── SQLite Writer (persistent local fallback) ─────────────────

_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS geo_series (
    accession TEXT PRIMARY KEY,
    title TEXT, summary TEXT, overall_design TEXT, experiment_type TEXT,
    contributors TEXT DEFAULT '[]', pubmed_ids TEXT DEFAULT '[]',
    submission_date TEXT, last_update_date TEXT, release_date TEXT,
    relations TEXT DEFAULT '{}', supplementary TEXT DEFAULT '[]',
    extra TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS geo_samples (
    accession TEXT PRIMARY KEY,
    title TEXT, sample_type TEXT, source_name TEXT, organism TEXT, taxid TEXT,
    characteristics TEXT DEFAULT '{}', treatment_protocol TEXT, extract_protocol TEXT,
    label TEXT, molecule TEXT, platform_ref TEXT, series_refs TEXT DEFAULT '[]',
    relations TEXT DEFAULT '{}', extra TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS geo_platforms (
    accession TEXT PRIMARY KEY,
    title TEXT, technology TEXT, distribution TEXT, organism TEXT, taxid TEXT,
    manufacturer TEXT, manufacture_protocol TEXT, extra TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS sra_studies (
    accession TEXT PRIMARY KEY,
    alias TEXT, center_name TEXT, title TEXT, abstract TEXT, study_type TEXT,
    external_ids TEXT DEFAULT '{}', extra TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS sra_samples (
    accession TEXT PRIMARY KEY,
    alias TEXT, title TEXT, taxon_id TEXT, scientific_name TEXT,
    attributes TEXT DEFAULT '{}', external_ids TEXT DEFAULT '{}', extra TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS sra_experiments (
    accession TEXT PRIMARY KEY,
    alias TEXT, title TEXT, study_ref TEXT, sample_ref TEXT,
    strategy TEXT, source TEXT, selection TEXT, layout TEXT, instrument_model TEXT,
    extra TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS sra_runs (
    accession TEXT PRIMARY KEY,
    alias TEXT, experiment_ref TEXT, total_spots INTEGER, total_bases INTEGER,
    size_bytes INTEGER, avg_length INTEGER, sra_files TEXT DEFAULT '[]',
    extra TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS biosamples (
    accession TEXT PRIMARY KEY,
    taxon_id TEXT, organism TEXT, attributes TEXT DEFAULT '{}',
    sra_sample_ref TEXT, extra TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS bioprojects (
    accession TEXT PRIMARY KEY,
    title TEXT, description TEXT, sra_study_ref TEXT, geo_series_ref TEXT,
    extra TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS id_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_db TEXT NOT NULL, source_id TEXT NOT NULL,
    target_db TEXT NOT NULL, target_id TEXT NOT NULL,
    link_type TEXT DEFAULT 'parsed',
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE (source_db, source_id, target_db, target_id)
);
CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL, last_accession TEXT,
    total_processed INTEGER DEFAULT 0, total_errors INTEGER DEFAULT 0,
    metadata TEXT DEFAULT '{}', updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS qc_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date TEXT DEFAULT (datetime('now')), table_name TEXT NOT NULL,
    total_rows INTEGER, null_counts TEXT DEFAULT '{}',
    error_count INTEGER DEFAULT 0, details TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now'))
);
"""

# Column names per table for SQLite INSERT (excluding auto-generated)
_SQLITE_COLUMNS: dict[str, list[str]] = {
    "geo_series": ["accession", "title", "summary", "overall_design", "experiment_type",
                    "contributors", "pubmed_ids", "submission_date", "last_update_date",
                    "release_date", "relations", "supplementary", "extra"],
    "geo_samples": ["accession", "title", "sample_type", "source_name", "organism", "taxid",
                     "characteristics", "treatment_protocol", "extract_protocol", "label",
                     "molecule", "platform_ref", "series_refs", "relations", "extra"],
    "geo_platforms": ["accession", "title", "technology", "distribution", "organism", "taxid",
                       "manufacturer", "manufacture_protocol", "extra"],
    "sra_studies": ["accession", "alias", "center_name", "title", "abstract", "study_type",
                     "external_ids", "extra"],
    "sra_samples": ["accession", "alias", "title", "taxon_id", "scientific_name",
                     "attributes", "external_ids", "extra"],
    "sra_experiments": ["accession", "alias", "title", "study_ref", "sample_ref",
                         "strategy", "source", "selection", "layout", "instrument_model", "extra"],
    "sra_runs": ["accession", "alias", "experiment_ref", "total_spots", "total_bases",
                  "size_bytes", "avg_length", "sra_files", "extra"],
    "biosamples": ["accession", "taxon_id", "organism", "attributes", "sra_sample_ref", "extra"],
    "bioprojects": ["accession", "title", "description", "sra_study_ref", "geo_series_ref", "extra"],
    "id_mappings": ["source_db", "source_id", "target_db", "target_id", "link_type"],
}


class SQLiteWriter:
    """Persistent local writer using SQLite. No PostgreSQL required.

    Stores data in a single .sqlite file with the same schema structure.
    JSON fields are stored as TEXT (JSON strings).
    """

    def __init__(self, db_path: str | Path = "data/metadata_crawl.sqlite") -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.executescript(_SQLITE_DDL)
        self._conn.commit()
        self._total_written: dict[str, int] = {}
        logger.info("SQLite database opened at %s", self._db_path)

    async def write(self, table: str, record: dict[str, Any]) -> None:
        """Insert or replace a record."""
        columns = _SQLITE_COLUMNS.get(table)
        if columns is None:
            raise ValueError(f"Unknown table: {table}")

        values = []
        for col in columns:
            v = record.get(col)
            # Serialize dicts/lists to JSON strings for TEXT columns
            if isinstance(v, (dict, list)):
                v = json.dumps(v, ensure_ascii=False, default=str)
            values.append(v)

        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(columns)
        sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"

        self._conn.execute(sql, values)
        self._total_written[table] = self._total_written.get(table, 0) + 1

        # Auto-commit every 500 records
        if self._total_written[table] % 500 == 0:
            self._conn.commit()

    async def flush(self, table: Optional[str] = None) -> None:
        self._conn.commit()

    async def flush_all(self) -> None:
        self._conn.commit()

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._total_written)

    def close(self) -> None:
        self._conn.commit()
        self._conn.close()
        logger.info("SQLite closed. Stats: %s", self._total_written)

    def export_to_json(self, output_dir: str | Path = "data/export") -> dict[str, Path]:
        """Export all tables to JSON files for easy inspection."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        exported = {}
        for table in _SQLITE_COLUMNS:
            cursor = self._conn.execute(f"SELECT * FROM {table}")
            col_names = [desc[0] for desc in cursor.description]
            rows = []
            for row in cursor:
                row_dict = dict(zip(col_names, row))
                # Parse JSON strings back to objects
                for k, v in row_dict.items():
                    if isinstance(v, str) and v.startswith(("{", "[")):
                        try:
                            row_dict[k] = json.loads(v)
                        except json.JSONDecodeError:
                            pass
                rows.append(row_dict)
            if rows:
                out_path = output_dir / f"{table}.json"
                out_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                exported[table] = out_path
                logger.info("Exported %d rows from %s to %s", len(rows), table, out_path)
        return exported


# ── Record preparation ────────────────────────────────────────

_JSONB_FIELDS = {
    "geo_series": {"contributors", "pubmed_ids", "relations", "supplementary", "extra"},
    "geo_samples": {"characteristics", "series_refs", "relations", "extra"},
    "geo_platforms": {"extra"},
    "sra_studies": {"external_ids", "extra"},
    "sra_samples": {"attributes", "external_ids", "extra"},
    "sra_experiments": {"extra"},
    "sra_runs": {"sra_files", "extra"},
    "biosamples": {"attributes", "extra"},
    "bioprojects": {"extra"},
    "id_mappings": set(),
}


def _prepare_record(table: str, rec: dict[str, Any]) -> dict[str, Any]:
    """Convert JSONB fields to JSON strings."""
    jsonb_fields = _JSONB_FIELDS.get(table, set())
    out = {}
    for k, v in rec.items():
        if k in jsonb_fields:
            out[k] = _json(v)
        else:
            out[k] = v
    return out
