"""Tests for DB models, schema DDL, and mock writer."""

from __future__ import annotations

import json

import pytest

from src.db.models import (
    GEOSeries,
    GEOSample,
    GEOPlatform,
    SRAStudy,
    SRASample,
    SRAExperiment,
    SRARun,
    BioSample,
    BioProject,
    IDMapping,
)
from src.db.schema import get_ddl, DDL
from src.db.writer import MockAsyncDBWriter, SQLiteWriter, _prepare_record


# ── Models ────────────────────────────────────────────────────


class TestModels:
    def test_geo_series_defaults(self) -> None:
        s = GEOSeries(accession="GSE1")
        assert s.title == ""
        assert s.contributors == []
        assert s.pubmed_ids == []
        assert s.extra == {}

    def test_sra_experiment_fields(self) -> None:
        e = SRAExperiment(
            accession="SRX1",
            strategy="RNA-Seq",
            layout="PAIRED",
            instrument_model="NovaSeq 6000",
        )
        assert e.strategy == "RNA-Seq"
        assert e.layout == "PAIRED"

    def test_sra_run_optional_ints(self) -> None:
        r = SRARun(accession="SRR1")
        assert r.total_spots is None
        assert r.total_bases is None

    def test_id_mapping(self) -> None:
        m = IDMapping(
            source_db="GEO",
            source_id="GSE1",
            target_db="SRA",
            target_id="SRP1",
            link_type="parsed",
        )
        assert m.source_db == "GEO"

    def test_biosample(self) -> None:
        bs = BioSample(accession="SAMN1", taxon_id="9606", organism="Homo sapiens")
        assert bs.attributes == {}

    def test_bioproject(self) -> None:
        bp = BioProject(accession="PRJNA1", title="Test project")
        assert bp.sra_study_ref == ""


# ── Schema DDL ────────────────────────────────────────────────


class TestSchema:
    def test_ddl_contains_all_tables(self) -> None:
        ddl = get_ddl()
        required_tables = [
            "geo_series",
            "geo_samples",
            "geo_platforms",
            "sra_studies",
            "sra_samples",
            "sra_experiments",
            "sra_runs",
            "biosamples",
            "bioprojects",
            "id_mappings",
            "pipeline_checkpoints",
            "qc_reports",
        ]
        for table in required_tables:
            assert f"CREATE TABLE IF NOT EXISTS {table}" in ddl, f"Missing table: {table}"

    def test_ddl_has_jsonb_columns(self) -> None:
        ddl = get_ddl()
        assert "JSONB" in ddl

    def test_ddl_has_indexes(self) -> None:
        ddl = get_ddl()
        assert "CREATE INDEX" in ddl

    def test_ddl_has_unique_constraints(self) -> None:
        ddl = get_ddl()
        assert "UNIQUE" in ddl


# ── Mock Writer ───────────────────────────────────────────────


class TestMockWriter:
    @pytest.mark.asyncio
    async def test_write_and_retrieve(self) -> None:
        w = MockAsyncDBWriter()
        await w.write("geo_series", {"accession": "GSE1", "title": "Test"})
        assert w.stats["geo_series"] == 1
        recs = w.get_records("geo_series")
        assert len(recs) == 1
        assert recs[0]["accession"] == "GSE1"

    @pytest.mark.asyncio
    async def test_multiple_tables(self) -> None:
        w = MockAsyncDBWriter()
        await w.write("geo_series", {"accession": "GSE1"})
        await w.write("sra_studies", {"accession": "SRP1"})
        await w.write("sra_studies", {"accession": "SRP2"})
        assert w.stats["geo_series"] == 1
        assert w.stats["sra_studies"] == 2

    @pytest.mark.asyncio
    async def test_flush_noop(self) -> None:
        w = MockAsyncDBWriter()
        await w.flush()  # should not raise
        await w.flush_all()


# ── Record preparation ────────────────────────────────────────


class TestPrepareRecord:
    def test_jsonb_fields_serialized(self) -> None:
        rec = _prepare_record("geo_series", {
            "accession": "GSE1",
            "title": "Test",
            "contributors": ["Alice", "Bob"],
            "pubmed_ids": ["123"],
            "relations": {"SRA": "SRP1"},
            "supplementary": [],
            "extra": {"note": "x"},
        })
        assert isinstance(rec["contributors"], str)
        assert json.loads(rec["contributors"]) == ["Alice", "Bob"]
        assert isinstance(rec["accession"], str)
        assert rec["accession"] == "GSE1"  # not serialized

    def test_non_jsonb_fields_pass_through(self) -> None:
        rec = _prepare_record("sra_experiments", {
            "accession": "SRX1",
            "strategy": "WGS",
            "extra": {},
        })
        assert rec["strategy"] == "WGS"
        assert isinstance(rec["extra"], str)


# ── SQLite Writer ────────────────────────────────────────────


class TestSQLiteWriter:
    @pytest.mark.asyncio
    async def test_write_and_stats(self, tmp_path) -> None:
        w = SQLiteWriter(tmp_path / "test.sqlite")
        await w.write("geo_series", {"accession": "GSE1", "title": "Test", "contributors": ["A"]})
        await w.write("geo_series", {"accession": "GSE2", "title": "Test2"})
        await w.flush_all()
        assert w.stats["geo_series"] == 2

    @pytest.mark.asyncio
    async def test_upsert_replaces(self, tmp_path) -> None:
        w = SQLiteWriter(tmp_path / "test.sqlite")
        await w.write("sra_studies", {"accession": "SRP1", "title": "Old"})
        await w.write("sra_studies", {"accession": "SRP1", "title": "New"})
        await w.flush_all()
        # Should have replaced, so count is 2 writes but 1 row
        cursor = w._conn.execute("SELECT title FROM sra_studies WHERE accession='SRP1'")
        assert cursor.fetchone()[0] == "New"

    @pytest.mark.asyncio
    async def test_export_to_json(self, tmp_path) -> None:
        w = SQLiteWriter(tmp_path / "test.sqlite")
        await w.write("geo_series", {"accession": "GSE1", "title": "T1", "contributors": ["A", "B"]})
        await w.write("sra_runs", {"accession": "SRR1", "total_spots": 1000})
        await w.flush_all()
        exported = w.export_to_json(tmp_path / "export")
        assert "geo_series" in exported
        assert "sra_runs" in exported
        data = json.loads(exported["geo_series"].read_text())
        assert len(data) == 1
        assert data[0]["accession"] == "GSE1"
        assert data[0]["contributors"] == ["A", "B"]  # parsed back from JSON

    @pytest.mark.asyncio
    async def test_id_mappings(self, tmp_path) -> None:
        w = SQLiteWriter(tmp_path / "test.sqlite")
        await w.write("id_mappings", {
            "source_db": "GEO", "source_id": "GSE1",
            "target_db": "SRA", "target_id": "SRP1", "link_type": "parsed"
        })
        await w.flush_all()
        cursor = w._conn.execute("SELECT * FROM id_mappings")
        rows = cursor.fetchall()
        assert len(rows) == 1

    def test_close(self, tmp_path) -> None:
        w = SQLiteWriter(tmp_path / "test.sqlite")
        w.close()
        assert (tmp_path / "test.sqlite").exists()
