"""Tests for the pipeline orchestrator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from src.config import Settings
from src.db.writer import MockAsyncDBWriter
from src.pipeline import MetadataPipeline
from src.qc.reporter import QCReporter


@pytest.fixture
def pipeline(settings: Settings, mock_writer: MockAsyncDBWriter) -> MetadataPipeline:
    return MetadataPipeline(settings=settings, writer=mock_writer)


class TestPipeline:
    @pytest.mark.asyncio
    async def test_run_with_no_data(self, pipeline: MetadataPipeline) -> None:
        """Pipeline runs without errors when there's nothing to process."""
        result = await pipeline.run(geo=False, sra=False)
        assert result["mappings"] == 0

    @pytest.mark.asyncio
    async def test_mock_writer_injected(self, pipeline: MetadataPipeline) -> None:
        writer = await pipeline._get_writer()
        assert isinstance(writer, MockAsyncDBWriter)

    @pytest.mark.asyncio
    async def test_write_sra_package(
        self, pipeline: MetadataPipeline, mock_writer: MockAsyncDBWriter
    ) -> None:
        pipeline._mapper = __import__(
            "src.linkers.id_mapper", fromlist=["IDMapper"]
        ).IDMapper(mock_writer)

        pkg = {
            "study": {"accession": "SRP1", "title": "Test", "external_ids": {}},
            "sample": {"accession": "SRS1", "title": "S1", "external_ids": {}},
            "experiment": {
                "accession": "SRX1",
                "study_ref": "SRP1",
                "sample_ref": "SRS1",
            },
            "runs": [
                {"accession": "SRR1", "experiment_ref": "SRX1"},
            ],
        }
        await pipeline._write_sra_package(mock_writer, pkg)
        assert mock_writer.stats.get("sra_studies", 0) == 1
        assert mock_writer.stats.get("sra_samples", 0) == 1
        assert mock_writer.stats.get("sra_experiments", 0) == 1
        assert mock_writer.stats.get("sra_runs", 0) == 1

    @pytest.mark.asyncio
    async def test_qc_reporter_records(self, pipeline: MetadataPipeline) -> None:
        pipeline.qc.record_row("sra_studies", {"accession": "SRP1", "title": ""})
        pipeline.qc.record_row("sra_studies", {"accession": "SRP2", "title": "Has title"})
        summary = pipeline.qc.summary()
        assert summary["tables"]["sra_studies"]["total_rows"] == 2
        assert summary["tables"]["sra_studies"]["null_counts"]["title"] == 1


class TestQCReporter:
    def test_record_and_summary(self) -> None:
        qc = QCReporter()
        qc.record_row("test_table", {"a": "val", "b": None, "c": ""})
        qc.record_row("test_table", {"a": "", "b": "val", "c": "val"})
        s = qc.summary()
        assert s["tables"]["test_table"]["total_rows"] == 2
        assert s["tables"]["test_table"]["null_counts"]["a"] == 1
        assert s["tables"]["test_table"]["null_counts"]["b"] == 1
        assert s["tables"]["test_table"]["null_counts"]["c"] == 1

    def test_error_recording(self) -> None:
        qc = QCReporter()
        qc.record_error("test", "something broke", accession="X1")
        s = qc.summary()
        assert s["total_errors"] == 1
        assert s["errors_sample"][0]["source"] == "test"

    def test_save_report(self, tmp_path: Path) -> None:
        qc = QCReporter()
        qc.record_row("t", {"a": "1"})
        p = qc.save_report(tmp_path / "report.json")
        assert p.exists()
        import json
        data = json.loads(p.read_text())
        assert "tables" in data

    def test_print_summary(self) -> None:
        qc = QCReporter()
        qc.record_row("t", {"a": None})
        text = qc.print_summary()
        assert "QC REPORT" in text
