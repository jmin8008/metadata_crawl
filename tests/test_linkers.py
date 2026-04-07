"""Tests for ID mapper / linker."""

from __future__ import annotations

import pytest

from src.db.writer import MockAsyncDBWriter
from src.linkers.id_mapper import IDMapper


@pytest.fixture
def mapper() -> tuple[IDMapper, MockAsyncDBWriter]:
    w = MockAsyncDBWriter()
    return IDMapper(w), w


class TestIDMapper:
    @pytest.mark.asyncio
    async def test_add_mapping(self, mapper) -> None:
        m, w = mapper
        await m.add_mapping("GEO", "GSE1", "SRA", "SRP1")
        assert m.mapping_count == 1
        recs = w.get_records("id_mappings")
        assert len(recs) == 1
        assert recs[0]["source_id"] == "GSE1"
        assert recs[0]["target_id"] == "SRP1"

    @pytest.mark.asyncio
    async def test_dedup(self, mapper) -> None:
        m, w = mapper
        await m.add_mapping("GEO", "GSE1", "SRA", "SRP1")
        await m.add_mapping("GEO", "GSE1", "SRA", "SRP1")
        assert m.mapping_count == 1

    @pytest.mark.asyncio
    async def test_extract_geo_series_links(self, mapper) -> None:
        m, w = mapper
        series = {
            "accession": "GSE12345",
            "relations": {
                "SRA": "https://www.ncbi.nlm.nih.gov/sra?term=SRP111111",
                "BioProject": "https://www.ncbi.nlm.nih.gov/bioproject/PRJNA111111",
            },
            "pubmed_ids": ["12345678"],
        }
        await m.extract_geo_series_links(series)
        assert m.mapping_count == 3  # SRA + BioProject + PubMed

    @pytest.mark.asyncio
    async def test_extract_geo_sample_links(self, mapper) -> None:
        m, w = mapper
        sample = {
            "accession": "GSM100001",
            "relations": {
                "SRA": "https://www.ncbi.nlm.nih.gov/sra?term=SRX222222",
                "BioSample": "https://www.ncbi.nlm.nih.gov/biosample/SAMN222222",
            },
            "series_refs": ["GSE12345"],
        }
        await m.extract_geo_sample_links(sample)
        assert m.mapping_count == 3

    @pytest.mark.asyncio
    async def test_extract_sra_links(self, mapper) -> None:
        m, w = mapper
        package = {
            "study": {
                "accession": "SRP000001",
                "external_ids": {"BioProject": "PRJNA000001"},
            },
            "sample": {
                "accession": "SRS000001",
                "external_ids": {"BioSample": "SAMN00000001"},
            },
            "experiment": {
                "accession": "SRX000001",
                "study_ref": "SRP000001",
                "sample_ref": "SRS000001",
            },
            "runs": [
                {"accession": "SRR000001"},
            ],
        }
        await m.extract_sra_links(package)
        # BioProject + BioSample + Exp->Study + Exp->Sample + Run->Exp = 5
        assert m.mapping_count == 5

    @pytest.mark.asyncio
    async def test_empty_accession_skipped(self, mapper) -> None:
        m, w = mapper
        await m.extract_geo_series_links({"accession": "", "relations": {}, "pubmed_ids": []})
        assert m.mapping_count == 0
