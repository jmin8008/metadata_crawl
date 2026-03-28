"""Tests for SRA XML, GEO MINiML, and GEO SOFT parsers."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.parsers.sra_xml_parser import iter_experiment_packages
from src.parsers.geo_miniml_parser import parse_miniml_file, parse_miniml_bytes
from src.parsers.geo_soft_parser import parse_soft_file, iter_soft_samples
from tests.conftest import SAMPLE_MINIML_XML


# ── SRA XML Parser ────────────────────────────────────────────


class TestSRAXMLParser:
    def test_iter_packages_basic(self, sra_xml_path: Path) -> None:
        packages = list(iter_experiment_packages(sra_xml_path))
        assert len(packages) == 1

    def test_study_parsed(self, sra_xml_path: Path) -> None:
        pkg = next(iter_experiment_packages(sra_xml_path))
        study = pkg["study"]
        assert study["accession"] == "SRP000001"
        assert study["title"] == "Human Liver Transcriptome"
        assert study["abstract"] == "A study of the liver transcriptome."
        assert study["study_type"] == "Transcriptome Analysis"
        assert study["external_ids"].get("BioProject") == "PRJNA000001"

    def test_sample_parsed(self, sra_xml_path: Path) -> None:
        pkg = next(iter_experiment_packages(sra_xml_path))
        sample = pkg["sample"]
        assert sample["accession"] == "SRS000001"
        assert sample["taxon_id"] == "9606"
        assert sample["scientific_name"] == "Homo sapiens"
        assert sample["attributes"]["tissue"] == "liver"
        assert sample["attributes"]["sex"] == "male"
        assert sample["external_ids"]["BioSample"] == "SAMN00000001"

    def test_experiment_parsed(self, sra_xml_path: Path) -> None:
        pkg = next(iter_experiment_packages(sra_xml_path))
        exp = pkg["experiment"]
        assert exp["accession"] == "SRX000001"
        assert exp["strategy"] == "RNA-Seq"
        assert exp["source"] == "TRANSCRIPTOMIC"
        assert exp["selection"] == "cDNA"
        assert exp["layout"] == "PAIRED"
        assert exp["instrument_model"] == "Illumina HiSeq 2500"
        assert exp["study_ref"] == "SRP000001"
        assert exp["sample_ref"] == "SRS000001"

    def test_run_parsed(self, sra_xml_path: Path) -> None:
        pkg = next(iter_experiment_packages(sra_xml_path))
        runs = pkg["runs"]
        assert len(runs) == 1
        run = runs[0]
        assert run["accession"] == "SRR000001"
        assert run["total_spots"] == 10_000_000
        assert run["total_bases"] == 2_000_000_000
        assert run["size"] == 500_000_000
        assert run["avg_length"] == 200
        assert run["experiment_ref"] == "SRX000001"
        assert len(run["sra_files"]) == 1


# ── GEO MINiML Parser ────────────────────────────────────────


class TestGEOMINiMLParser:
    def test_parse_miniml_file(self, miniml_xml_path: Path) -> None:
        data = parse_miniml_file(miniml_xml_path)
        assert len(data["series"]) == 1
        assert len(data["samples"]) == 1
        assert len(data["platforms"]) == 1

    def test_series_fields(self, miniml_xml_path: Path) -> None:
        data = parse_miniml_file(miniml_xml_path)
        s = data["series"][0]
        assert s["accession"] == "GSE12345"
        assert s["title"] == "Test Series Title"
        assert "test summary" in s["summary"].lower()
        assert "Case-control" in s["overall_design"]
        assert "12345678" in s["pubmed_ids"]
        assert s["submission_date"] == "2024-01-15"
        assert "SRA" in s["relations"]
        assert "BioProject" in s["relations"]

    def test_sample_fields(self, miniml_xml_path: Path) -> None:
        data = parse_miniml_file(miniml_xml_path)
        sam = data["samples"][0]
        assert sam["accession"] == "GSM100001"
        assert sam["organism"] == "Homo sapiens"
        assert sam["taxid"] == "9606"
        assert sam["characteristics"]["tissue"] == "blood"
        assert sam["characteristics"]["disease state"] == "normal"
        assert "TRIzol" in sam["extract_protocol"]
        assert sam["molecule"] == "total RNA"

    def test_platform_fields(self, miniml_xml_path: Path) -> None:
        data = parse_miniml_file(miniml_xml_path)
        p = data["platforms"][0]
        assert p["accession"] == "GPL16791"
        assert p["manufacturer"] == "Illumina"
        assert p["organism"] == "Homo sapiens"
        assert p["taxid"] == "9606"

    def test_parse_bytes(self) -> None:
        data = parse_miniml_bytes(SAMPLE_MINIML_XML.encode())
        assert len(data["series"]) == 1

    def test_contributor_parsed(self, miniml_xml_path: Path) -> None:
        data = parse_miniml_file(miniml_xml_path)
        s = data["series"][0]
        assert "John Doe" in s["contributors"]


# ── GEO SOFT Parser ──────────────────────────────────────────


class TestGEOSOFTParser:
    def test_parse_soft(self, soft_file_path: Path) -> None:
        data = parse_soft_file(soft_file_path)
        assert "GSE99999" in data["series"]
        assert "GSM200001" in data["samples"]
        assert "GPL570" in data["platforms"]

    def test_series_fields(self, soft_file_path: Path) -> None:
        data = parse_soft_file(soft_file_path)
        s = data["series"]["GSE99999"]
        assert s["series_title"] == "Soft Test Series"
        assert "SOFT format test" in s["series_summary"]

    def test_sample_characteristics(self, soft_file_path: Path) -> None:
        data = parse_soft_file(soft_file_path)
        sam = data["samples"]["GSM200001"]
        assert sam["sample_title"] == "SOFT Sample 1"
        # Characteristics are repeated keys → list
        chars = sam["sample_characteristics_ch1"]
        assert isinstance(chars, list)
        assert "tissue: brain" in chars
        assert "age: 45" in chars

    def test_iter_soft_samples(self, soft_file_path: Path) -> None:
        samples = list(iter_soft_samples(soft_file_path))
        assert len(samples) == 1
        assert samples[0]["accession"] == "GSM200001"

    def test_platform_fields(self, soft_file_path: Path) -> None:
        data = parse_soft_file(soft_file_path)
        p = data["platforms"]["GPL570"]
        assert p["platform_manufacturer"] == "Affymetrix"
