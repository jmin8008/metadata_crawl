"""Cross-reference ID mapper.

Extracts mappings from parsed metadata (GEO relations, SRA external_ids)
and supplements with E-utilities elink queries.

Mapping types:
    GEO_Series  <-> SRA_Study  (SRP)
    GEO_Series  <-> BioProject (PRJNA)
    GEO_Series  <-> PubMed
    GEO_Sample  <-> SRA_Sample (SRS)
    SRA_Sample  <-> BioSample  (SAMN)
    SRA_Study   <-> BioProject
    SRA_Experiment -> SRA_Study, SRA_Sample
    SRA_Run     -> SRA_Experiment
"""

from __future__ import annotations

import logging
import re
from typing import Any

from src.db.models import IDMapping
from src.db.writer import AsyncDBWriter, MockAsyncDBWriter
from src.downloaders.http_downloader import HTTPDownloader

logger = logging.getLogger(__name__)

# Regex patterns for accession extraction
_PATTERNS = {
    "SRP": re.compile(r"(SRP\d+)"),
    "SRS": re.compile(r"(SRS\d+)"),
    "SRX": re.compile(r"(SRX\d+)"),
    "SRR": re.compile(r"(SRR\d+)"),
    "GSE": re.compile(r"(GSE\d+)"),
    "GSM": re.compile(r"(GSM\d+)"),
    "GPL": re.compile(r"(GPL\d+)"),
    "PRJNA": re.compile(r"(PRJNA\d+)"),
    "PRJEB": re.compile(r"(PRJEB\d+)"),
    "PRJDB": re.compile(r"(PRJDB\d+)"),
    "SAMN": re.compile(r"(SAMN\d+)"),
    "SAME": re.compile(r"(SAME\d+)"),
}


class IDMapper:
    """Extracts and stores cross-reference mappings."""

    def __init__(self, writer: AsyncDBWriter | MockAsyncDBWriter) -> None:
        self._writer = writer
        self._seen: set[tuple[str, str, str, str]] = set()

    async def add_mapping(
        self,
        source_db: str,
        source_id: str,
        target_db: str,
        target_id: str,
        link_type: str = "parsed",
    ) -> None:
        key = (source_db, source_id, target_db, target_id)
        if key in self._seen:
            return
        self._seen.add(key)
        await self._writer.write(
            "id_mappings",
            {
                "source_db": source_db,
                "source_id": source_id,
                "target_db": target_db,
                "target_id": target_id,
                "link_type": link_type,
            },
        )

    # ── Extract from parsed GEO data ─────────────────────────

    async def extract_geo_series_links(self, series: dict[str, Any]) -> None:
        acc = series.get("accession", "")
        if not acc:
            return

        # Relations often contain SRA, BioProject links
        for rtype, target in series.get("relations", {}).items():
            if "SRA" in rtype.upper():
                m = _PATTERNS["SRP"].search(target)
                if m:
                    await self.add_mapping("GEO", acc, "SRA", m.group(1))
            if "BioProject" in rtype:
                m = _PATTERNS["PRJNA"].search(target) or _PATTERNS["PRJEB"].search(target)
                if m:
                    await self.add_mapping("GEO", acc, "BioProject", m.group(1))

        # PubMed IDs
        for pmid in series.get("pubmed_ids", []):
            await self.add_mapping("GEO", acc, "PubMed", str(pmid))

    async def extract_geo_sample_links(self, sample: dict[str, Any]) -> None:
        acc = sample.get("accession", "")
        if not acc:
            return

        for rtype, target in sample.get("relations", {}).items():
            if "SRA" in rtype.upper():
                m = _PATTERNS["SRS"].search(target) or _PATTERNS["SRX"].search(target)
                if m:
                    await self.add_mapping("GEO", acc, "SRA", m.group(1))
            if "BioSample" in rtype:
                m = _PATTERNS["SAMN"].search(target) or _PATTERNS["SAME"].search(target)
                if m:
                    await self.add_mapping("GEO", acc, "BioSample", m.group(1))

        for series_ref in sample.get("series_refs", sample.get("series_ref", [])):
            if series_ref:
                await self.add_mapping("GEO_Sample", acc, "GEO_Series", series_ref)

    # ── Extract from parsed SRA data ─────────────────────────

    async def extract_sra_links(self, package: dict[str, Any]) -> None:
        study = package.get("study", {})
        sample = package.get("sample", {})
        experiment = package.get("experiment", {})
        runs = package.get("runs", [])

        study_acc = study.get("accession", "")
        sample_acc = sample.get("accession", "")
        exp_acc = experiment.get("accession", "")

        # Study external IDs
        for ns, xid in study.get("external_ids", {}).items():
            if "BioProject" in ns:
                await self.add_mapping("SRA", study_acc, "BioProject", xid)
            elif "GEO" in ns:
                await self.add_mapping("SRA", study_acc, "GEO", xid)
            elif "pubmed" in ns.lower():
                await self.add_mapping("SRA", study_acc, "PubMed", xid)

        # Sample external IDs
        for ns, xid in sample.get("external_ids", {}).items():
            if "BioSample" in ns:
                await self.add_mapping("SRA_Sample", sample_acc, "BioSample", xid)

        # Experiment -> Study, Sample refs
        if exp_acc:
            study_ref = experiment.get("study_ref", "")
            sample_ref = experiment.get("sample_ref", "")
            if study_ref:
                await self.add_mapping("SRA_Experiment", exp_acc, "SRA_Study", study_ref)
            if sample_ref:
                await self.add_mapping("SRA_Experiment", exp_acc, "SRA_Sample", sample_ref)

        # Runs -> Experiment
        for run in runs:
            run_acc = run.get("accession", "")
            if run_acc and exp_acc:
                await self.add_mapping("SRA_Run", run_acc, "SRA_Experiment", exp_acc)

    # ── E-utilities elink supplement ──────────────────────────

    async def elink_supplement(
        self,
        http: HTTPDownloader,
        source_db: str,
        target_db: str,
        ids: list[str],
    ) -> None:
        """Use NCBI ELink to discover additional cross-references."""
        if not ids:
            return
        try:
            link_results = await http.elink(
                dbfrom=source_db, db=target_db, ids=ids
            )
            for lr in link_results:
                from_id = lr.get("from_id", "")
                for to_id in lr.get("to_ids", []):
                    await self.add_mapping(
                        source_db, from_id, target_db, to_id, link_type="elink"
                    )
        except Exception:
            logger.exception("ELink failed: %s->%s for %d ids", source_db, target_db, len(ids))

    @property
    def mapping_count(self) -> int:
        return len(self._seen)
