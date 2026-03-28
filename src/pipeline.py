"""Main orchestration pipeline.

Coordinates:
1. FTP download of SRA XML dump + GEO MINiML files
2. Streaming parse of downloaded XML/SOFT
3. Async batch write to PostgreSQL
4. Cross-reference ID mapping
5. QC reporting

Usage:
    python -m src.pipeline            # run full pipeline
    python -m src.pipeline --geo-only # GEO only
    python -m src.pipeline --sra-only # SRA only
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any, Optional

from src.config import Settings, get_settings
from src.db.schema import DDL
from src.db.writer import AsyncDBWriter, MockAsyncDBWriter, SQLiteWriter
from src.downloaders.ftp_downloader import FTPDownloader
from src.downloaders.http_downloader import HTTPDownloader
from src.linkers.id_mapper import IDMapper
from src.parsers.geo_miniml_parser import parse_miniml_file
from src.parsers.sra_xml_parser import iter_experiment_packages
from src.qc.reporter import QCReporter

logger = logging.getLogger(__name__)


class MetadataPipeline:
    """Top-level pipeline orchestrator."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        writer: Optional[AsyncDBWriter | SQLiteWriter | MockAsyncDBWriter] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._writer = writer  # allow injection for tests
        self.qc = QCReporter()
        self._mapper: Optional[IDMapper] = None

    # ── Public entry points ───────────────────────────────────

    async def run(
        self,
        *,
        geo: bool = True,
        sra: bool = True,
        geo_accessions: Optional[list[str]] = None,
        sra_file: Optional[Path] = None,
    ) -> dict[str, Any]:
        """Execute the full pipeline.

        Parameters
        ----------
        geo : bool
            Whether to process GEO data.
        sra : bool
            Whether to process SRA data.
        geo_accessions : list[str] | None
            If provided, only fetch these GSE accessions. Otherwise, uses FTP bulk.
        """
        writer = await self._get_writer()
        self._mapper = IDMapper(writer)

        results: dict[str, Any] = {"geo": {}, "sra": {}, "mappings": 0}

        if sra:
            results["sra"] = await self._process_sra(writer, sra_file=sra_file)

        if geo:
            results["geo"] = await self._process_geo(writer, geo_accessions)

        # Flush remaining buffers
        await writer.flush_all()

        results["mappings"] = self._mapper.mapping_count

        # QC report
        report_path = self.settings.log_dir / "qc_report.json"
        self.qc.save_report(report_path)
        self.qc.print_summary()

        return results

    # ── SRA processing ────────────────────────────────────────

    async def _process_sra(
        self,
        writer: AsyncDBWriter | SQLiteWriter | MockAsyncDBWriter,
        sra_file: Optional[Path] = None,
    ) -> dict[str, int]:
        """Download SRA XML dump from FTP and stream-parse."""
        logger.info("Starting SRA pipeline")
        stats = {"studies": 0, "samples": 0, "experiments": 0, "runs": 0}

        if sra_file and sra_file.exists():
            xml_path = sra_file
            logger.info("Using provided SRA file: %s", xml_path)
        else:
            # Download
            dl_dir = self.settings.download_dir / "sra"
            dl_dir.mkdir(parents=True, exist_ok=True)

            try:
                ftp = FTPDownloader(self.settings)
                with ftp:
                    xml_path = ftp.download_sra_full_xml(dl_dir)
            except Exception:
                logger.exception("SRA FTP download failed")
                self.qc.record_error("sra_download", "FTP download failed")
                return stats

        # Parse — FTP dumps yield individual entities with _entity_type
        for record in iter_experiment_packages(xml_path):
            try:
                etype = record.pop("_entity_type", None)
                if etype == "study":
                    self.qc.record_row("sra_studies", record)
                    await writer.write("sra_studies", record)
                    stats["studies"] += 1
                elif etype == "sample":
                    self.qc.record_row("sra_samples", record)
                    await writer.write("sra_samples", record)
                    stats["samples"] += 1
                elif etype == "experiment":
                    self.qc.record_row("sra_experiments", record)
                    await writer.write("sra_experiments", record)
                    stats["experiments"] += 1
                    # Extract ID links from experiment
                    if self._mapper and record.get("study_ref"):
                        await self._mapper.add_mapping(
                            "SRA_Experiment", record.get("accession", ""),
                            "SRA_Study", record["study_ref"],
                        )
                    if self._mapper and record.get("sample_ref"):
                        await self._mapper.add_mapping(
                            "SRA_Experiment", record.get("accession", ""),
                            "SRA_Sample", record["sample_ref"],
                        )
                elif etype == "run":
                    self.qc.record_row("sra_runs", record)
                    await writer.write("sra_runs", record)
                    stats["runs"] += 1
                    if self._mapper and record.get("experiment_ref"):
                        await self._mapper.add_mapping(
                            "SRA_Run", record.get("accession", ""),
                            "SRA_Experiment", record["experiment_ref"],
                        )
                else:
                    # Legacy EXPERIMENT_PACKAGE format (from API)
                    await self._write_sra_package(writer, record)
                    stats["studies"] += 1 if record.get("study") else 0
                    stats["samples"] += 1 if record.get("sample") else 0
                    stats["experiments"] += 1 if record.get("experiment") else 0
                    stats["runs"] += len(record.get("runs", []))

                total = sum(stats.values())
                if total % 100_000 == 0 and total > 0:
                    logger.info("SRA progress: %s", stats)

            except Exception:
                logger.exception("Error writing SRA record")
                self.qc.record_error("sra_parse", "Record write failed")

        await writer.flush_all()
        logger.info("SRA pipeline complete: %s", stats)
        return stats

    async def _write_sra_package(
        self,
        writer: AsyncDBWriter | SQLiteWriter | MockAsyncDBWriter,
        pkg: dict[str, Any],
    ) -> None:
        study = pkg.get("study")
        if study and study.get("accession"):
            self.qc.record_row("sra_studies", study)
            await writer.write("sra_studies", study)

        sample = pkg.get("sample")
        if sample and sample.get("accession"):
            self.qc.record_row("sra_samples", sample)
            await writer.write("sra_samples", sample)

        exp = pkg.get("experiment")
        if exp and exp.get("accession"):
            self.qc.record_row("sra_experiments", exp)
            await writer.write("sra_experiments", exp)

        for run in pkg.get("runs", []):
            if run.get("accession"):
                self.qc.record_row("sra_runs", run)
                await writer.write("sra_runs", run)

        # ID mappings
        if self._mapper:
            await self._mapper.extract_sra_links(pkg)

    # ── GEO processing ────────────────────────────────────────

    # ── GEO checkpoint helpers ────────────────────────────────

    def _load_geo_checkpoint(self) -> set[str]:
        """Load set of already-processed GSE accessions from checkpoint file."""
        cp_path = self.settings.download_dir.parent / "geo_checkpoint.txt"
        if cp_path.exists():
            done = set(cp_path.read_text().strip().splitlines())
            logger.info("Loaded GEO checkpoint: %d GSEs already done", len(done))
            return done
        return set()

    def _save_geo_checkpoint(self, done: set[str]) -> None:
        """Append-friendly checkpoint save."""
        cp_path = self.settings.download_dir.parent / "geo_checkpoint.txt"
        cp_path.write_text("\n".join(sorted(done)) + "\n")

    async def _process_geo(
        self,
        writer: AsyncDBWriter | SQLiteWriter | MockAsyncDBWriter,
        accessions: Optional[list[str]] = None,
    ) -> dict[str, int]:
        """Download and parse GEO MINiML files with checkpoint/resume."""
        logger.info("Starting GEO pipeline")
        stats = {"series": 0, "samples": 0, "platforms": 0}

        dl_dir = self.settings.download_dir / "geo"
        dl_dir.mkdir(parents=True, exist_ok=True)

        if not accessions:
            # Auto-discover all GSE accessions from FTP
            logger.info("No specific GSE list provided; discovering all GSE from FTP...")
            try:
                ftp = FTPDownloader(self.settings)
                with ftp:
                    accessions = ftp.list_all_gse_accessions()
                logger.info("Discovered %d GSE accessions for processing", len(accessions))
            except Exception:
                logger.exception("GEO FTP GSE discovery failed")
                self.qc.record_error("geo_discovery", "FTP GSE listing failed")
                return stats

        # Resume: skip already-processed GSEs
        done = self._load_geo_checkpoint()
        remaining = [g for g in accessions if g not in done]
        if len(done) > 0:
            logger.info("Resuming: %d/%d remaining (%d already done)",
                        len(remaining), len(accessions), len(done))

        consecutive_errors = 0
        ftp = FTPDownloader(self.settings)

        for i, gse in enumerate(remaining):
            try:
                sub = await self._process_single_gse(writer, ftp, gse, dl_dir)
                for k in stats:
                    stats[k] += sub.get(k, 0)
                done.add(gse)
                consecutive_errors = 0

                # Checkpoint every 100 GSEs
                if (i + 1) % 100 == 0:
                    self._save_geo_checkpoint(done)
                    await writer.flush_all()
                    logger.info("GEO progress: %d/%d (total done: %d/%d) — %s",
                                i + 1, len(remaining), len(done), len(accessions), stats)

            except Exception as exc:
                logger.warning("Failed %s: %s", gse, exc)
                self.qc.record_error("geo_download", f"Failed {gse}", accession=gse)
                consecutive_errors += 1

                # Reconnect FTP on errors
                ftp.disconnect()

                # If too many consecutive errors, pause and retry
                if consecutive_errors >= 10:
                    logger.warning("10 consecutive errors — sleeping 60s before retry")
                    import asyncio as _aio
                    await _aio.sleep(60)
                    consecutive_errors = 0

        # Final checkpoint
        self._save_geo_checkpoint(done)
        ftp.disconnect()
        await writer.flush_all()
        logger.info("GEO pipeline complete: %s (total GSEs: %d)", stats, len(done))
        return stats

    async def _process_single_gse(
        self,
        writer: AsyncDBWriter | SQLiteWriter | MockAsyncDBWriter,
        ftp: FTPDownloader,
        gse_id: str,
        dl_dir: Path,
    ) -> dict[str, int]:
        stats = {"series": 0, "samples": 0, "platforms": 0}

        # Download MINiML
        try:
            ftp.connect()
            xml_path = ftp.download_geo_miniml(gse_id, dl_dir)
        except Exception:
            logger.exception("GEO FTP download failed for %s", gse_id)
            raise

        # Parse
        data = parse_miniml_file(xml_path)

        for s in data.get("series", []):
            self.qc.record_row("geo_series", s)
            await writer.write("geo_series", s)
            stats["series"] += 1
            if self._mapper:
                await self._mapper.extract_geo_series_links(s)

        for s in data.get("samples", []):
            self.qc.record_row("geo_samples", s)
            await writer.write("geo_samples", s)
            stats["samples"] += 1
            if self._mapper:
                await self._mapper.extract_geo_sample_links(s)

        for p in data.get("platforms", []):
            self.qc.record_row("geo_platforms", p)
            await writer.write("geo_platforms", p)
            stats["platforms"] += 1

        # Delete downloaded file to save disk
        try:
            xml_path.unlink()
        except OSError:
            pass

        return stats

    # ── Writer management ─────────────────────────────────────

    async def _get_writer(self) -> AsyncDBWriter | SQLiteWriter | MockAsyncDBWriter:
        if self._writer is not None:
            return self._writer

        # Try to create a real DB connection pool (fast fail: 3s timeout)
        try:
            import psycopg

            # Quick connectivity check before opening a pool
            conn = await asyncio.wait_for(
                psycopg.AsyncConnection.connect(self.settings.dsn),
                timeout=3.0,
            )
            await conn.close()

            import psycopg_pool

            pool = psycopg_pool.AsyncConnectionPool(
                self.settings.dsn,
                min_size=self.settings.db_pool_min,
                max_size=self.settings.db_pool_max,
            )
            await pool.open()
            async with pool.connection() as c:
                await c.execute(DDL)
                await c.commit()
            self._writer = AsyncDBWriter(pool, self.settings.batch_size)
            logger.info("Using PostgreSQL writer")
        except Exception:
            # Fallback to SQLite (persistent local file)
            db_path = self.settings.download_dir.parent / "metadata_crawl.sqlite"
            logger.info("PostgreSQL not available — using SQLite at %s", db_path)
            self._writer = SQLiteWriter(db_path)

        return self._writer


# ── CLI ───────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="GEO/SRA Metadata Pipeline")
    parser.add_argument("--geo-only", action="store_true")
    parser.add_argument("--sra-only", action="store_true")
    parser.add_argument("--gse", nargs="*", help="Specific GSE accessions")
    parser.add_argument("--sra-file", type=str, help="Path to already-downloaded SRA dump file (skip FTP)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--export-json", action="store_true",
                        help="Export SQLite data to JSON files after pipeline completes")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    geo = not args.sra_only
    sra = not args.geo_only

    pipeline = MetadataPipeline()
    sra_file = Path(args.sra_file) if args.sra_file else None
    result = asyncio.run(pipeline.run(geo=geo, sra=sra, geo_accessions=args.gse, sra_file=sra_file))
    print(json.dumps(result, indent=2, default=str))

    # Export to JSON if using SQLite and requested
    if args.export_json and isinstance(pipeline._writer, SQLiteWriter):
        exported = pipeline._writer.export_to_json()
        print(f"\nExported {len(exported)} tables to JSON:")
        for table, path in exported.items():
            print(f"  {table} → {path}")


if __name__ == "__main__":
    main()
