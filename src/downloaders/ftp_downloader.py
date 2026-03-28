"""FTP downloader with resume (REST) support and progress callback."""

from __future__ import annotations

import ftplib
import logging
import time
from pathlib import Path
from typing import Callable, Optional

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.config import Settings

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int], None]


class FTPDownloader:
    """Download files from NCBI FTP with resume capability."""

    # Well-known NCBI FTP paths
    SRA_METADATA_PATH = "/sra/reports/Metadata"
    GEO_MINIML_BASE = "/geo/series"
    GEO_DATASETS_BASE = "/geo/datasets"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._ftp: Optional[ftplib.FTP] = None

    # ── Connection management ─────────────────────────────────

    def connect(self) -> ftplib.FTP:
        if self._ftp is not None:
            try:
                self._ftp.voidcmd("NOOP")
                return self._ftp
            except (ftplib.error_temp, ftplib.error_perm, OSError):
                self._ftp = None

        logger.info("Connecting to %s", self.settings.ftp_host)
        ftp = ftplib.FTP(timeout=self.settings.ftp_timeout)
        ftp.connect(self.settings.ftp_host)
        ftp.login()  # anonymous
        self._ftp = ftp
        return ftp

    def disconnect(self) -> None:
        if self._ftp is not None:
            try:
                self._ftp.quit()
            except Exception:
                pass
            self._ftp = None

    # ── Core download ─────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=4, max=60),
        retry=retry_if_exception_type((ftplib.error_temp, OSError, EOFError)),
        reraise=True,
    )
    def download_file(
        self,
        remote_path: str,
        local_path: Path,
        *,
        resume: bool = True,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> Path:
        """Download a single file with optional resume.

        Parameters
        ----------
        remote_path : str
            Absolute path on the FTP server.
        local_path : Path
            Where to save locally.
        resume : bool
            If True and *local_path* exists, resume from the current size.
        progress_cb : callable(downloaded_bytes, total_bytes)
            Optional progress callback.

        Returns
        -------
        Path
            The local_path after successful download.
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        ftp = self.connect()

        # Remote file size
        try:
            remote_size = ftp.size(remote_path)
        except ftplib.error_perm:
            remote_size = None

        # Resume offset
        offset = 0
        mode = "wb"
        if resume and local_path.exists() and remote_size is not None:
            local_size = local_path.stat().st_size
            if local_size >= remote_size:
                logger.info("Already downloaded: %s", local_path)
                return local_path
            offset = local_size
            mode = "ab"
            logger.info("Resuming %s from byte %d", remote_path, offset)

        downloaded = offset

        def _write(chunk: bytes) -> None:
            nonlocal downloaded
            fp.write(chunk)
            downloaded += len(chunk)
            if progress_cb and remote_size:
                progress_cb(downloaded, remote_size)

        ftp.voidcmd("TYPE I")  # binary mode
        with open(local_path, mode) as fp:
            if offset > 0:
                ftp.retrbinary(f"RETR {remote_path}", _write, rest=offset)
            else:
                ftp.retrbinary(f"RETR {remote_path}", _write)

        logger.info("Downloaded %s → %s (%d bytes)", remote_path, local_path, downloaded)
        return local_path

    # ── Convenience helpers ───────────────────────────────────

    def list_dir(self, remote_dir: str) -> list[str]:
        ftp = self.connect()
        return ftp.nlst(remote_dir)

    def download_sra_full_xml(
        self,
        local_dir: Path,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> Path:
        """Download the latest SRA full metadata dump (.tar.gz).

        Prefers ``NCBI_SRA_Metadata_Full_*.tar.gz`` (monthly full snapshot).
        Falls back to the latest daily ``NCBI_SRA_Metadata_*.tar.gz``.
        """
        files = self.list_dir(self.SRA_METADATA_PATH)

        # Prefer Full monthly dumps
        full_dumps = sorted(
            [f for f in files if "Full" in f and f.endswith(".tar.gz")],
        )
        if full_dumps:
            target = full_dumps[-1]  # latest
        else:
            # Fallback to daily dumps
            daily_dumps = sorted(
                [f for f in files if f.endswith(".tar.gz") and "Full" not in f],
            )
            if not daily_dumps:
                raise FileNotFoundError("No SRA metadata dump found on FTP")
            target = daily_dumps[-1]

        logger.info("Selected SRA dump: %s", target)
        local_path = Path(local_dir) / Path(target).name
        return self.download_file(target, local_path, progress_cb=progress_cb)

    def list_all_gse_accessions(
        self,
        *,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> list[str]:
        """Discover all GSE accessions from the GEO FTP directory tree.

        GEO FTP structure:
            /geo/series/GSEnnn/GSE12345/
            /geo/series/GSE1nnn/GSE1000/
            /geo/series/GSE1nnn/GSE1001/
            ...

        Returns sorted list of GSE accession strings.
        """
        ftp = self.connect()
        logger.info("Listing GEO series group directories from %s", self.GEO_MINIML_BASE)

        # Step 1: List group directories (GSEnnn, GSE1nnn, GSE2nnn, ...)
        try:
            group_dirs = ftp.nlst(self.GEO_MINIML_BASE)
        except ftplib.error_perm:
            logger.error("Cannot list %s", self.GEO_MINIML_BASE)
            return []

        # Filter to GSE* pattern only
        group_dirs = sorted(
            d for d in group_dirs
            if d.split("/")[-1].startswith("GSE")
        )
        logger.info("Found %d group directories", len(group_dirs))

        # Step 2: List individual GSE dirs within each group
        all_gse: list[str] = []
        for i, group_path in enumerate(group_dirs):
            try:
                ftp = self.connect()  # reconnect if needed
                entries = ftp.nlst(group_path)
                gse_ids = [
                    e.split("/")[-1]
                    for e in entries
                    if e.split("/")[-1].startswith("GSE")
                    and e.split("/")[-1][3:].isdigit()
                ]
                all_gse.extend(gse_ids)

                if progress_cb:
                    progress_cb(i + 1, len(group_dirs))

            except (ftplib.error_temp, ftplib.error_perm, OSError) as exc:
                logger.warning("Failed to list %s: %s", group_path, exc)
                # Reconnect on transient errors
                self._ftp = None
                time.sleep(2)
                continue

        all_gse.sort()
        logger.info("Discovered %d GSE accessions total", len(all_gse))
        return all_gse

    def download_geo_miniml(
        self,
        gse_id: str,
        local_dir: Path,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> Path:
        """Download MINiML XML for a given GSE accession.

        GEO stores MINiML at:
            /geo/series/GSE{nnn}nnn/GSE{id}/miniml/GSE{id}_family.xml.tgz
        """
        numeric = gse_id.replace("GSE", "")
        prefix = numeric[:-3] + "nnn" if len(numeric) > 3 else "nnn"
        remote_dir = f"{self.GEO_MINIML_BASE}/GSE{prefix}/{gse_id}/miniml"
        filename = f"{gse_id}_family.xml.tgz"
        remote_path = f"{remote_dir}/{filename}"
        local_path = Path(local_dir) / filename
        return self.download_file(remote_path, local_path, progress_cb=progress_cb)

    # ── Context manager ───────────────────────────────────────

    def __enter__(self) -> "FTPDownloader":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.disconnect()
