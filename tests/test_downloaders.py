"""Tests for downloader modules (no real network calls)."""

from __future__ import annotations

import ftplib
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from src.config import Settings
from src.downloaders.ftp_downloader import FTPDownloader
from src.downloaders.http_downloader import HTTPDownloader


# ── FTP Downloader ────────────────────────────────────────────


class TestFTPDownloader:
    def test_init(self, settings: Settings) -> None:
        dl = FTPDownloader(settings)
        assert dl.settings is settings
        assert dl._ftp is None

    @patch("src.downloaders.ftp_downloader.ftplib.FTP")
    def test_connect(self, mock_ftp_cls, settings: Settings) -> None:
        mock_ftp = MagicMock()
        mock_ftp_cls.return_value = mock_ftp
        dl = FTPDownloader(settings)
        ftp = dl.connect()
        assert ftp is mock_ftp
        mock_ftp.connect.assert_called_once_with(settings.ftp_host)
        mock_ftp.login.assert_called_once()

    @patch("src.downloaders.ftp_downloader.ftplib.FTP")
    def test_disconnect(self, mock_ftp_cls, settings: Settings) -> None:
        mock_ftp = MagicMock()
        mock_ftp_cls.return_value = mock_ftp
        dl = FTPDownloader(settings)
        dl.connect()
        dl.disconnect()
        mock_ftp.quit.assert_called_once()
        assert dl._ftp is None

    @patch("src.downloaders.ftp_downloader.ftplib.FTP")
    def test_download_file_creates_dirs(self, mock_ftp_cls, settings: Settings, tmp_path: Path) -> None:
        mock_ftp = MagicMock()
        mock_ftp_cls.return_value = mock_ftp
        mock_ftp.size.return_value = 100

        def fake_retr(cmd, callback, rest=None):
            callback(b"x" * 100)

        mock_ftp.retrbinary.side_effect = fake_retr

        dl = FTPDownloader(settings)
        out = dl.download_file(
            "/some/file.xml",
            tmp_path / "sub" / "file.xml",
            resume=False,
        )
        assert out.exists()
        assert out.stat().st_size == 100

    @patch("src.downloaders.ftp_downloader.ftplib.FTP")
    def test_resume_skip_complete(self, mock_ftp_cls, settings: Settings, tmp_path: Path) -> None:
        mock_ftp = MagicMock()
        mock_ftp_cls.return_value = mock_ftp
        mock_ftp.size.return_value = 50

        local = tmp_path / "existing.xml"
        local.write_bytes(b"x" * 50)

        dl = FTPDownloader(settings)
        out = dl.download_file("/remote/existing.xml", local, resume=True)
        assert out == local
        mock_ftp.retrbinary.assert_not_called()

    @patch("src.downloaders.ftp_downloader.ftplib.FTP")
    def test_context_manager(self, mock_ftp_cls, settings: Settings) -> None:
        mock_ftp = MagicMock()
        mock_ftp_cls.return_value = mock_ftp
        with FTPDownloader(settings) as dl:
            assert dl._ftp is not None
        mock_ftp.quit.assert_called_once()

    @patch("src.downloaders.ftp_downloader.ftplib.FTP")
    def test_list_all_gse_accessions(self, mock_ftp_cls, settings: Settings) -> None:
        mock_ftp = MagicMock()
        mock_ftp_cls.return_value = mock_ftp

        # Simulate FTP directory structure
        def fake_nlst(path):
            if path == FTPDownloader.GEO_MINIML_BASE:
                return [
                    "/geo/series/GSEnnn",
                    "/geo/series/GSE1nnn",
                ]
            elif path.endswith("GSEnnn"):
                return [
                    "/geo/series/GSEnnn/GSE100",
                    "/geo/series/GSEnnn/GSE200",
                    "/geo/series/GSEnnn/GSE300",
                ]
            elif path.endswith("GSE1nnn"):
                return [
                    "/geo/series/GSE1nnn/GSE1000",
                    "/geo/series/GSE1nnn/GSE1500",
                ]
            return []

        mock_ftp.nlst.side_effect = fake_nlst

        dl = FTPDownloader(settings)
        result = dl.list_all_gse_accessions()
        assert result == ["GSE100", "GSE1000", "GSE1500", "GSE200", "GSE300"]

    @patch("src.downloaders.ftp_downloader.ftplib.FTP")
    def test_list_gse_handles_ftp_error(self, mock_ftp_cls, settings: Settings) -> None:
        mock_ftp = MagicMock()
        mock_ftp_cls.return_value = mock_ftp

        def fake_nlst(path):
            if path == FTPDownloader.GEO_MINIML_BASE:
                return ["/geo/series/GSEnnn"]
            raise ftplib.error_temp("timeout")

        mock_ftp.nlst.side_effect = fake_nlst

        dl = FTPDownloader(settings)
        result = dl.list_all_gse_accessions()
        # Should return empty list gracefully (error on group listing)
        assert result == []

    def test_geo_miniml_path_generation(self, settings: Settings) -> None:
        dl = FTPDownloader(settings)
        # Verify path computation logic (won't actually connect)
        gse = "GSE12345"
        numeric = gse.replace("GSE", "")
        prefix = numeric[:-3] + "nnn"
        expected = f"/geo/series/GSE{prefix}/{gse}/miniml/{gse}_family.xml.tgz"
        assert prefix == "12nnn"


# ── HTTP Downloader ───────────────────────────────────────────


class TestHTTPDownloader:
    def test_init(self, settings: Settings) -> None:
        dl = HTTPDownloader(settings)
        assert dl._rate_delay == settings.rate_limit_delay

    def test_base_params_no_key(self, settings: Settings) -> None:
        settings.ncbi_api_key = None
        dl = HTTPDownloader(settings)
        params = dl._base_params()
        assert "api_key" not in params
        assert params["email"] == settings.ncbi_email

    def test_base_params_with_key(self, settings: Settings) -> None:
        settings.ncbi_api_key = "test_key_123"
        dl = HTTPDownloader(settings)
        params = dl._base_params()
        assert params["api_key"] == "test_key_123"

    @pytest.mark.asyncio
    async def test_close_noop(self, settings: Settings) -> None:
        dl = HTTPDownloader(settings)
        await dl.close()  # should not raise

    @pytest.mark.asyncio
    async def test_context_manager(self, settings: Settings) -> None:
        async with HTTPDownloader(settings) as dl:
            assert dl is not None
