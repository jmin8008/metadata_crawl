"""Async HTTP downloader using httpx for E-utilities API calls."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional
from xml.etree import ElementTree as ET

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.config import Settings

logger = logging.getLogger(__name__)

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


class HTTPDownloader:
    """Async HTTP client for NCBI E-utilities with rate limiting."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_downloads)
        self._rate_delay = settings.rate_limit_delay
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    def _base_params(self) -> dict[str, str]:
        params: dict[str, str] = {
            "tool": self.settings.ncbi_tool_name,
            "email": self.settings.ncbi_email,
        }
        if self.settings.ncbi_api_key:
            params["api_key"] = self.settings.ncbi_api_key
        return params

    # ── Core request ──────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException)),
        reraise=True,
    )
    async def _request(
        self,
        endpoint: str,
        params: dict[str, Any],
    ) -> httpx.Response:
        async with self._semaphore:
            client = await self._get_client()
            url = f"{EUTILS_BASE}/{endpoint}"
            resp = await client.get(url, params={**self._base_params(), **params})
            resp.raise_for_status()
            await asyncio.sleep(self._rate_delay)
            return resp

    # ── E-utilities wrappers ──────────────────────────────────

    async def esearch(
        self,
        db: str,
        term: str,
        retmax: int = 500,
        retstart: int = 0,
    ) -> dict[str, Any]:
        """Run an ESearch and return parsed result dict."""
        resp = await self._request(
            "esearch.fcgi",
            {"db": db, "term": term, "retmax": retmax, "retstart": retstart, "retmode": "json"},
        )
        return resp.json()

    async def efetch_xml(
        self,
        db: str,
        ids: list[str],
    ) -> ET.Element:
        """Run EFetch and return parsed XML Element."""
        resp = await self._request(
            "efetch.fcgi",
            {"db": db, "id": ",".join(ids), "retmode": "xml"},
        )
        return ET.fromstring(resp.content)

    async def elink(
        self,
        dbfrom: str,
        db: str,
        ids: list[str],
    ) -> list[dict[str, Any]]:
        """Run ELink and return list of link sets.

        Returns a list of dicts with keys: ``from_id``, ``to_ids``, ``link_name``.
        """
        resp = await self._request(
            "elink.fcgi",
            {
                "dbfrom": dbfrom,
                "db": db,
                "id": ",".join(ids),
                "retmode": "xml",
            },
        )
        root = ET.fromstring(resp.content)
        results: list[dict[str, Any]] = []
        for linkset in root.findall(".//LinkSet"):
            from_id_el = linkset.find(".//IdList/Id")
            from_id = from_id_el.text if from_id_el is not None else None
            for linksetdb in linkset.findall(".//LinkSetDb"):
                link_name_el = linksetdb.find("LinkName")
                link_name = link_name_el.text if link_name_el is not None else ""
                to_ids = [el.text for el in linksetdb.findall(".//Link/Id") if el.text]
                results.append(
                    {"from_id": from_id, "to_ids": to_ids, "link_name": link_name}
                )
        return results

    async def esummary(
        self,
        db: str,
        ids: list[str],
    ) -> dict[str, Any]:
        resp = await self._request(
            "esummary.fcgi",
            {"db": db, "id": ",".join(ids), "retmode": "json", "version": "2.0"},
        )
        return resp.json()

    # ── Context manager ───────────────────────────────────────

    async def __aenter__(self) -> "HTTPDownloader":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()
