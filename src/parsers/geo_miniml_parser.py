"""Parser for GEO MINiML (MINIATURE Markup Language) XML files.

MINiML is NCBI's XML schema for GEO data.  A "family" file contains
Series, Samples and Platform elements for one GSE.
"""

from __future__ import annotations

import gzip
import logging
import tarfile
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

from lxml import etree

logger = logging.getLogger(__name__)

# MINiML namespace
NS = {"ml": "http://www.ncbi.nlm.nih.gov/geo/info/MINiML"}


def _txt(el: Optional[etree._Element], xpath: str, ns: dict = NS) -> str:
    if el is None:
        return ""
    node = el.find(xpath, ns)
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def _all_text(el: Optional[etree._Element], xpath: str, ns: dict = NS) -> str:
    """Concatenate all text content including children."""
    if el is None:
        return ""
    node = el.find(xpath, ns)
    if node is None:
        return ""
    return "".join(node.itertext()).strip()


# ── Public API ────────────────────────────────────────────────


def parse_miniml_file(xml_path: Path) -> dict[str, Any]:
    """Parse a MINiML XML file and return structured data.

    Handles both raw .xml and .xml.tgz archives.
    """
    data = _read_xml_bytes(xml_path)
    root = etree.fromstring(data)
    return _parse_root(root)


def parse_miniml_bytes(data: bytes) -> dict[str, Any]:
    try:
        root = etree.fromstring(data)
    except etree.XMLSyntaxError:
        # Retry with encoding errors replaced
        cleaned = data.decode("utf-8", errors="replace").encode("utf-8")
        root = etree.fromstring(cleaned)
    return _parse_root(root)


# ── Internal ──────────────────────────────────────────────────


def _read_xml_bytes(path: Path) -> bytes:
    """Open .xml, .xml.gz, or .xml.tgz and return raw bytes."""
    suffix = "".join(path.suffixes)
    if suffix.endswith(".tgz") or suffix.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as tf:
            for member in tf.getmembers():
                if member.name.endswith(".xml"):
                    f = tf.extractfile(member)
                    if f:
                        return f.read()
        raise FileNotFoundError(f"No XML found inside {path}")
    elif suffix.endswith(".gz"):
        with gzip.open(path, "rb") as f:
            return f.read()
    else:
        return path.read_bytes()


def _parse_root(root: etree._Element) -> dict[str, Any]:
    result: dict[str, Any] = {
        "series": [],
        "samples": [],
        "platforms": [],
    }

    for series_el in root.findall("ml:Series", NS):
        result["series"].append(_parse_series(series_el))

    for sample_el in root.findall("ml:Sample", NS):
        result["samples"].append(_parse_sample(sample_el))

    for plat_el in root.findall("ml:Platform", NS):
        result["platforms"].append(_parse_platform(plat_el))

    return result


def _parse_series(el: etree._Element) -> dict[str, Any]:
    iid = el.get("iid", "")

    # Contributors
    contributors = []
    for c in el.findall("ml:Contributor", NS):
        person = _txt(c, "ml:Person/ml:First", NS)
        last = _txt(c, "ml:Person/ml:Last", NS)
        if person or last:
            contributors.append(f"{person} {last}".strip())

    # PubMed IDs
    pubmed_ids = [
        pid.text.strip()
        for pid in el.findall("ml:Pubmed-ID", NS)
        if pid.text
    ]

    # Relation links (SRA, BioProject, etc.)
    relations = {}
    for rel in el.findall("ml:Relation", NS):
        rtype = rel.get("type", "")
        target = rel.get("target", "")
        if rtype:
            relations[rtype] = target

    return {
        "accession": iid,
        "title": _txt(el, "ml:Title"),
        "summary": _all_text(el, "ml:Summary"),
        "overall_design": _all_text(el, "ml:Overall-Design"),
        "experiment_type": _txt(el, "ml:Type"),
        "contributors": contributors,
        "pubmed_ids": pubmed_ids,
        "submission_date": _txt(el, "ml:Status/ml:Submission-Date"),
        "last_update_date": _txt(el, "ml:Status/ml:Last-Update-Date"),
        "release_date": _txt(el, "ml:Status/ml:Release-Date"),
        "relations": relations,
        "supplementary_data": [
            s.text.strip()
            for s in el.findall("ml:Supplementary-Data", NS)
            if s.text
        ],
    }


def _parse_sample(el: etree._Element) -> dict[str, Any]:
    iid = el.get("iid", "")

    # Characteristics
    characteristics: dict[str, str] = {}
    for ch in el.findall("ml:Channel/ml:Characteristics", NS):
        tag = ch.get("tag", "")
        val = (ch.text or "").strip()
        if tag:
            characteristics[tag] = val

    # Channel details
    channel = el.find("ml:Channel", NS)

    relations = {}
    for rel in el.findall("ml:Relation", NS):
        rtype = rel.get("type", "")
        target = rel.get("target", "")
        if rtype:
            relations[rtype] = target

    return {
        "accession": iid,
        "title": _txt(el, "ml:Title"),
        "type": _txt(el, "ml:Type"),
        "source_name": _txt(channel, "ml:Source") if channel is not None else "",
        "organism": _txt(channel, "ml:Organism") if channel is not None else "",
        "taxid": (
            channel.find("ml:Organism", NS).get("taxid", "")
            if channel is not None and channel.find("ml:Organism", NS) is not None
            else ""
        ),
        "characteristics": characteristics,
        "treatment_protocol": _all_text(channel, "ml:Treatment-Protocol") if channel is not None else "",
        "extract_protocol": _all_text(channel, "ml:Extract-Protocol") if channel is not None else "",
        "label": _txt(channel, "ml:Label") if channel is not None else "",
        "molecule": _txt(channel, "ml:Molecule") if channel is not None else "",
        "platform_ref": _txt(el, "ml:Platform-Ref"),
        "series_ref": [
            s.get("ref", "")
            for s in el.findall("ml:Series-Ref", NS)
        ],
        "relations": relations,
        "supplementary_data": [
            s.text.strip()
            for s in el.findall("ml:Supplementary-Data", NS)
            if s.text
        ],
    }


def _parse_platform(el: etree._Element) -> dict[str, Any]:
    iid = el.get("iid", "")
    return {
        "accession": iid,
        "title": _txt(el, "ml:Title"),
        "technology": _txt(el, "ml:Technology"),
        "distribution": _txt(el, "ml:Distribution"),
        "organism": _txt(el, "ml:Organism"),
        "taxid": (
            el.find("ml:Organism", NS).get("taxid", "")
            if el.find("ml:Organism", NS) is not None
            else ""
        ),
        "manufacturer": _txt(el, "ml:Manufacturer"),
        "manufacture_protocol": _all_text(el, "ml:Manufacture-Protocol"),
    }
