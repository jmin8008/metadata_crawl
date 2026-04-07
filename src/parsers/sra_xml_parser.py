"""Streaming SRA XML parser using lxml iterparse.

Processes EXPERIMENT_PACKAGE elements one at a time to keep memory flat
even for multi-GB dump files.
"""

from __future__ import annotations

import gzip
import io
import logging
import tarfile
from pathlib import Path
from typing import Any, BinaryIO, Generator, Optional

from lxml import etree

from src.db.models import (
    SRAStudy,
    SRASample,
    SRAExperiment,
    SRARun,
    BioSample,
    BioProject,
)

logger = logging.getLogger(__name__)


def _text(el: Optional[etree._Element], xpath: str = ".", default: str = "") -> str:
    if el is None:
        return default
    target = el.find(xpath) if xpath != "." else el
    if target is None or target.text is None:
        return default
    return target.text.strip()


def _attr(el: Optional[etree._Element], key: str, default: str = "") -> str:
    if el is None:
        return default
    return el.get(key, default)


def _collect_attrs(el: Optional[etree._Element], xpath: str) -> dict[str, str]:
    """Collect tag/value attribute pairs into a dict.

    SRA SAMPLE_ATTRIBUTEs use child elements <TAG> and <VALUE>,
    not XML attributes.
    """
    result: dict[str, str] = {}
    if el is None:
        return result
    for item in el.findall(xpath):
        # Try child elements first (SRA standard)
        tag_el = item.find("TAG")
        val_el = item.find("VALUE")
        if tag_el is not None and tag_el.text:
            tag = tag_el.text.strip()
            val = (val_el.text or "").strip() if val_el is not None else ""
        else:
            # Fallback: XML attributes
            tag = item.get("tag", "") or item.get("attribute_name", "")
            val = (item.text or "").strip() if item.text else item.get("value", "")
        if tag:
            result[tag] = val
    return result


# ── Public streaming API ──────────────────────────────────────


def _iter_from_stream(
    stream: BinaryIO,
    label: str = "",
) -> Generator[dict[str, Any], None, None]:
    """Parse EXPERIMENT_PACKAGEs from a single XML byte stream."""
    context = etree.iterparse(
        stream,
        events=("end",),
        tag="EXPERIMENT_PACKAGE",
    )

    count = 0
    for _event, elem in context:
        try:
            yield _parse_package(elem)
            count += 1
            if count % 10_000 == 0:
                logger.info("Parsed %d packages%s", count, f" ({label})" if label else "")
        except Exception:
            logger.exception("Error parsing EXPERIMENT_PACKAGE")
        finally:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    if label and count > 0:
        logger.info("Finished %s: %d packages", label, count)


# ── FTP dump entity parsers ──────────────────────────────────


def _iter_entities_from_stream(
    stream: BinaryIO,
    tag: str,
    parser_fn,
    label: str = "",
) -> Generator[dict[str, Any], None, None]:
    """Parse individual entities (STUDY, SAMPLE, EXPERIMENT, RUN) from FTP dump XML."""
    context = etree.iterparse(stream, events=("end",), tag=tag)
    count = 0
    for _event, elem in context:
        try:
            yield parser_fn(elem)
            count += 1
        except Exception:
            logger.exception("Error parsing %s element", tag)
        finally:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
    if count > 0 and label:
        logger.debug("%s: %d %s elements", label, count, tag)


def iter_experiment_packages(
    xml_path: Path,
) -> Generator[dict[str, Any], None, None]:
    """Yield parsed metadata dicts from each EXPERIMENT_PACKAGE.

    Supports:
    - Plain .xml files containing EXPERIMENT_PACKAGE elements
    - Gzipped .xml.gz files
    - .tar.gz archives (NCBI SRA FTP dump format with per-entity XML files)
    """
    path_str = str(xml_path)
    total = 0

    if path_str.endswith(".tar.gz") or path_str.endswith(".tgz"):
        for pkg in _iter_tar_gz_dump(xml_path):
            yield pkg
            total += 1
    elif path_str.endswith(".gz"):
        for pkg in _iter_from_stream(gzip.open(xml_path, "rb"), label=xml_path.name):
            yield pkg
            total += 1
    else:
        for pkg in _iter_from_stream(open(xml_path, "rb"), label=xml_path.name):
            yield pkg
            total += 1

    logger.info("Grand total: %d records from %s", total, xml_path.name)


def _iter_tar_gz_dump(
    tar_path: Path,
) -> Generator[dict[str, Any], None, None]:
    """Parse NCBI SRA FTP metadata dump (.tar.gz).

    The dump contains per-submission directories, each with separate XML files:
      SRA1202847/SRA1202847.study.xml      -> <STUDY> elements
      SRA1202847/SRA1202847.sample.xml     -> <SAMPLE> elements
      SRA1202847/SRA1202847.experiment.xml -> <EXPERIMENT> elements
      SRA1202847/SRA1202847.run.xml        -> <RUN> elements
      SRA1202847/SRA1202847.submission.xml -> skip

    Each entity is yielded as {"type": "study"|"sample"|"experiment"|"run", ...fields}.
    """
    logger.info("Opening SRA FTP dump: %s", tar_path)

    # Map file suffix to (XML tag, parser function, entity type name)
    _ENTITY_MAP = {
        ".study.xml": ("STUDY", _parse_study, "study"),
        ".sample.xml": ("SAMPLE", _parse_sample, "sample"),
        ".experiment.xml": ("EXPERIMENT", _parse_experiment, "experiment"),
        ".run.xml": ("RUN", _parse_run, "run"),
    }

    submission_count = 0
    entity_counts: dict[str, int] = {"study": 0, "sample": 0, "experiment": 0, "run": 0}

    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar:
            if not member.isfile() or not member.name.endswith(".xml"):
                continue

            # Determine entity type from filename suffix
            entity_type = None
            for suffix, (tag, parser_fn, etype) in _ENTITY_MAP.items():
                if member.name.endswith(suffix):
                    entity_type = etype
                    break

            if entity_type is None:
                continue  # skip submission.xml and unknown files

            f = tar.extractfile(member)
            if f is None:
                continue

            tag, parser_fn, etype = _ENTITY_MAP[suffix]
            for record in _iter_entities_from_stream(f, tag, parser_fn, label=member.name):
                record["_entity_type"] = etype
                yield record
                entity_counts[etype] += 1

            # Track submissions for progress
            if entity_type == "experiment":
                submission_count += 1
                if submission_count % 50_000 == 0:
                    logger.info(
                        "Progress: %d submissions processed — %s",
                        submission_count, dict(entity_counts),
                    )

    logger.info(
        "SRA dump complete: %d submissions, entities: %s",
        submission_count, dict(entity_counts),
    )


# ── Internal parsers ──────────────────────────────────────────


def _parse_package(pkg: etree._Element) -> dict[str, Any]:
    """Parse a single EXPERIMENT_PACKAGE element into model dicts."""
    result: dict[str, Any] = {}

    # Study
    study_el = pkg.find(".//STUDY")
    if study_el is not None:
        result["study"] = _parse_study(study_el)

    # Sample (SRA)
    sample_el = pkg.find(".//SAMPLE")
    if sample_el is not None:
        result["sample"] = _parse_sample(sample_el)

    # Experiment
    exp_el = pkg.find(".//EXPERIMENT")
    if exp_el is not None:
        result["experiment"] = _parse_experiment(exp_el)

    # Runs
    runs = []
    for run_el in pkg.findall(".//RUN"):
        runs.append(_parse_run(run_el))
    result["runs"] = runs

    return result


def _parse_study(el: etree._Element) -> dict[str, Any]:
    descriptor = el.find("DESCRIPTOR")
    ext_ids = {}
    for xref in el.findall(".//EXTERNAL_ID"):
        ns = xref.get("namespace", "")
        ext_ids[ns] = (xref.text or "").strip()
    for xref in el.findall(".//XREF_LINK"):
        db = _text(xref, "DB")
        xid = _text(xref, "ID")
        if db and xid:
            ext_ids[db] = xid

    return {
        "accession": _attr(el, "accession"),
        "alias": _attr(el, "alias"),
        "center_name": _attr(el, "center_name"),
        "title": _text(descriptor, "STUDY_TITLE") if descriptor is not None else "",
        "abstract": _text(descriptor, "STUDY_ABSTRACT") if descriptor is not None else "",
        "study_type": (
            _attr(descriptor.find("STUDY_TYPE"), "existing_study_type")
            if descriptor is not None and descriptor.find("STUDY_TYPE") is not None
            else ""
        ),
        "external_ids": ext_ids,
    }


def _parse_sample(el: etree._Element) -> dict[str, Any]:
    taxon_el = el.find(".//TAXON_ID")
    if taxon_el is None:
        taxon_el = el.find(".//SAMPLE_NAME/TAXON_ID")
    organism_el = el.find(".//SCIENTIFIC_NAME")
    if organism_el is None:
        organism_el = el.find(".//SAMPLE_NAME/SCIENTIFIC_NAME")
    attrs = _collect_attrs(el, ".//SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE")

    ext_ids = {}
    for xref in el.findall(".//EXTERNAL_ID"):
        ns = xref.get("namespace", "")
        ext_ids[ns] = (xref.text or "").strip()

    return {
        "accession": _attr(el, "accession"),
        "alias": _attr(el, "alias"),
        "title": _text(el, "TITLE"),
        "taxon_id": _text(taxon_el) if taxon_el is not None else "",
        "scientific_name": _text(organism_el) if organism_el is not None else "",
        "attributes": attrs,
        "external_ids": ext_ids,
    }


def _parse_experiment(el: etree._Element) -> dict[str, Any]:
    design = el.find("DESIGN")
    lib = design.find("LIBRARY_DESCRIPTOR") if design is not None else None
    platform_el = el.find("PLATFORM")

    # Instrument model: varies by platform type (ILLUMINA, etc.)
    instrument = ""
    if platform_el is not None:
        for child in platform_el:
            inst = child.find("INSTRUMENT_MODEL")
            if inst is not None and inst.text:
                instrument = inst.text.strip()
                break

    layout = ""
    if lib is not None:
        layout_el = lib.find("LIBRARY_LAYOUT")
        if layout_el is not None:
            for child in layout_el:
                layout = child.tag  # SINGLE or PAIRED
                break

    return {
        "accession": _attr(el, "accession"),
        "alias": _attr(el, "alias"),
        "title": _text(el, "TITLE"),
        "study_ref": _attr(el.find("STUDY_REF"), "accession") if el.find("STUDY_REF") is not None else "",
        "sample_ref": (
            _attr(design.find("SAMPLE_DESCRIPTOR"), "accession")
            if design is not None and design.find("SAMPLE_DESCRIPTOR") is not None
            else ""
        ),
        "strategy": _text(lib, "LIBRARY_STRATEGY") if lib is not None else "",
        "source": _text(lib, "LIBRARY_SOURCE") if lib is not None else "",
        "selection": _text(lib, "LIBRARY_SELECTION") if lib is not None else "",
        "layout": layout,
        "instrument_model": instrument,
    }


def _parse_run(el: etree._Element) -> dict[str, Any]:
    total_spots = _attr(el, "total_spots")
    total_bases = _attr(el, "total_bases")
    size = _attr(el, "size")

    # SRA files (download paths)
    sra_files = []
    for sf in el.findall(".//SRAFile"):
        sra_files.append({
            "url": _attr(sf, "url"),
            "filename": _attr(sf, "filename"),
            "size": _attr(sf, "size"),
        })

    return {
        "accession": _attr(el, "accession"),
        "alias": _attr(el, "alias"),
        "total_spots": int(total_spots) if total_spots else None,
        "total_bases": int(total_bases) if total_bases else None,
        "size": int(size) if size else None,
        "avg_length": (
            int(int(total_bases) / int(total_spots))
            if total_bases and total_spots and int(total_spots) > 0
            else None
        ),
        "experiment_ref": (
            _attr(el.find("EXPERIMENT_REF"), "accession")
            if el.find("EXPERIMENT_REF") is not None
            else ""
        ),
        "sra_files": sra_files,
    }
