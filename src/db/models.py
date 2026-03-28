"""Data models (dataclasses) for pipeline entities."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


# ── GEO Models ────────────────────────────────────────────────


@dataclass
class GEOSeries:
    accession: str
    title: str = ""
    summary: str = ""
    overall_design: str = ""
    experiment_type: str = ""
    contributors: list[str] = field(default_factory=list)
    pubmed_ids: list[str] = field(default_factory=list)
    submission_date: Optional[str] = None
    last_update_date: Optional[str] = None
    release_date: Optional[str] = None
    relations: dict[str, str] = field(default_factory=dict)
    supplementary_data: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class GEOSample:
    accession: str
    title: str = ""
    sample_type: str = ""
    source_name: str = ""
    organism: str = ""
    taxid: str = ""
    characteristics: dict[str, str] = field(default_factory=dict)
    treatment_protocol: str = ""
    extract_protocol: str = ""
    label: str = ""
    molecule: str = ""
    platform_ref: str = ""
    series_refs: list[str] = field(default_factory=list)
    relations: dict[str, str] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class GEOPlatform:
    accession: str
    title: str = ""
    technology: str = ""
    distribution: str = ""
    organism: str = ""
    taxid: str = ""
    manufacturer: str = ""
    manufacture_protocol: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


# ── SRA Models ────────────────────────────────────────────────


@dataclass
class SRAStudy:
    accession: str
    alias: str = ""
    center_name: str = ""
    title: str = ""
    abstract: str = ""
    study_type: str = ""
    external_ids: dict[str, str] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SRASample:
    accession: str
    alias: str = ""
    title: str = ""
    taxon_id: str = ""
    scientific_name: str = ""
    attributes: dict[str, str] = field(default_factory=dict)
    external_ids: dict[str, str] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SRAExperiment:
    accession: str
    alias: str = ""
    title: str = ""
    study_ref: str = ""
    sample_ref: str = ""
    strategy: str = ""
    source: str = ""
    selection: str = ""
    layout: str = ""
    instrument_model: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SRARun:
    accession: str
    alias: str = ""
    experiment_ref: str = ""
    total_spots: Optional[int] = None
    total_bases: Optional[int] = None
    size: Optional[int] = None
    avg_length: Optional[int] = None
    sra_files: list[dict[str, str]] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


# ── Cross-reference Models ────────────────────────────────────


@dataclass
class BioSample:
    accession: str  # SAMN...
    taxon_id: str = ""
    organism: str = ""
    attributes: dict[str, str] = field(default_factory=dict)
    sra_sample_ref: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class BioProject:
    accession: str  # PRJNA...
    title: str = ""
    description: str = ""
    sra_study_ref: str = ""
    geo_series_ref: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class IDMapping:
    source_db: str
    source_id: str
    target_db: str
    target_id: str
    link_type: str = ""  # e.g., "elink", "parsed", "cross_ref"
