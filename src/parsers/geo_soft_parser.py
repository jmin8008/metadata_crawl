"""Parser for GEO SOFT (Simple Omnibus Format in Text) files.

SOFT is a line-oriented format where sections are delimited by
^SERIES, ^SAMPLE, ^PLATFORM lines and key=value pairs start with '!'.
"""

from __future__ import annotations

import gzip
import logging
import re
from pathlib import Path
from typing import Any, Generator, Optional, TextIO

logger = logging.getLogger(__name__)

_SECTION_RE = re.compile(r"^\^(SERIES|SAMPLE|PLATFORM)\s*=\s*(\S+)")
_KV_RE = re.compile(r"^!(\w[\w\-]*)\s*=\s*(.*)")


def parse_soft_file(path: Path) -> dict[str, Any]:
    """Parse a SOFT file into structured dicts.

    Returns dict with keys 'series', 'samples', 'platforms',
    each mapping accession -> attribute dict.
    """
    opener = gzip.open if str(path).endswith(".gz") else open
    result: dict[str, Any] = {"series": {}, "samples": {}, "platforms": {}}

    with opener(path, "rt", errors="replace") as fh:
        current_section: Optional[str] = None
        current_acc: Optional[str] = None
        current_data: dict[str, Any] = {}

        for line in fh:
            line = line.rstrip("\n\r")

            # Section header
            m = _SECTION_RE.match(line)
            if m:
                # Save previous section
                if current_section and current_acc:
                    _store(result, current_section, current_acc, current_data)
                current_section = m.group(1).lower()
                current_acc = m.group(2)
                current_data = {}
                continue

            # Key=value
            m = _KV_RE.match(line)
            if m and current_section:
                key = m.group(1).lower().replace("-", "_")
                value = m.group(2).strip()
                # Some keys repeat (e.g., characteristics)
                if key in current_data:
                    existing = current_data[key]
                    if isinstance(existing, list):
                        existing.append(value)
                    else:
                        current_data[key] = [existing, value]
                else:
                    current_data[key] = value

        # Last section
        if current_section and current_acc:
            _store(result, current_section, current_acc, current_data)

    return result


def iter_soft_samples(path: Path) -> Generator[dict[str, Any], None, None]:
    """Yield sample dicts one at a time (memory-efficient for large files)."""
    opener = gzip.open if str(path).endswith(".gz") else open

    with opener(path, "rt", errors="replace") as fh:
        current_section: Optional[str] = None
        current_acc: Optional[str] = None
        current_data: dict[str, Any] = {}

        for line in fh:
            line = line.rstrip("\n\r")
            m = _SECTION_RE.match(line)
            if m:
                if current_section == "sample" and current_acc:
                    yield {"accession": current_acc, **current_data}
                current_section = m.group(1).lower()
                current_acc = m.group(2)
                current_data = {}
                continue

            m = _KV_RE.match(line)
            if m and current_section == "sample":
                key = m.group(1).lower().replace("-", "_")
                value = m.group(2).strip()
                if key in current_data:
                    existing = current_data[key]
                    if isinstance(existing, list):
                        existing.append(value)
                    else:
                        current_data[key] = [existing, value]
                else:
                    current_data[key] = value

        if current_section == "sample" and current_acc:
            yield {"accession": current_acc, **current_data}


def _store(
    result: dict[str, Any],
    section: str,
    accession: str,
    data: dict[str, Any],
) -> None:
    key = section + "s" if not section.endswith("s") else section
    if key == "seriess":
        key = "series"
    result.setdefault(key, {})[accession] = data
