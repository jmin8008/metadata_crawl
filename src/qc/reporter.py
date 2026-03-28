"""QC Reporter: null-field statistics, parse-error logs, coverage report.

Can write reports to both the ``qc_reports`` table and a JSON/text file.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class QCReporter:
    """Collects quality metrics during pipeline execution."""

    def __init__(self) -> None:
        self._null_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._error_log: list[dict[str, Any]] = []
        self._row_counts: dict[str, int] = defaultdict(int)

    # ── Recording ─────────────────────────────────────────────

    def record_row(self, table: str, record: dict[str, Any]) -> None:
        """Inspect a record for null/empty fields and tally."""
        self._row_counts[table] += 1
        for key, value in record.items():
            if value is None or value == "" or value == [] or value == {}:
                self._null_counts[table][key] += 1

    def record_error(
        self,
        source: str,
        message: str,
        *,
        accession: str = "",
        details: Optional[dict] = None,
    ) -> None:
        self._error_log.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "message": message,
            "accession": accession,
            "details": details or {},
        })

    # ── Report generation ─────────────────────────────────────

    def summary(self) -> dict[str, Any]:
        """Return a summary dict suitable for JSON serialisation."""
        tables = {}
        for table in sorted(set(self._row_counts) | set(self._null_counts)):
            total = self._row_counts.get(table, 0)
            nulls = dict(self._null_counts.get(table, {}))
            # Compute percentage
            pct = {}
            for field, cnt in nulls.items():
                pct[field] = round(cnt / total * 100, 2) if total > 0 else 0
            tables[table] = {
                "total_rows": total,
                "null_counts": nulls,
                "null_pct": pct,
            }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "tables": tables,
            "total_errors": len(self._error_log),
            "errors_sample": self._error_log[:50],  # first 50
        }

    def save_report(self, path: Path) -> Path:
        """Write JSON report to disk."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        report = self.summary()
        path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
        logger.info("QC report written to %s", path)
        return path

    async def save_to_db(self, writer) -> None:
        """Persist per-table QC rows to the qc_reports table (if using real DB)."""
        report = self.summary()
        for table_name, info in report["tables"].items():
            await writer.write(
                "qc_reports",
                {
                    "table_name": table_name,
                    "total_rows": info["total_rows"],
                    "null_counts": info["null_counts"],
                    "error_count": len(self._error_log),
                    "details": {"null_pct": info["null_pct"]},
                },
            )

    def print_summary(self) -> str:
        """Return human-readable summary text."""
        lines = ["=" * 60, "QC REPORT", "=" * 60]
        report = self.summary()
        for table, info in report["tables"].items():
            lines.append(f"\n  {table}: {info['total_rows']} rows")
            for field, pct in sorted(info["null_pct"].items(), key=lambda x: -x[1]):
                if pct > 0:
                    lines.append(f"    {field}: {pct}% null ({info['null_counts'][field]})")
        lines.append(f"\n  Total errors: {report['total_errors']}")
        lines.append("=" * 60)
        text = "\n".join(lines)
        logger.info(text)
        return text
