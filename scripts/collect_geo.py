"""GEO metadata collection — async semaphore-based parallel with checkpoint.

Usage:
    python scripts/collect_geo.py              # resume
    python scripts/collect_geo.py --fresh
    python scripts/collect_geo.py --workers 10
"""
import asyncio
import json
import logging
import os
import sys
import tarfile
import time
import urllib.request
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
from src.db.writer import SQLiteWriter
from src.parsers.geo_miniml_parser import parse_miniml_bytes
from src.qc.reporter import QCReporter

# Suppress httpx request logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data/logs/geo_pipeline.log"),
    ],
)
logger = logging.getLogger(__name__)

DATA_DIR = Path("data")
CHECKPOINT_FILE = DATA_DIR / "geo_checkpoint.txt"
GSE_LIST_FILE = DATA_DIR / "geo_gse_list.txt"


# ── GSE list ──────────────────────────────────────────────────

def fetch_all_gse_ids() -> list[str]:
    if GSE_LIST_FILE.exists():
        ids = GSE_LIST_FILE.read_text().strip().splitlines()
        logger.info("Loaded cached GSE list: %d IDs", len(ids))
        return ids

    logger.info("Fetching all GSE IDs from E-utilities...")
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    api_key = os.environ.get("MC_NCBI_API_KEY", "")
    key_param = f"&api_key={api_key}" if api_key else ""

    url = f"{base}?db=gds&term=GSE[ETYP]&retmax=0&retmode=json{key_param}"
    resp = json.loads(urllib.request.urlopen(url, timeout=30).read())
    total = int(resp["esearchresult"]["count"])

    gse_ids = set()
    for start in range(0, total, 10000):
        url = f"{base}?db=gds&term=GSE[ETYP]&retmax=10000&retstart={start}&retmode=json{key_param}"
        resp = json.loads(urllib.request.urlopen(url, timeout=30).read())
        for uid in resp["esearchresult"]["idlist"]:
            if uid.startswith("200"):
                gse_ids.add("GSE" + str(int(uid[3:])))
        logger.info("Fetched: %d/%d → %d GSEs", min(start + 10000, total), total, len(gse_ids))
        time.sleep(0.35)

    result = sorted(gse_ids)
    GSE_LIST_FILE.write_text("\n".join(result) + "\n")
    return result


# ── Checkpoint ────────────────────────────────────────────────

def load_checkpoint() -> set[str]:
    if CHECKPOINT_FILE.exists():
        done = set(CHECKPOINT_FILE.read_text().strip().splitlines())
        logger.info("Checkpoint: %d GSEs done", len(done))
        return done
    return set()

def save_checkpoint(done: set[str]):
    CHECKPOINT_FILE.write_text("\n".join(sorted(done)) + "\n")


# ── URL helper ────────────────────────────────────────────────

def make_url(gse_id: str) -> str:
    numeric = gse_id.replace("GSE", "")
    prefix = numeric[:-3] + "nnn" if len(numeric) > 3 else "nnn"
    return f"https://ftp.ncbi.nlm.nih.gov/geo/series/GSE{prefix}/{gse_id}/miniml/{gse_id}_family.xml.tgz"


# ── Main ──────────────────────────────────────────────────────

async def main():
    Path("data/logs").mkdir(parents=True, exist_ok=True)

    fresh = "--fresh" in sys.argv
    num_workers = 10
    for i, arg in enumerate(sys.argv):
        if arg == "--workers" and i + 1 < len(sys.argv):
            num_workers = int(sys.argv[i + 1])

    all_gse = fetch_all_gse_ids()
    done = set() if fresh else load_checkpoint()
    remaining = [g for g in all_gse if g not in done]
    logger.info("Total: %d, Done: %d, Remaining: %d, Workers: %d",
                len(all_gse), len(done), len(remaining), num_workers)

    if not remaining:
        logger.info("All done!")
        return

    writer = SQLiteWriter(DATA_DIR / "metadata_crawl.sqlite")
    qc = QCReporter()
    stats = {"series": 0, "samples": 0, "platforms": 0, "errors": 0}
    processed = 0
    start_time = time.time()

    sem = asyncio.Semaphore(num_workers)
    timeout = httpx.Timeout(300.0, connect=30.0)
    limits = httpx.Limits(max_connections=num_workers + 2, max_keepalive_connections=num_workers)

    async with httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True) as client:

        async def process_one(gse_id: str):
            nonlocal processed
            async with sem:
                try:
                    resp = await client.get(make_url(gse_id))
                    resp.raise_for_status()
                    buf = BytesIO(resp.content)

                    data = {"series": [], "samples": [], "platforms": []}
                    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
                        for member in tar:
                            if member.isfile() and member.name.endswith(".xml"):
                                f = tar.extractfile(member)
                                if f:
                                    parsed = parse_miniml_bytes(f.read())
                                    data["series"].extend(parsed.get("series", []))
                                    data["samples"].extend(parsed.get("samples", []))
                                    data["platforms"].extend(parsed.get("platforms", []))
                    return ("ok", gse_id, data)
                except Exception as e:
                    return ("error", gse_id, str(e)[:150])

        # Fire all tasks with semaphore limiting concurrency
        # Process in chunks of 100 for checkpoint granularity
        for chunk_start in range(0, len(remaining), 100):
            chunk = remaining[chunk_start:chunk_start + 100]
            tasks = [process_one(gse) for gse in chunk]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                if isinstance(r, Exception):
                    stats["errors"] += 1
                    continue
                status, gse_id, payload = r
                if status == "ok":
                    for s in payload.get("series", []):
                        qc.record_row("geo_series", s)
                        await writer.write("geo_series", s)
                        stats["series"] += 1
                    for s in payload.get("samples", []):
                        qc.record_row("geo_samples", s)
                        await writer.write("geo_samples", s)
                        stats["samples"] += 1
                    for p in payload.get("platforms", []):
                        qc.record_row("geo_platforms", p)
                        await writer.write("geo_platforms", p)
                        stats["platforms"] += 1
                    done.add(gse_id)
                else:
                    stats["errors"] += 1

            processed = chunk_start + len(chunk)

            # Checkpoint + progress every chunk (100)
            save_checkpoint(done)
            await writer.flush_all()
            elapsed = time.time() - start_time
            rate = processed / elapsed * 3600 if elapsed > 0 else 0
            eta = (len(remaining) - processed) / rate if rate > 0 else 0
            logger.info(
                "%d/%d (%.1f%%) | total done: %d/%d | %s | %.0f/hr | ETA: %.1fh",
                processed, len(remaining), processed / len(remaining) * 100,
                len(done), len(all_gse), stats, rate, eta,
            )

    save_checkpoint(done)
    await writer.flush_all()
    qc.save_report(DATA_DIR / "logs" / "geo_qc_report.json")
    qc.print_summary()

    elapsed = time.time() - start_time
    logger.info("Complete in %.1fh: %s (done: %d/%d)", elapsed / 3600, stats, len(done), len(all_gse))


if __name__ == "__main__":
    asyncio.run(main())
