"""Standalone SRA metadata dump downloader with progress bar.

Usage:
    python scripts/download_sra.py

Downloads NCBI_SRA_Metadata_Full_*.tar.gz to data/downloads/sra/
Supports resume if interrupted.
"""
import ftplib
import sys
from pathlib import Path

FTP_HOST = "ftp.ncbi.nlm.nih.gov"
METADATA_PATH = "/sra/reports/Metadata"
OUTPUT_DIR = Path("data/downloads/sra")


def download():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to {FTP_HOST}...")
    ftp = ftplib.FTP(FTP_HOST, timeout=120)
    ftp.login()

    # Find latest full dump
    files = ftp.nlst(METADATA_PATH)
    full_dumps = sorted(f for f in files if "Full" in f and f.endswith(".tar.gz"))
    if not full_dumps:
        print("ERROR: No Full dump found!")
        sys.exit(1)

    target = full_dumps[-1]
    filename = target.split("/")[-1]
    local_path = OUTPUT_DIR / filename
    print(f"Target: {target}")

    # Get remote size
    ftp.sendcmd("TYPE I")
    remote_size = ftp.size(target)
    print(f"Remote size: {remote_size:,} bytes ({remote_size/1024/1024/1024:.2f} GB)")

    # Resume support
    offset = 0
    mode = "wb"
    if local_path.exists():
        local_size = local_path.stat().st_size
        if local_size >= remote_size:
            print(f"Already complete: {local_path}")
            ftp.quit()
            return str(local_path)
        offset = local_size
        mode = "ab"
        print(f"Resuming from {local_size:,} bytes ({local_size/remote_size*100:.1f}%)")

    downloaded = offset
    last_pct = -1

    def callback(chunk: bytes):
        nonlocal downloaded, last_pct
        fp.write(chunk)
        downloaded += len(chunk)
        pct = int(downloaded / remote_size * 100)
        if pct != last_pct:
            gb = downloaded / 1024 / 1024 / 1024
            total_gb = remote_size / 1024 / 1024 / 1024
            bar = "=" * (pct // 2) + ">" + " " * (50 - pct // 2)
            print(f"\r[{bar}] {pct}% ({gb:.1f}/{total_gb:.1f} GB)", end="", flush=True)
            last_pct = pct

    print("Downloading...")
    ftp.voidcmd("TYPE I")
    with open(local_path, mode) as fp:
        if offset > 0:
            ftp.retrbinary(f"RETR {target}", callback, rest=offset)
        else:
            ftp.retrbinary(f"RETR {target}", callback)

    print(f"\nDone! {local_path} ({downloaded:,} bytes)")
    ftp.quit()

    # Verify
    actual = local_path.stat().st_size
    if actual == remote_size:
        print("Checksum OK (size match)")
    else:
        print(f"WARNING: size mismatch! local={actual:,} remote={remote_size:,}")

    return str(local_path)


if __name__ == "__main__":
    download()
