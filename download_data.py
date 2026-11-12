"""Download the pinned arXiv metadata Parquet snapshot and write a manifest.

Source chain: arXiv -> Cornell University's Kaggle "arXiv metadata" dataset ->
librarian-bots/arxiv-metadata-snapshot (Parquet mirror on Hugging Face).

This script only fetches files. All cleaning and analysis happens in Spark
(pipeline.py). The revision is pinned so the run is reproducible even if the
mirror's `main` branch is updated later.

Usage:
    python download_data.py            # download (skips files already complete)
    python download_data.py --check    # only re-measure local files / manifest
"""

import argparse
import datetime as dt
import json
import shutil
from pathlib import Path

from huggingface_hub import HfApi, snapshot_download

REPO_ID = "librarian-bots/arxiv-metadata-snapshot"
REVISION = "c428226b689a0e17db2cd55265133b9eca15d97f"
EXPECTED_ROWS = 3_224_635          # from the dataset card at this revision
EXPECTED_DOWNLOAD_SIZE = 2_700_966_734
EXPECTED_FILES = 11

ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "data" / "raw" / "arxiv-metadata-snapshot"
MANIFEST = ROOT / "metadata" / "data_manifest.json"
CARD_COPY = ROOT / "metadata" / "dataset_card_README.md"


def remote_file_sizes():
    """Return {relative_path: size} for the pinned revision (from the Hub API)."""
    info = HfApi().dataset_info(REPO_ID, revision=REVISION, files_metadata=True)
    assert info.sha == REVISION, f"Hub returned revision {info.sha}"
    return {s.rfilename: s.size for s in info.siblings}, info


def download():
    snapshot_download(
        repo_id=REPO_ID,
        repo_type="dataset",
        revision=REVISION,
        allow_patterns=["data/*.parquet", "README.md"],
        local_dir=RAW_DIR,
        max_workers=4,
    )


def build_manifest(remote_sizes, info, download_date):
    parquet_files = sorted(RAW_DIR.glob("data/*.parquet"))
    files = []
    for f in parquet_files:
        rel = f.relative_to(RAW_DIR).as_posix()
        local_size = f.stat().st_size
        files.append({
            "path": f"data/raw/arxiv-metadata-snapshot/{rel}",
            "local_bytes": local_size,
            "source_declared_bytes": remote_sizes.get(rel),
            "size_matches_source": local_size == remote_sizes.get(rel),
        })
    total = sum(x["local_bytes"] for x in files)

    old = json.loads(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.exists() else {}
    spark_measured = old.get("locally_measured", {}).get("spark_row_count")

    card = info.card_data.to_dict() if info.card_data else {}
    split = (card.get("dataset_info") or {}).get("splits", [{}])[0]
    return {
        "dataset": "arXiv paper metadata (Parquet mirror)",
        "repo_id": REPO_ID,
        "repo_type": "dataset",
        "revision": REVISION,
        "revision_last_modified": str(info.last_modified),
        "source_urls": {
            "upstream_kaggle": "https://www.kaggle.com/datasets/Cornell-University/arxiv",
            "mirror": f"https://huggingface.co/datasets/{REPO_ID}",
            "pinned_commit": f"https://huggingface.co/datasets/{REPO_ID}/commit/{REVISION}",
            "pinned_tree": f"https://huggingface.co/datasets/{REPO_ID}/tree/{REVISION}/data",
        },
        "license_metadata": card.get("license"),
        "license_note": ("CC0-1.0 applies to the metadata records in this mirror (per its "
                         "dataset card). Each paper's full text has its own license; no "
                         "full texts are downloaded by this project."),
        "download_date_local": download_date,
        "source_declared": {
            "num_rows": split.get("num_examples"),
            "download_size_bytes": card.get("dataset_info", {}).get("download_size"),
            "num_parquet_files": sum(1 for k in remote_sizes if k.endswith(".parquet")),
            "parquet_bytes_sum_from_hub_api": sum(v for k, v in remote_sizes.items()
                                                  if k.endswith(".parquet")),
        },
        "locally_measured": {
            "num_parquet_files": len(files),
            "parquet_total_bytes": total,
            "parquet_total_gb_decimal": round(total / 1e9, 3),
            "parquet_total_gib": round(total / 2**30, 3),
            "spark_row_count": spark_measured,  # filled in by pipeline.py
        },
        "files": files,
        "note": ("Only real .parquet data files are counted (no cache, outputs, "
                 "event logs or the data card)."),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="skip download")
    args = parser.parse_args()

    free_gb = shutil.disk_usage(ROOT).free / 1e9
    print(f"Free disk space: {free_gb:.1f} GB")
    if free_gb < 10 and not args.check:
        raise SystemExit("Need at least ~10 GB free (raw data + outputs).")

    remote_sizes, info = remote_file_sizes()
    if not args.check:
        print(f"Downloading {REPO_ID}@{REVISION} -> {RAW_DIR.relative_to(ROOT).as_posix()}")
        download()

    download_date = dt.date.today().isoformat()
    if args.check and MANIFEST.exists():
        download_date = json.loads(MANIFEST.read_text(encoding="utf-8")).get(
            "download_date_local", download_date)
    manifest = build_manifest(remote_sizes, info, download_date)

    # Keep a small copy of the pinned data card (license + schema) in metadata/.
    card_src = RAW_DIR / "README.md"
    if card_src.exists():
        shutil.copyfile(card_src, CARD_COPY)

    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    m = manifest["locally_measured"]
    print(f"Parquet files: {m['num_parquet_files']}  total bytes: {m['parquet_total_bytes']:,} "
          f"({m['parquet_total_gb_decimal']} GB)")
    assert m["num_parquet_files"] == EXPECTED_FILES, "unexpected number of shards"
    assert all(f["size_matches_source"] for f in manifest["files"]), "size mismatch vs Hub"
    assert m["parquet_total_bytes"] > 1_000_000_000, "dataset must be > 1 GB"
    print(f"Manifest written to {MANIFEST.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
