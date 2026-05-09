"""
Downloads the Healthcare Provider Fraud Detection dataset from Kaggle.

Prerequisites:
  - pip install kaggle
  - Kaggle API credentials at ~/.kaggle/kaggle.json
  - You have accepted the dataset's terms (visit dataset page once)

Usage:
  python data/download_data.py
"""
import sys
from pathlib import Path

DATASET = "rohitrox/healthcare-provider-fraud-detection-analysis"
RAW_DIR = Path(__file__).parent / "raw"


def download():
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError:
        print("ERROR: kaggle package not installed. Run: pip install kaggle")
        sys.exit(1)

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Authenticating with Kaggle...")
    api = KaggleApi()
    api.authenticate()

    print(f"Downloading {DATASET} to {RAW_DIR}...")
    api.dataset_download_files(DATASET, path=str(RAW_DIR), unzip=True)

    print(f"\nDownload complete. Files in {RAW_DIR}:")
    for f in sorted(RAW_DIR.glob("*.csv")):
        size_mb = f.stat().st_size / 1024 / 1024
        print(f"  - {f.name} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    download()
