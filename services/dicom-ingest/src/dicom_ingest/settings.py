import os
from pathlib import Path

SCP_AE_TITLE = "MY_SCP"

BASE_DIR = os.getenv("DATA_DIR", str(Path(__file__).parents[1].resolve() / "data"))

USE_NIFTI = os.getenv("USE_NIFTI", "").strip().lower() in ("1", "true", "yes", "y", "on")
USE_FAST_ANONYMIZER = os.getenv("USE_FAST_ANONYMIZER", "true").strip().lower() in ("1", "true", "yes", "y", "on")

STAGING_TMPFS_DIR = os.getenv("STAGING_TMPFS_DIR", "/dicom-staging")
STAGING_OVERFLOW_DIR = os.getenv("STAGING_OVERFLOW_DIR", "/dicom-staging-overflow")
STAGING_TMPFS_THRESHOLD_PCT = int(os.getenv("STAGING_TMPFS_THRESHOLD_PCT", "85"))
