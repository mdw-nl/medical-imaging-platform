"""Runtime settings loaded from environment variables."""

import os
from pathlib import Path

BASE_DIR = os.getenv("DATA_DIR", str(Path(__file__).parents[1].resolve() / "data"))

USE_NIFTI = os.getenv("USE_NIFTI", "true").strip().lower() in ("1", "true", "yes", "y", "on")
DEFER_NIFTI = os.getenv("DEFER_NIFTI", "true").strip().lower() in ("1", "true", "yes", "y", "on")

STAGING_TMPFS_DIR = os.getenv("STAGING_TMPFS_DIR", "/dicom-staging")
STAGING_OVERFLOW_DIR = os.getenv("STAGING_OVERFLOW_DIR", "/dicom-staging-overflow")
STAGING_TMPFS_THRESHOLD_PCT = int(os.getenv("STAGING_TMPFS_THRESHOLD_PCT", "85"))
