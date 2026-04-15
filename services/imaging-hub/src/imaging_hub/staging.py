"""Encrypted staging layer for incoming DICOM files using tmpfs with disk overflow."""

import logging
import shutil
import tempfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

from cryptography.fernet import Fernet
from pydicom import Dataset

logger = logging.getLogger(__name__)


@dataclass
class StagedFile:
    """Reference to a staged DICOM file, potentially encrypted on overflow disk."""

    path: str
    encrypted: bool


class StagingManager:
    """Write incoming DICOM to fast tmpfs, falling back to encrypted overflow when space is low."""

    def __init__(self, tmpfs_dir: str, overflow_dir: str, tmpfs_threshold_pct: int = 85, encrypt_overflow: bool = True):
        self._tmpfs_dir = Path(tmpfs_dir)
        self._overflow_dir = Path(overflow_dir)
        self._threshold = tmpfs_threshold_pct / 100
        self._encrypt = encrypt_overflow
        self._fernet = Fernet(Fernet.generate_key()) if encrypt_overflow else None

        self._tmpfs_dir.mkdir(parents=True, exist_ok=True)
        self._overflow_dir.mkdir(parents=True, exist_ok=True)

        for child in self._overflow_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        logger.info("Wiped stale overflow contents in %s", self._overflow_dir)

        logger.info(
            "StagingManager ready — tmpfs=%s (threshold %d%%), overflow=%s (encrypted=%s)",
            self._tmpfs_dir,
            tmpfs_threshold_pct,
            self._overflow_dir,
            self._encrypt,
        )

    def stage(self, ds: Dataset, assoc_id: str, sop_uid: str) -> StagedFile:
        """Write a DICOM dataset to tmpfs (or encrypted overflow) and return a StagedFile handle."""
        if self._tmpfs_has_space():
            directory = self._tmpfs_dir / assoc_id
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"{sop_uid}.dcm"
            try:
                ds.save_as(str(path), write_like_original=False)
                return StagedFile(path=str(path), encrypted=False)
            except OSError:
                path.unlink(missing_ok=True)
                logger.debug("tmpfs write failed (race), falling back to overflow")

        return self._stage_overflow(ds, assoc_id, sop_uid)

    def _stage_overflow(self, ds: Dataset, assoc_id: str, sop_uid: str) -> StagedFile:
        directory = self._overflow_dir / assoc_id
        directory.mkdir(parents=True, exist_ok=True)
        if self._encrypt:
            path = directory / f"{sop_uid}.dcm.enc"
            buf = BytesIO()
            ds.save_as(buf, write_like_original=False)
            path.write_bytes(self._fernet.encrypt(buf.getvalue()))
            logger.debug("Overflow: encrypted staging to %s", path)
            return StagedFile(path=str(path), encrypted=True)
        path = directory / f"{sop_uid}.dcm"
        ds.save_as(str(path), write_like_original=False)
        logger.debug("Overflow: unencrypted staging to %s", path)
        return StagedFile(path=str(path), encrypted=False)

    def read_to_tempfile(self, staged: StagedFile) -> str:
        """Return a readable file path, decrypting to a temp file if necessary."""
        if not staged.encrypted:
            return staged.path
        encrypted_bytes = Path(staged.path).read_bytes()
        decrypted = self._fernet.decrypt(encrypted_bytes)
        with tempfile.NamedTemporaryFile(suffix=".dcm", delete=False) as tmp:
            tmp.write(decrypted)
            return tmp.name

    def cleanup(self, staged: StagedFile, temp_path: str | None = None):
        """Remove the staged file (and optional temp copy) from disk."""
        Path(staged.path).unlink(missing_ok=True)
        parent = Path(staged.path).parent
        try:
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
        except OSError:
            pass
        if temp_path and temp_path != staged.path:
            Path(temp_path).unlink(missing_ok=True)

    def _tmpfs_has_space(self) -> bool:
        try:
            usage = shutil.disk_usage(self._tmpfs_dir)
            return usage.used / usage.total < self._threshold
        except OSError:
            return False
