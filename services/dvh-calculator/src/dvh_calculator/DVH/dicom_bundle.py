"""Container for a matched set of RT DICOM files (plan, struct, dose, CT)."""

import logging
from pathlib import Path

from dicompylercore.dicomparser import DicomParser

logger = logging.getLogger(__name__)


class DicomBundle:
    """Group of related RT DICOM files for a single patient and plan."""

    __hash__ = None

    def __init__(self, patient_id, rt_plan: str, rt_struct: str, rt_dose: list, rt_ct: str, read=True):
        self.patient_id = patient_id
        self.rt_plan_path = rt_plan
        self.rt_struct_path: str = rt_struct
        self.rt_ct_path: str | None = rt_ct[: rt_ct.rindex("/") + 1] if rt_ct else None
        if read:
            try:
                self.rt_plan: DicomParser = DicomParser(rt_plan)
                self.rt_struct: DicomParser = DicomParser(rt_struct)
                self.rt_dose: list = [DicomParser(rt) for rt in rt_dose]
                self.rt_dose_path: list = rt_dose
            except Exception:
                logger.exception("Error reading DICOM files")
                raise
        logger.info("Ct path is %s", self.rt_ct_path)

    def __eq__(self, other):
        """Compare bundles by plan, CT, and struct paths."""
        if not isinstance(other, DicomBundle):
            return False
        return (
            self.rt_plan_path == other.rt_plan_path
            and self.rt_ct_path == other.rt_ct_path
            and self.rt_struct_path == other.rt_struct_path
        )

    def rm_data_patient(self):
        """Delete all DICOM files associated with this bundle from disk."""
        try:
            logger.info("Removing data for patient %s", self.patient_id)
            Path(self.rt_plan_path).unlink()
            logger.info("Removing rt plan  %s", self.rt_plan_path)
            Path(self.rt_struct_path).unlink()
            logger.info("Removing data rt struct %s", self.rt_struct_path)
            for rt in self.rt_dose_path:
                logger.info("Removing data rt dose %s", rt)
                Path(rt).unlink()
            if self.rt_plan_path is not None and self.rt_ct_path is not None:
                ct_dir = Path(self.rt_ct_path)
                for f in ct_dir.iterdir():
                    f.unlink()
        except Exception:
            logger.exception("Error deleting files")
