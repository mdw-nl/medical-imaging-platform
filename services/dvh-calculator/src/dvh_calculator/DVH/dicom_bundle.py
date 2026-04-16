"""Container for a matched set of RT DICOM files (plan, struct, dose, CT)."""

import logging

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
