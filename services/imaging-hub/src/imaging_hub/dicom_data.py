"""Extract and persist DICOM metadata from anonymized datasets."""

from dataclasses import dataclass
from pathlib import Path

from pydicom import Dataset
from pydicom.valuerep import PersonName

from imaging_hub.settings import BASE_DIR


@dataclass(frozen=True, slots=True)
class DicomMetadata:
    """Immutable set of DICOM header fields extracted after anonymization."""

    patient_name: str
    patient_id: str
    study_uid: str
    series_uid: str
    modality: str
    sop_uid: str
    sop_class_uid: str
    instance_number: int | str
    modality_type: str
    referenced_rt_plan_uid: str
    referenced_sop_class_uid: str
    referenced_rtstruct_sop_uid: str
    referenced_ct_series_uid: str


def return_dicom_data(ds: Dataset) -> DicomMetadata:
    """Extract relevant metadata fields from a pydicom Dataset into a DicomMetadata instance."""
    patient_name = ds.PatientName if "PatientName" in ds else "UNKNOWN"
    if isinstance(patient_name, PersonName):
        patient_name = str(patient_name)

    patient_id = ds.PatientID if "PatientID" in ds else "UNKNOWN"
    study_uid = ds.StudyInstanceUID if "StudyInstanceUID" in ds else "UNKNOWN"
    series_uid = ds.SeriesInstanceUID if "SeriesInstanceUID" in ds else "UNKNOWN"
    modality = ds.Modality if "Modality" in ds else "UNKNOWN"
    sop_uid = ds.SOPInstanceUID if "SOPInstanceUID" in ds else "UNKNOWN"
    sop_class_uid = ds.SOPClassUID if "SOPClassUID" in ds else "UNKNOWN"
    instance_number = ds.InstanceNumber if "InstanceNumber" in ds else "UNKNOWN"
    instance_number = "UNKNOWN" if instance_number is None or instance_number == "UNKNOWN" else int(instance_number)
    modality_type = ds.get("ModalityType", "UNKNOWN")
    referenced_rt_plan_seq = ds.get("ReferencedRTPlanSequence", [{}])
    referenced_rt_plan_uid = (
        referenced_rt_plan_seq[0].get("ReferencedSOPInstanceUID", "UNKNOWN") if referenced_rt_plan_seq else "UNKNOWN"
    )
    referenced_sop_class_uid = (
        referenced_rt_plan_seq[0].get("ReferencedSOPClassUID", "UNKNOWN") if referenced_rt_plan_seq else "UNKNOWN"
    )

    referenced_rtstruct_seq = ds.get("ReferencedStructureSetSequence", [{}])
    referenced_rtstruct_sop_uid = (
        referenced_rtstruct_seq[0].get("ReferencedSOPInstanceUID", "UNKNOWN") if referenced_rtstruct_seq else "UNKNOWN"
    )

    referenced_ct_series_uid = "UNKNOWN"
    for frame_ref in ds.get("ReferencedFrameOfReferenceSequence", []):
        for study_ref in frame_ref.get("RTReferencedStudySequence", []):
            for series_ref in study_ref.get("RTReferencedSeriesSequence", []):
                uid = series_ref.get("SeriesInstanceUID", None)
                if uid:
                    referenced_ct_series_uid = str(uid)
                    break
            if referenced_ct_series_uid != "UNKNOWN":
                break
        if referenced_ct_series_uid != "UNKNOWN":
            break

    return DicomMetadata(
        patient_name=patient_name,
        patient_id=patient_id,
        study_uid=study_uid,
        series_uid=series_uid,
        modality=modality,
        sop_uid=sop_uid,
        sop_class_uid=sop_class_uid,
        instance_number=instance_number,
        modality_type=modality_type,
        referenced_rt_plan_uid=referenced_rt_plan_uid,
        referenced_sop_class_uid=referenced_sop_class_uid,
        referenced_rtstruct_sop_uid=referenced_rtstruct_sop_uid,
        referenced_ct_series_uid=referenced_ct_series_uid,
    )


def create_folder(patient_id, study_uid, modality, sop_uid):
    """Create the patient/study/modality directory tree and return the target file path."""
    patient_folder = Path(BASE_DIR) / patient_id / study_uid / modality
    patient_folder.mkdir(parents=True, exist_ok=True)
    return str(patient_folder / f"{sop_uid}.dcm")
