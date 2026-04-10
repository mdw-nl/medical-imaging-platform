import hashlib
import logging

from pydicom.dataset import Dataset
from pydicom.sequence import Sequence
from pydicom.uid import ExplicitVRLittleEndian


def test_kept_tags_survive(anonymizer, make_dataset):
    ds = make_dataset(
        PatientSex="F",
        PatientAge="040Y",
        BodyPartExamined="HEAD",
        StudyDate="20240315",
        SeriesDate="20240315",
        ImageType=["ORIGINAL", "PRIMARY"],
    )
    result = anonymizer.anonymize_dataset(ds)
    assert result.PatientSex == "F"
    assert result.PatientAge == "040Y"
    assert result.BodyPartExamined == "HEAD"
    assert result.StudyDate == "20240315"
    assert result.Rows == 512
    assert result.Columns == 512
    assert result.BitsAllocated == 16


def test_non_whitelisted_tags_removed(anonymizer, make_dataset):
    ds = make_dataset(
        InstitutionName="Secret Hospital",
        PatientAddress="123 Main St",
        PhysicianOfRecord="DR^SMITH",
        OperatorName="TECH^BOB",
        ReferringPhysicianAddress="456 Oak Ave",
        DeviceSerialNumber="SN12345",
        StationName="CT_SCANNER_1",
    )
    result = anonymizer.anonymize_dataset(ds)
    assert "InstitutionName" not in result
    assert "PatientAddress" not in result
    assert "PhysicianOfRecord" not in result
    assert "OperatorName" not in result
    assert "ReferringPhysicianAddress" not in result
    assert "DeviceSerialNumber" not in result
    assert "StationName" not in result


def test_private_tags_removed(anonymizer, make_dataset):
    ds = make_dataset()
    ds.add_new(0x00091001, "LO", "VendorPrivateData")
    result = anonymizer.anonymize_dataset(ds)
    assert 0x00091001 not in result
    block = result.private_block(0x1001, "Deid")
    assert block[0x01].value == "TEST_PROFILE"
    block3 = result.private_block(0x1003, "Deid")
    assert block3[0x01].value == "TEST_PROJECT"


def test_uids_replaced(anonymizer, make_dataset):
    ds = make_dataset()
    original_study_uid = ds.StudyInstanceUID
    original_series_uid = ds.SeriesInstanceUID
    original_sop_class_uid = ds.SOPClassUID
    result = anonymizer.anonymize_dataset(ds)
    assert result.StudyInstanceUID != original_study_uid
    assert result.SeriesInstanceUID != original_series_uid
    assert result.StudyInstanceUID.startswith("99999.")
    assert result.SOPClassUID == original_sop_class_uid


def test_add_rules_applied(anonymizer, make_dataset):
    ds = make_dataset()
    result = anonymizer.anonymize_dataset(ds)
    assert result.PatientIdentityRemoved == "YES"
    assert result.PatientName == "TEST_PATIENT"
    assert result.PatientID == "PYTIM05"
    assert str(result.DeidentificationMethod).startswith("deid:")


def test_replace_rules_applied(anonymizer, make_dataset):
    ds = make_dataset(AccessionNumber="ORIG_ACC_123")
    result = anonymizer.anonymize_dataset(ds)
    assert result.AccessionNumber != "ORIG_ACC_123"
    assert len(result.AccessionNumber) == 16


def test_blank_rules_applied(anonymizer, make_dataset):
    ds = make_dataset(
        Manufacturer="SIEMENS",
        ReferringPhysicianName="DR^JONES",
        StudyDescription="Patient follow-up scan",
        SeriesDescription="Chest CT with contrast",
        StudyID="STUDY001",
    )
    result = anonymizer.anonymize_dataset(ds)
    assert result.Manufacturer is None
    assert result.ReferringPhysicianName is None
    assert result.StudyDescription is None
    assert result.SeriesDescription is None
    assert result.StudyID is None


def test_nested_sequence_phi_removed(anonymizer, make_dataset):
    ds = make_dataset()
    ref_seq_item = Dataset()
    ref_seq_item.ReferencedSOPInstanceUID = "1.2.3.999"
    ref_seq_item.ReferencedSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    ref_seq_item.PhysicianOfRecord = "DR^HIDDEN"
    ds.ReferencedStudySequence = Sequence([ref_seq_item])
    result = anonymizer.anonymize_dataset(ds)
    assert "ReferencedStudySequence" in result
    assert len(result.ReferencedStudySequence) == 1
    nested = result.ReferencedStudySequence[0]
    assert "PhysicianOfRecord" not in nested
    assert "ReferencedSOPInstanceUID" in nested


def test_rtstruct_contours_preserved(anonymizer, make_dataset):
    roi_seq_item = Dataset()
    roi_seq_item.ROINumber = 1
    roi_seq_item.ROIName = "LUNG_L"
    roi_seq_item.ROIGenerationAlgorithm = "AUTOMATIC"

    contour_item = Dataset()
    contour_item.ContourGeometricType = "CLOSED_PLANAR"
    contour_item.NumberOfContourPoints = 3
    contour_item.ContourData = [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0]
    roi_contour_item = Dataset()
    roi_contour_item.ReferencedROINumber = 1
    roi_contour_item.ContourSequence = Sequence([contour_item])

    ds = make_dataset(
        Modality="RTSTRUCT",
        SOPClassUID="1.2.840.10008.5.1.4.1.1.481.3",
        StructureSetROISequence=Sequence([roi_seq_item]),
        ROIContourSequence=Sequence([roi_contour_item]),
    )
    result = anonymizer.anonymize_dataset(ds)
    assert "ROIContourSequence" in result
    assert "StructureSetROISequence" in result
    contour = result.ROIContourSequence[0].ContourSequence[0]
    assert contour.ContourGeometricType == "CLOSED_PLANAR"
    assert contour.NumberOfContourPoints == 3


def test_pixel_data_preserved(anonymizer, make_dataset):
    ds = make_dataset(PixelData=b"\x00" * 512 * 512 * 2)
    result = anonymizer.anonymize_dataset(ds)
    assert "PixelData" in result
    assert len(result.PixelData) == 512 * 512 * 2
    assert result.Rows == 512
    assert result.Columns == 512
    assert result.BitsAllocated == 16


def test_burned_in_annotation_warning(anonymizer, make_dataset, caplog, tmp_path):
    ds = make_dataset(BurnedInAnnotation="YES")
    path = str(tmp_path / "burned_in.dcm")
    ds.save_as(path, write_like_original=False)
    with caplog.at_level(logging.WARNING):
        anonymizer.run_dataset(path)
    assert "Potential burned-in annotation" in caplog.text
    assert "BurnedInAnnotation=YES" in caplog.text


def test_burned_in_modality_warning(anonymizer, make_dataset, caplog, tmp_path):
    ds = make_dataset(
        Modality="SC",
        SOPClassUID="1.2.840.10008.5.1.4.1.1.7",
    )
    path = str(tmp_path / "burned_in_sc.dcm")
    ds.save_as(path, write_like_original=False)
    with caplog.at_level(logging.WARNING):
        anonymizer.run_dataset(path)
    assert "Potential burned-in annotation" in caplog.text
    assert "Modality=SC" in caplog.text


def test_hash_uses_hmac(anonymizer):
    plain_md5 = hashlib.md5(b"ACC123456").hexdigest()[:16]  # noqa: S324
    result = anonymizer.hash_func(item=None, value="ACC123456", field=None, dicom=None)
    assert result != plain_md5
    assert len(result) == 16
    result2 = anonymizer.hash_func(item=None, value="ACC123456", field=None, dicom=None)
    assert result == result2


def test_unknown_patient_rejected(anonymizer):
    assert anonymizer.is_patient_known("PYTIM05") is True
    assert anonymizer.is_patient_known("UNKNOWN_PATIENT_999") is False


def test_unknown_tag_removed(anonymizer, make_dataset):
    ds = make_dataset(ResponsibleOrganization="Secret Corp")
    result = anonymizer.anonymize_dataset(ds)
    assert "ResponsibleOrganization" not in result


def test_rt_dose_group_preserved(anonymizer, make_dataset):
    ds = make_dataset(
        Modality="RTDOSE",
        SOPClassUID="1.2.840.10008.5.1.4.1.1.481.2",
        DoseGridScaling=0.001,
        DoseUnits="GY",
        DoseType="PHYSICAL",
        DoseSummationType="PLAN",
    )
    ds.file_meta = Dataset()
    ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds.file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
    ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
    result = anonymizer.anonymize_dataset(ds)
    assert result.DoseGridScaling == 0.001
    assert result.DoseUnits == "GY"
    assert result.DoseType == "PHYSICAL"
    assert result.DoseSummationType == "PLAN"
