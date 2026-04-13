import shutil
from pathlib import Path

import pytest
from pydicom.dataset import Dataset
from pydicom.uid import ExplicitVRLittleEndian

from imaging_common import AnonymizationSettings
from imaging_hub.anonymization import Anonymizer

RECIPES_DIR = Path(__file__).resolve().parents[1] / "recipes"


@pytest.fixture
def anon_settings():
    return AnonymizationSettings(
        patient_name="TEST_PATIENT",
        profile_name="TEST_PROFILE",
        project_name="TEST_PROJECT",
        trial_name="TEST_TRIAL",
        site_name="TEST_SITE",
        site_id="1",
        uid_secret="test-secret-key-do-not-use-in-prod",
        uid_prefix="99999.",
    )


@pytest.fixture
def recipes_dir(tmp_path):
    dest = tmp_path / "recipes"
    shutil.copytree(RECIPES_DIR, dest)
    with (dest / "patient_lookup.csv").open("a") as f:
        f.write("PYTIM05,PYTIM05\n")
    return dest


@pytest.fixture
def anonymizer(anon_settings, recipes_dir):
    return Anonymizer(settings=anon_settings, recipes_dir=recipes_dir)


@pytest.fixture
def make_dataset():
    def _factory(**overrides):
        ds = Dataset()
        ds.file_meta = Dataset()
        ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
        ds.file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
        ds.file_meta.MediaStorageSOPInstanceUID = "1.2.3.4.5.6.7.8.9"
        ds.is_implicit_VR = False
        ds.is_little_endian = True

        ds.PatientName = "DOE^JOHN"
        ds.PatientID = "PYTIM05"
        ds.StudyInstanceUID = "1.2.3.100"
        ds.SeriesInstanceUID = "1.2.3.200"
        ds.SOPInstanceUID = "1.2.3.300"
        ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
        ds.Modality = "CT"
        ds.InstanceNumber = 1
        ds.StudyDate = "20240101"
        ds.SeriesDate = "20240101"
        ds.PatientSex = "M"
        ds.PatientAge = "065Y"
        ds.Rows = 512
        ds.Columns = 512
        ds.BitsAllocated = 16
        ds.BitsStored = 12
        ds.HighBit = 11
        ds.PixelRepresentation = 0
        ds.SamplesPerPixel = 1
        ds.AccessionNumber = "ACC123456"

        for key, value in overrides.items():
            setattr(ds, key, value)
        return ds

    return _factory
