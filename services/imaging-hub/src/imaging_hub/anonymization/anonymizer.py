"""DICOM dataset anonymizer driven by a recipe file, patient lookup CSV, and ROI map."""

import hashlib
import hmac
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from pydicom import dcmread
from pydicom.datadict import add_private_dict_entries

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RecipeRule:
    """Single parsed action from the de-identification recipe file."""

    action: str
    keyword: str
    value: str | None = None


_UID_KEEP_KEYWORDS = frozenset(
    {
        "SOPClassUID",
        "ReferencedSOPClassUID",
        "FailedSOPInstanceUIDList",
        "CodeSetExtensionCreatorUID",
        "TransactionUID",
        "IrradiationEventUID",
        "CreatorVersionUID",
        "SynchronizationFrameOfReferenceUID",
        "ConcatenationUID",
        "DimensionOrganizationUID",
        "PaletteColorLookupTableUID",
        "LargePaletteColorLookupTableUID",
        "TemplateExtensionOrganizationUID",
        "TemplateExtensionCreatorUID",
        "FiducialUID",
        "StorageMediaFileSetUID",
        "DigitalSignatureUID",
        "RelatedFrameOfReferenceUID",
        "DoseReferenceUID",
    }
)

_BURNED_IN_MODALITIES = frozenset({"SC", "US", "XA"})

_WHITELIST_KEYWORDS = frozenset(
    {
        "SpecificCharacterSet",
        "InstanceCreationDate",
        "SOPClassUID",
        "StudyDate",
        "SeriesDate",
        "AcquisitionDate",
        "ContentDate",
        "AcquisitionDatetime",
        "StudyTime",
        "SeriesTime",
        "AcquisitionTime",
        "ContentTime",
        "PatientSex",
        "PatientAge",
        "PatientSize",
        "PatientWeight",
        "BodyPartExamined",
        "PatientIdentityRemoved",
        "PatientName",
        "PatientID",
        "DeidentificationMethod",
        "AccessionNumber",
        "PlacerOrderNumber",
        "FillerOrderNumber",
        "StructureSetLabel",
        "VerifyingObserverName",
        "PersonName",
        "Manufacturer",
        "ReferringPhysicianName",
        "PatientBirthDate",
        "ContrastBolusAgent",
        "StructureSetDate",
        "StructureSetTime",
        "StudyID",
        "StudyDescription",
        "SeriesDescription",
        "SOPInstanceUID",
        "StudyInstanceUID",
        "SeriesInstanceUID",
        "FrameOfReferenceUID",
        "Modality",
        "InstanceNumber",
        "SeriesNumber",
        "AcquisitionNumber",
        "ImageType",
        "PositionReferenceIndicator",
        "ReferencedStudySequence",
        "ReferencedPerformedProcedureStepSequence",
        "ReferencedImageSequence",
        "ReferencedSOPClassUID",
        "ReferencedSOPInstanceUID",
        "SourceImageSequence",
        "ReferencedRTPlanSequence",
        "ReferencedStructureSetSequence",
        "ReferencedFrameOfReferenceSequence",
        "RTReferencedStudySequence",
        "RTReferencedSeriesSequence",
        "BitsAllocated",
        "BitsStored",
        "HighBit",
        "PixelRepresentation",
        "SamplesPerPixel",
        "PhotometricInterpretation",
        "Rows",
        "Columns",
        "NumberOfFrames",
        "PlanarConfiguration",
        "PixelSpacing",
        "ImageOrientationPatient",
        "ImagePositionPatient",
        "SliceThickness",
        "SliceLocation",
        "WindowCenter",
        "WindowWidth",
        "RescaleIntercept",
        "RescaleSlope",
        "RescaleType",
        "SmallestImagePixelValue",
        "LargestImagePixelValue",
        "PixelPaddingValue",
        "LossyImageCompression",
        "LossyImageCompressionRatio",
        "LossyImageCompressionMethod",
        "PixelData",
        "SpacingBetweenSlices",
        "DataCollectionDiameter",
        "ReconstructionDiameter",
        "DistanceSourceToDetector",
        "DistanceSourceToPatient",
        "GantryDetectorTilt",
        "TableHeight",
        "RotationDirection",
        "ExposureTime",
        "XRayTubeCurrent",
        "Exposure",
        "FilterType",
        "GeneratorPower",
        "FocalSpots",
        "ConvolutionKernel",
        "KVP",
        "NumberOfSlices",
        "DataCollectionCenterPatient",
        "ReconstructionTargetCenterPatient",
    }
)

_WHITELIST_GROUPS = frozenset(
    {
        0x3004,
        0x3006,
        0x300A,
        0x300C,
        0x300E,
        0x7FE0,
    }
)


class Anonymizer:
    """Apply recipe-based de-identification, UID hashing, and ROI normalization to DICOM datasets."""

    def __init__(self, settings, recipes_dir):
        from imaging_common import (
            AnonymizationSettings,
        )

        s: AnonymizationSettings = settings
        self.PatientName = s.patient_name
        self.ProfileName = s.profile_name
        self.ProjectName = s.project_name
        self.TrialName = s.trial_name
        self.SiteName = s.site_name
        self.SiteID = s.site_id
        self._uid_secret = s.uid_secret.encode()
        self._uid_prefix = s.uid_prefix

        path_base = Path(recipes_dir)
        self.recipe_path = str(path_base / "recipe.dicom")
        self.patient_lookup_csv = str(path_base / "patient_lookup.csv")
        df = pd.read_csv(self.patient_lookup_csv, dtype=str)
        self._patient_map = dict(zip(df["original"], df["new"], strict=False))

        self.ROI_normalization_path = str(path_base / "ROI_normalization.yaml")
        with Path(self.ROI_normalization_path).open() as f:
            roi_map = yaml.safe_load(f) or {}
        self._compiled_roi_map = {
            canonical: [re.compile(p, re.IGNORECASE) for p in patterns] for canonical, patterns in roi_map.items()
        }

        self._blank_keywords, self._keep_keywords, self._add_rules, self._replace_rules = self._parse_recipe(
            self.recipe_path
        )
        self._keep_set = (
            _WHITELIST_KEYWORDS
            | self._keep_keywords
            | {r.keyword for r in self._add_rules}
            | {r.keyword for r in self._replace_rules}
            | set(self._blank_keywords)
        )
        self._func_registry: dict[str, object] = {
            "hash_func": self.hash_func,
            "CSV_lookup_func": self.csv_lookup_func,
            "DeIdentificationMethod": self.current_date,
        }

        private_entries = {
            0x10011001: ("SH", "1", "ProfileName"),
            0x10031001: ("SH", "1", "ProjectName"),
            0x10051001: ("SH", "1", "TrialName"),
            0x10071001: ("SH", "1", "SiteName"),
            0x10091001: ("SH", "1", "SiteID"),
        }
        add_private_dict_entries("Deid", private_entries)

    def hash_func(self, item, value, field, dicom):
        """Return a truncated HMAC-SHA256 hash of *value*."""
        return hmac.new(self._uid_secret, value.encode(), "sha256").hexdigest()[:16]

    @staticmethod
    def current_date(item, value, field, dicom):  # noqa: ARG004
        """Return a de-identification timestamp string for the current moment."""
        now = datetime.now()
        return f"deid: {now.strftime('%d%m%Y:%H%M%S')}"

    def csv_lookup_func(self, item, value, field, dicom):
        """Look up the replacement value for the dataset's PatientID in the CSV map."""
        patient_id = getattr(dicom, "PatientID", None)
        if patient_id is None:
            raise ValueError("PatientID missing")
        try:
            return self._patient_map[patient_id]
        except KeyError as e:
            raise ValueError(f"PatientID '{patient_id}' not found in patient lookup CSV") from e

    def ROI_normalization(self, rtstruct):
        """Normalize ROI names in all RTSTRUCT files in the folder using the YAML mapping."""
        compiled_map = self._compiled_roi_map

        for roi in rtstruct.StructureSetROISequence:
            original_raw = roi.ROIName
            original = original_raw.strip()

            normalized = None
            for canonical, regex_list in compiled_map.items():
                if any(regex.search(original) for regex in regex_list):
                    normalized = canonical
                    break

            if normalized and original_raw != normalized:
                roi.ROIName = normalized
            elif normalized is None:
                logger.warning("No ROI map found for '%s' in RTSTRUCT dataset", original_raw)

        return rtstruct

    def is_patient_known(self, patient_id: str) -> bool:
        """Return True if *patient_id* exists in the patient lookup CSV."""
        return patient_id in self._patient_map

    @staticmethod
    def _parse_recipe(recipe_path: str):
        blank_keywords: list[str] = []
        keep_keywords: set[str] = set()
        add_rules: list[RecipeRule] = []
        replace_rules: list[RecipeRule] = []

        with Path(recipe_path).open() as f:
            for raw_line in f:
                stripped = raw_line.strip()
                if not stripped or stripped.startswith(("#", "%")) or stripped.upper().startswith("FORMAT"):
                    continue
                parts = stripped.split(None, 2)
                action = parts[0].upper()
                keyword = parts[1] if len(parts) > 1 else ""
                value = parts[2] if len(parts) > 2 else None

                if action == "BLANK":
                    blank_keywords.append(keyword)
                elif action == "KEEP":
                    keep_keywords.add(keyword)
                elif action == "ADD":
                    add_rules.append(RecipeRule(action="ADD", keyword=keyword, value=value))
                elif action == "REPLACE":
                    replace_rules.append(RecipeRule(action="REPLACE", keyword=keyword, value=value))

        logger.info(
            "Parsed recipe: %d BLANK, %d KEEP, %d ADD, %d REPLACE",
            len(blank_keywords),
            len(keep_keywords),
            len(add_rules),
            len(replace_rules),
        )
        return blank_keywords, keep_keywords, add_rules, replace_rules

    def _resolve_value(self, value_spec: str | None, ds) -> str:
        if value_spec is None:
            return ""
        if value_spec.startswith("var:"):
            return getattr(self, value_spec[4:], "")
        if value_spec.startswith("func:"):
            func_name = value_spec[5:]
            func = self._func_registry.get(func_name)
            if func is None:
                logger.warning("Unknown function '%s' in recipe", func_name)
                return ""
            return func(item=None, value=getattr(ds, "PatientID", ""), field=None, dicom=ds)
        return value_spec

    def _inject_private_blocks(self, ds):
        ds.remove_private_tags()
        ds.private_block(0x1001, "Deid", create=True).add_new(0x01, "SH", self.ProfileName)
        ds.private_block(0x1003, "Deid", create=True).add_new(0x01, "SH", self.ProjectName)
        ds.private_block(0x1005, "Deid", create=True).add_new(0x01, "SH", self.TrialName)
        ds.private_block(0x1007, "Deid", create=True).add_new(0x01, "SH", self.SiteName)
        ds.private_block(0x1009, "Deid", create=True).add_new(0x01, "SH", self.SiteID)

    def _remove_non_whitelisted(self, ds):
        to_delete = []
        for elem in ds:
            if elem.tag.is_private:
                continue
            if elem.VR == "UI":
                continue
            if elem.tag.group in _WHITELIST_GROUPS:
                if elem.VR == "SQ" and elem.value:
                    for item in elem.value:
                        self._remove_non_whitelisted(item)
                continue
            if (elem.keyword or "") in self._keep_set:
                if elem.VR == "SQ" and elem.value:
                    for item in elem.value:
                        self._remove_non_whitelisted(item)
                continue
            to_delete.append(elem.tag)
        for tag in to_delete:
            del ds[tag]

    def anonymize_dataset(self, ds):
        """Strip non-whitelisted elements, apply recipe rules, hash UIDs, and inject private tags."""
        self._remove_non_whitelisted(ds)

        for kw in self._blank_keywords:
            if kw in ds:
                ds[kw].value = None

        for rule in self._add_rules:
            value = self._resolve_value(rule.value, ds)
            setattr(ds, rule.keyword, value)

        for rule in self._replace_rules:
            if rule.keyword in ds:
                value = self._resolve_value(rule.value, ds)
                ds[rule.keyword].value = value

        self._replace_uids(ds)
        self._inject_private_blocks(ds)

        return ds

    def _warn_burned_in(self, ds):
        modality = getattr(ds, "Modality", "")
        burned_in = getattr(ds, "BurnedInAnnotation", None)
        if burned_in == "YES" or modality in _BURNED_IN_MODALITIES:
            sop_uid = getattr(ds, "SOPInstanceUID", "UNKNOWN")
            logger.warning(
                "Potential burned-in annotation: SOP %s, Modality=%s, BurnedInAnnotation=%s",
                sop_uid,
                modality,
                burned_in,
            )

    def run_dataset(self, file_path: str):
        """Read a DICOM file, anonymize it, and return the modified Dataset (or None on failure)."""
        try:
            ds = dcmread(file_path)
            self._warn_burned_in(ds)

            if getattr(ds, "Modality", None) == "RTSTRUCT":
                self.ROI_normalization(ds)

            ds = self.anonymize_dataset(ds)
            if ds is None:
                logger.error("Anonymization failed, returning None")
                return None
        except Exception:
            logger.exception("Error in run_dataset()")
            return None
        else:
            logger.info("File anonymised successfully (fast path)")
            return ds

    def _hash_uid(self, original_uid: str) -> str:
        h = hashlib.sha256(self._uid_secret + original_uid.encode()).digest()
        numeric = str(int.from_bytes(h[:16], "big"))
        return f"{self._uid_prefix}{numeric}"[:64]

    def _replace_uids(self, ds):
        for elem in ds:
            if elem.VR == "UI" and (elem.keyword or "") not in _UID_KEEP_KEYWORDS:
                if isinstance(elem.value, str) and elem.value:
                    elem.value = self._hash_uid(elem.value)
            elif elem.VR == "SQ" and elem.value:
                for item in elem.value:
                    self._replace_uids(item)
