import contextlib
import gc
import hashlib
import logging
import os
import re
import tempfile
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from deid.config import DeidRecipe
from deid.dicom import get_identifiers, replace_identifiers
from pydicom import dcmread
from pydicom.datadict import add_private_dict_entries

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RecipeRule:
    action: str
    keyword: str
    value: str | None = None


@contextlib.contextmanager
def suppress_output() -> Generator[None, None, None]:
    devnull = Path(os.devnull)
    with (
        devnull.open("w") as out,
        devnull.open("w") as err,
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        yield


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


class Anonymizer:
    def __init__(self, path_files="dicomsorter/dicomsorter/recipes/"):
        path_base = Path(path_files)
        path_var = path_base / "variables.yaml"
        with path_var.open() as f:
            config_data = yaml.safe_load(f)

        variables = config_data.get("variables", {})
        self.PatientName = variables.get("PatientName")
        self.ProfileName = variables.get("ProfileName")
        self.ProjectName = variables.get("ProjectName")
        self.TrialName = variables.get("TrialName")
        self.SiteName = variables.get("SiteName")
        self.SiteID = variables.get("SiteID")
        self._uid_secret = variables.get("UIDSecret", "digione-default-secret").encode()
        self._uid_prefix = variables.get("UIDPrefix", "99999.")

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

        self._recipe = DeidRecipe(deid=self.recipe_path)

        self._remove_keywords, self._blank_keywords, self._keep_keywords, self._add_rules, self._replace_rules = (
            self._parse_recipe(self.recipe_path)
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

    @staticmethod
    def hash_func(item, value, field, dicom):  # noqa: ARG004
        return hashlib.md5(value.encode()).hexdigest()[:16]  # noqa: S324

    @staticmethod
    def patient_mapping(csv_path):
        df = pd.read_csv(csv_path)

        def lookup(item, value, field, dicom):
            patient_id = dicom.PatientID
            matched = df.loc[df["original"] == patient_id, "new"]
            if matched.empty:
                raise ValueError(
                    f"PatientID: '{patient_id}' not found in patient lookup CSV. Stopping the pipeline for this patient"
                )
            return matched.to_numpy()[0]

        return lookup

    @staticmethod
    def current_date(field, value, item, dicom):  # noqa: ARG004
        now = datetime.now()
        return f"deid: {now.strftime('%d%m%Y:%H%M%S')}"

    def csv_lookup_func(self, item, value, field, dicom):
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
        return patient_id in self._patient_map

    @staticmethod
    def _parse_recipe(recipe_path: str):
        remove_keywords: list[str] = []
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

                if action == "REMOVE":
                    remove_keywords.append(keyword)
                elif action == "BLANK":
                    blank_keywords.append(keyword)
                elif action == "KEEP":
                    keep_keywords.add(keyword)
                elif action == "ADD":
                    add_rules.append(RecipeRule(action="ADD", keyword=keyword, value=value))
                elif action == "REPLACE":
                    replace_rules.append(RecipeRule(action="REPLACE", keyword=keyword, value=value))

        logger.info(
            "Parsed recipe: %d REMOVE, %d BLANK, %d KEEP, %d ADD, %d REPLACE",
            len(remove_keywords),
            len(blank_keywords),
            len(keep_keywords),
            len(add_rules),
            len(replace_rules),
        )
        return remove_keywords, blank_keywords, keep_keywords, add_rules, replace_rules

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

    def anonymize_dataset(self, ds):
        rtstruct_contours = None
        if getattr(ds, "Modality", None) == "RTSTRUCT" and hasattr(ds, "ROIContourSequence"):
            rtstruct_contours = ds.ROIContourSequence
            del ds.ROIContourSequence

        for kw in self._remove_keywords:
            if kw in ds:
                del ds[kw]

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

        ds.remove_private_tags()
        ds.private_block(0x1001, "Deid", create=True).add_new(0x01, "SH", self.ProfileName)
        ds.private_block(0x1003, "Deid", create=True).add_new(0x01, "SH", self.ProjectName)
        ds.private_block(0x1005, "Deid", create=True).add_new(0x01, "SH", self.TrialName)
        ds.private_block(0x1007, "Deid", create=True).add_new(0x01, "SH", self.SiteName)
        ds.private_block(0x1009, "Deid", create=True).add_new(0x01, "SH", self.SiteID)

        if rtstruct_contours is not None:
            ds.ROIContourSequence = rtstruct_contours

        return ds

    def run_dataset(self, file_path: str):
        try:
            ds = dcmread(file_path)

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

    def anonymize(self, dicom_obj):
        rtstruct_contours = None
        if getattr(dicom_obj, "Modality", None) == "RTSTRUCT" and hasattr(dicom_obj, "ROIContourSequence"):
            rtstruct_contours = dicom_obj.ROIContourSequence
            del dicom_obj.ROIContourSequence

        with suppress_output(), tempfile.TemporaryDirectory() as tmpdir:
            temp_path = str(Path(tmpdir) / "temp.dcm")

            dicom_obj.save_as(temp_path, write_like_original=False)
            del dicom_obj

            items = get_identifiers([temp_path], expand_sequences=False)

            for key in items:
                items[key].update(
                    {
                        "CSV_lookup_func": self.csv_lookup_func,
                        "hash_func": self.hash_func,
                        "DeIdentificationMethod": self.current_date,
                        "PatientName": self.PatientName,
                    }
                )

            updated = replace_identifiers(dicom_files=[temp_path], deid=self._recipe, ids=items)

            del items

            dicom_obj = updated[0]
            del updated

        if rtstruct_contours is not None:
            dicom_obj.ROIContourSequence = rtstruct_contours

        self._replace_uids(dicom_obj)

        dicom_obj.remove_private_tags()
        dicom_obj.private_block(0x1001, "Deid", create=True).add_new(0x01, "SH", self.ProfileName)
        dicom_obj.private_block(0x1003, "Deid", create=True).add_new(0x01, "SH", self.ProjectName)
        dicom_obj.private_block(0x1005, "Deid", create=True).add_new(0x01, "SH", self.TrialName)
        dicom_obj.private_block(0x1007, "Deid", create=True).add_new(0x01, "SH", self.SiteName)
        dicom_obj.private_block(0x1009, "Deid", create=True).add_new(0x01, "SH", self.SiteID)

        gc.collect()

        return dicom_obj

    def anonymize_file(self, file_path: str):
        dicom_obj = dcmread(file_path)

        rtstruct_contours = None
        if getattr(dicom_obj, "Modality", None) == "RTSTRUCT" and hasattr(dicom_obj, "ROIContourSequence"):
            rtstruct_contours = dicom_obj.ROIContourSequence
            del dicom_obj.ROIContourSequence

        dicom_obj.save_as(file_path, write_like_original=False)
        del dicom_obj

        with suppress_output():
            items = get_identifiers([file_path], expand_sequences=False)

            for key in items:
                items[key].update(
                    {
                        "CSV_lookup_func": self.csv_lookup_func,
                        "hash_func": self.hash_func,
                        "DeIdentificationMethod": self.current_date,
                        "PatientName": self.PatientName,
                    }
                )

            updated = replace_identifiers(dicom_files=[file_path], deid=self._recipe, ids=items)

            del items

            dicom_obj = updated[0]
            del updated

        if rtstruct_contours is not None:
            dicom_obj.ROIContourSequence = rtstruct_contours

        self._replace_uids(dicom_obj)

        dicom_obj.remove_private_tags()
        dicom_obj.private_block(0x1001, "Deid", create=True).add_new(0x01, "SH", self.ProfileName)
        dicom_obj.private_block(0x1003, "Deid", create=True).add_new(0x01, "SH", self.ProjectName)
        dicom_obj.private_block(0x1005, "Deid", create=True).add_new(0x01, "SH", self.TrialName)
        dicom_obj.private_block(0x1007, "Deid", create=True).add_new(0x01, "SH", self.SiteName)
        dicom_obj.private_block(0x1009, "Deid", create=True).add_new(0x01, "SH", self.SiteID)

        gc.collect()

        return dicom_obj

    def run_file(self, file_path: str):
        try:
            ds = dcmread(file_path, stop_before_pixels=True)
            if ds.Modality == "RTSTRUCT":
                full_ds = dcmread(file_path)
                self.ROI_normalization(full_ds)
                full_ds.save_as(file_path, write_like_original=False)
                del full_ds
            del ds

            result = self.anonymize_file(file_path)
            logger.info("Anonymization process completed.")
            if result is None:
                logger.error("Anonymization failed, returning None")
                return None
        except Exception:
            logger.exception("Error processing message in run_file()")
            return None
        else:
            logger.info("File anonymised successfully")
            return result

    def run(self, dicomdata):
        try:
            if dicomdata.Modality == "RTSTRUCT":
                self.ROI_normalization(dicomdata)

            dicomdata = self.anonymize(dicomdata)
            logger.info("Anonymization process completed.")
            logger.log(5, "Anonymized DICOM data: %s", dicomdata)
            if dicomdata is None:
                logger.error("Anonymization failed, returning None")
                return None
        except Exception:
            logger.exception("Error processing message in run()")
            return None
        else:
            logger.info("File anonymised successfully")
            return dicomdata
