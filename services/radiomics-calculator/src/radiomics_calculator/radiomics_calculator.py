"""PyRadiomics feature extraction for CT images and ROI masks."""

import csv
import io
import logging
import os
from pathlib import Path

import radiomics
import radiomics.featureextractor
import SimpleITK as sitk

logger = logging.getLogger(__name__)

SKIP_ROIS_DEFAULT = "Body,Shoulders,Posterior_Neck,RingPTVLow,RingPTVHigh"
SKIP_ROIS = {r.strip() for r in os.getenv("SKIP_ROIS", SKIP_ROIS_DEFAULT).split(",") if r.strip()}


class RadiomicsCalculator:
    """Compute PyRadiomics features for one or more ROI masks against a CT image."""

    def __init__(self, settings="/radiomics_settings/Params.yaml"):
        self.settings = settings
        self.result_dict = {}
        self._extractor = radiomics.featureextractor.RadiomicsFeatureExtractor(self.settings)

    def calculate_single_roi(self, image, mask_path):
        """Extract radiomics features for a single ROI mask against *image*."""
        logger.info("Calculating features for ROI: %s", Path(mask_path).name)
        mask = sitk.ReadImage(str(mask_path))
        return self._extractor.execute(image, mask)

    def get_csv_and_metadata(self, package):
        """Serialize accumulated results to CSV and return (csv_content, metadata, filename)."""
        patient_id = package["patient_id"]
        study_uid = package["study_uid"]

        output = io.StringIO()
        fieldnames = list(next(iter(self.result_dict.values())).keys())
        writer = csv.DictWriter(output, fieldnames=["id", *fieldnames])
        writer.writeheader()
        for key, od in self.result_dict.items():
            row = {"id": key}
            row.update(od)
            writer.writerow(row)
        csv_content = output.getvalue()

        study_uid_label = study_uid.replace(".", "_")
        metadata = {
            "project": "UNKNOWN",
            "subject": patient_id,
            "experiment": study_uid_label,
            "sop_instance_uid": package["rtstruct_sop_uid"],
        }
        logger.info("PatientID: %s, Study: %s", patient_id, study_uid)

        filename = f"radiomics_results_{patient_id}.csv"
        return csv_content, metadata, filename

    def run(self, image_path, mask_paths, package):
        """Run feature extraction on all non-skipped ROIs and return CSV output with metadata."""
        self.result_dict = {}
        image = sitk.ReadImage(str(image_path))
        logger.info("CT image loaded once for %d ROIs", len(mask_paths))
        for mask in mask_paths:
            roi_name = mask["roi_name"]
            if roi_name in SKIP_ROIS:
                logger.info("Skipping non-clinical ROI: %s", roi_name)
                continue
            self.result_dict[roi_name] = self.calculate_single_roi(image, mask["file_path"])
        return self.get_csv_and_metadata(package)
