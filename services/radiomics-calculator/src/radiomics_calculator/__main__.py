"""Radiomics calculator service that polls imaging-hub for NIfTI packages and computes PyRadiomics features."""

import logging
import os
from pathlib import Path

from imaging_common import APIPoller, PostgresInterface, XNATUploader
from radiomics_calculator.radiomics_calculator import RadiomicsCalculator
from radiomics_calculator.radiomics_results_postgress import send_postgress, setup_radiomics_db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

SEND_XNAT = os.getenv("SEND_XNAT", "false").strip().lower() in ("1", "true", "yes")
SEND_POSTGRES = os.getenv("SEND_POSTGRES", "true").strip().lower() in ("1", "true", "yes")


class RadiomicsPipeline:
    """Orchestrate feature extraction, database storage, and optional XNAT upload."""

    def __init__(self):
        config_path = Path(os.getenv("CONFIG_PATH", str(Path(__file__).parents[2] / "config" / "config.yaml")))
        self.db = PostgresInterface.connect_from_yaml(config_path)
        self.calculator = RadiomicsCalculator()
        self.xnat_sender = XNATUploader() if SEND_XNAT else None
        postgres_db = setup_radiomics_db()
        postgres_db.run(self.db)

    def process_message(self, package: dict):
        """Extract radiomics features for all ROI masks in *package* and store the results."""
        rtstruct_sop_uid = package["rtstruct_sop_uid"]
        logger.info("Received NIfTI package for RTSTRUCT %s", rtstruct_sop_uid)

        image_path = package["image_path"]
        masks = package["masks"]

        if not masks:
            logger.warning("No masks in package for RTSTRUCT %s, skipping", rtstruct_sop_uid)
            return

        try:
            csv_content, metadata, filename = self.calculator.run(image_path, masks, package)

            if SEND_POSTGRES:
                send_postgress(self.db, csv_content, metadata)

            if self.xnat_sender is not None:
                self.xnat_sender.upload_file(
                    project=metadata["project"],
                    subject=metadata["subject"],
                    experiment=metadata["experiment"],
                    resource_type="csv",
                    filename=filename,
                    content=csv_content,
                )
            logger.info("Radiomics pipeline completed successfully.")

        except Exception:
            logger.exception("An error occurred in the pipeline.")


if __name__ == "__main__":
    pipeline = RadiomicsPipeline()
    poller = APIPoller(
        endpoint="/nifti_package",
        request_body={"modality": "RTSTRUCT"},
        callback=pipeline.process_message,
    )
    poller.poll()
