"""REST API data retrieval and on-demand DVH calculation."""

import logging

import pandas as pd

from dvh_calculator.Config.global_var import QUERY_PATIENT
from dvh_calculator.dvh_processor import calculate_dvh_curves, collect_patients_dicom, verify_full
from imaging_common import PostgresInterface

logger = logging.getLogger(__name__)


class DataAPI:
    """Retrieve patient data from Postgres and compute DVH on demand."""

    def __init__(self):
        self.df = None
        from dvh_calculator.config import CONFIG_PATH  # noqa: PLC0415

        self.db = PostgresInterface.connect_from_yaml(CONFIG_PATH)

    def get_data_api(self, patient_id):
        """Load DICOM metadata for a patient from the database."""
        self.df = pd.read_sql_query(QUERY_PATIENT, self.db.conn, params=[patient_id])

    def dvh_api(self, structure_name):
        """Calculate DVH for the given structure name and return the result."""
        verify = verify_full(self.df)
        if verify:
            dicom_bundles = collect_patients_dicom(self.df)

            if dicom_bundles:
                for dicom_bundle in dicom_bundles:
                    logger.info("Patients to analyze: %s", len(dicom_bundles))
                    logger.info("%s", dicom_bundles[0])
                    res = calculate_dvh_curves(dicom_bundle, str_name=structure_name)
                    logger.info("Dvh calculation complete for patient %s", dicom_bundle.patient_id)
                    return res
        return None
