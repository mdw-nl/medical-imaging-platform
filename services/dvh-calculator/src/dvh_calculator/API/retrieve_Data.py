import logging
import traceback

import pandas as pd

from dvh_calculator.Config.global_var import QUERY_PATIENT
from dvh_calculator.dvh_processor import calculate_dvh_curves, collect_patients_dicom, connect_db, verify_full


class DataAPI:
    def __init__(self):
        self.df = None
        self.db = connect_db()

    def get_data_api(self, patient_id):

        try:
            self.df = pd.read_sql_query(QUERY_PATIENT, self.db.conn, params=[patient_id])
        except Exception as e:
            raise e

    def dvh_api(self, structure_name):

        verify = verify_full(self.df)
        if verify:
            dicom_bundles = collect_patients_dicom(self.df)

            if dicom_bundles:
                for dicom_bundle in dicom_bundles:
                    logging.info(f"Patients to analyze:{len(dicom_bundles)} ")
                    logging.info(f"{dicom_bundles[0]}")
                    try:
                        res = calculate_dvh_curves(dicom_bundle, str_name=structure_name, gdp=False)
                        logging.info(f"Dvh calculation complete for patient {dicom_bundle.patient_id} {res}")
                        return res
                    except Exception as e:
                        logging.warning(f"Error during calculation, Exception Message: {e}")
                        logging.warning(f"Exception Type: {type(e).__name__}")
                        logging.warning(traceback.format_exc())
                        raise e
        else:
            return None
