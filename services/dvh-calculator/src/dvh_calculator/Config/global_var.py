"""Global constants, SQL queries, and environment-driven settings."""

import os

NUMBER_ATTEMPTS = 5
RETRY_DELAY_IN_SECONDS = 10
QUERY_UID = "SELECT * FROM public.dicom_insert WHERE study_instance_uid = %s;"
UPLOAD_DESTINATION = os.getenv("UPLOAD_DESTINATION", "gdp")
INSERT_QUERY_DICOM_META = """
    INSERT INTO calculation_status (
        study_uid,status,timestamp
    ) VALUES (%s, %s, %s)
"""

QUERY_PATIENT = """SELECT * FROM public.dicom_insert WHERE patient_id = %s;"""
