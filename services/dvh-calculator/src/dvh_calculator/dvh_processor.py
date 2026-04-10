"""Orchestrate DVH calculation from study retrieval through result upload."""

import logging
from pathlib import Path

import pandas as pd
from dicompylercore.dicomparser import DicomParser
from rt_utils import RTStructBuilder

from dvh_calculator.config import CONFIG_PATH as _CONFIG_PATH
from dvh_calculator.config import yaml_config as _config
from dvh_calculator.Config.global_var import DELETE_END, QUERY_UID, UPLOAD_DESTINATION
from dvh_calculator.DVH.dicom_bundle import DicomBundle
from dvh_calculator.DVH.dvh import DVHCalculation
from dvh_calculator.DVH.output import return_output
from dvh_calculator.postgres_dvh import PostgresUploader
from dvh_calculator.roi_handler import check_if_roi_exist, combine_rois, roi_list, roi_operation
from dvh_calculator.XNAT_service import XNATUploader
from imaging_common import PostgresInterface

logger = logging.getLogger(__name__)


def verify_bundle(dicom_bundle):
    """Verify that the dicom bundle component paths exist."""
    logger.info("Verifying DicomBundle for patient %s", dicom_bundle.patient_id)
    logger.info("RT Plan path: %s", dicom_bundle.rt_plan_path)
    logger.info("RT Struct path: %s", dicom_bundle.rt_struct_path)
    logger.info("RT Dose path: %s", dicom_bundle.rt_dose_path)
    if not dicom_bundle.rt_plan_path or not dicom_bundle.rt_struct_path:
        logger.warning("Missing RT Plan, RT Struct path in the DicomBundle")
        return False
    if not Path(dicom_bundle.rt_plan_path).exists():
        logger.warning("RT Plan file does not exist: %s", dicom_bundle.rt_plan_path)
        return False
    if not Path(dicom_bundle.rt_struct_path).exists():
        logger.warning("RT Struct file does not exist: %s", dicom_bundle.rt_struct_path)
        return False
    return True


def process_message(study_uid):
    """Use the study_uid to retrieve data and start the dvh calculation."""
    logger.info("Delete is: %s", DELETE_END)
    db = PostgresInterface.connect_from_yaml(_CONFIG_PATH)

    if study_uid is None:
        msg = "Study uid is None"
        raise ValueError(msg)
    logger.info("The study uid is: %s", study_uid)
    result = get_all_uid(db, study_uid)

    verified = verify_full(result)
    if verified:
        logger.info("result is: %s", result)
        dicom_bundles = collect_patients_dicom(result)
        if dicom_bundles:
            for dicom_bundle in dicom_bundles:
                logger.info("Patients to analyze: %s", len(dicom_bundles))
                logger.info("%s", dicom_bundles[0])
                calculate_dvh_curves(dicom_bundle)
            logger.info("%s", DELETE_END)
            if DELETE_END:
                logger.info("Deleting patient data from the database, %s", DELETE_END)
                for dicom_bundle in dicom_bundles:
                    dicom_bundle.rm_data_patient()
        else:
            logger.info("No dicom bundles found for the study uid")
    db.disconnect()


def get_all_uid(db, uid):
    """Retrieve all UIDs from the database for a study."""
    return pd.read_sql_query(QUERY_UID, db.conn, params=[uid])


def check_if_all_in(list_v):
    """Return True if all required RT modalities are present in the list."""
    required = ["CT", "RTSTRUCT", "RTPLAN", "RTDOSE"]
    return all(e in list_v for e in required)


def verify_full(df: pd.DataFrame) -> bool:
    """Verify all required modalities are present for each patient."""
    list_patient = list(set(df["patient_id"].values.tolist()))
    n_patients = len(list_patient)
    if n_patients > 1:
        logger.info("More than one patients in the database %s", n_patients)
        result = any(
            check_if_all_in(list(set(df.loc[df["patient_id"] == patient_id]["modality"].values.tolist())))
            for patient_id in list_patient
        )
        logger.info("All dicom component received? %s for %s patients", result, n_patients)
    elif n_patients == 1:
        logger.info("Only one patient")
        patient_id = list_patient[0]
        result = check_if_all_in(list(set(df.loc[df["patient_id"] == patient_id]["modality"].values.tolist())))
    else:
        result = False
    logger.debug("All dicom component received? %s", result)
    return result


def link_rt_plan_dose(df, rt_plan_uid_list, patient_id, ct, rt_struct):
    """Link RT plan to dose files."""
    list_do = []
    for k in rt_plan_uid_list:
        rt_dose = df.loc[(df["referenced_rt_plan_uid"] == k) & (df["modality"] == "RTDOSE")][
            "file_path"
        ].values.tolist()
        rt_plan = df.loc[(df["sop_instance_uid"] == k) & (df["modality"] == "RTPLAN")]["file_path"].values.tolist()
        logger.info("RT dose and plan: %s, %s", rt_dose, rt_plan)
        logger.info("rt struct %s", rt_struct[0])
        logger.info("rt plan  %s", rt_plan[0])
        logger.info("ct  %s", ct[0])
        logger.info("rt dose  %s", rt_dose)
        dicom_bundle = DicomBundle(
            patient_id=patient_id, rt_ct=ct[0], rt_plan=rt_plan[0], rt_dose=rt_dose, rt_struct=rt_struct[0]
        )
        list_do.append(dicom_bundle)
    return list_do


def collect_patients_dicom(df: pd.DataFrame):
    """Collect DICOM bundles for each patient."""
    logger.info("Dataframe is %s", df.columns)
    list_patient = list(set(df["patient_id"].values.tolist()))
    result_list = []
    for patient_id in list_patient:
        df_o_p: pd.DataFrame = df.loc[df["patient_id"] == patient_id]
        logger.info("Collecting dicom for patient %s, modalities: %s", patient_id, df_o_p["modality"].values.tolist())
        ref_rt_plan_uid_list = df_o_p["referenced_rt_plan_uid"].values.tolist()
        rt_struct = df_o_p.loc[df_o_p["modality"] == "RTSTRUCT"]["file_path"].values.tolist()
        ct = df_o_p.loc[df_o_p["modality"] == "CT"]["file_path"].values.tolist()
        ref_rt_plan_uid_list = [uid for uid in ref_rt_plan_uid_list if uid != "UNKNOWN"]
        dicom_bundles = link_rt_plan_dose(df_o_p, ref_rt_plan_uid_list, patient_id, ct, rt_struct)
        result_list.extend(dicom_bundles)

    return result_list


def calculate_dvh_curves(dicom_bundle, str_name=None):
    """Run DVH calculation for a DICOM bundle and upload results."""
    dvh_c = DVHCalculation()
    logger.info("RTstruct %s", dicom_bundle.rt_struct)
    logger.info("RTPlan: %s", dicom_bundle.rt_plan)
    logger.info("RTdose: %s", dicom_bundle.rt_dose)
    dicom_bundle = combine(dicom_bundle)
    structures = dicom_bundle.rt_struct.GetStructures()
    output = dvh_c.calculate_dvh_all(dicom_bundle, structures, str_name)
    if UPLOAD_DESTINATION == "gdp":
        return_output(dicom_bundle.patient_id, output)
    elif UPLOAD_DESTINATION == "xnat":
        xnat = XNATUploader()
        xnat.run(output, dicom_bundle)
    elif UPLOAD_DESTINATION != "postgres":
        logger.warning("Upload destination %s not supported, skipping external upload", UPLOAD_DESTINATION)
    logger.info("Calculation complete for %s", dicom_bundle.patient_id)
    pg = PostgresUploader()
    pg.run(output, dicom_bundle)
    return output


def structure_combination(item, rt_struct):
    """Apply a single ROI combination rule to the RT structure set."""
    roi_string = next(iter(item.values()))["roi"]
    ROI_total_string = roi_string
    string_parts = roi_string.split()
    operations_list = roi_operation(string_parts)
    ROI_list = roi_list(string_parts)
    rt_struct_rois = rt_struct.get_roi_names()
    for k in ROI_list:
        if not check_if_roi_exist(k, rt_struct_rois):
            logger.info("Roi combination cancelled. %s not in the roi list", k)
            return rt_struct
    combined_mask = combine_rois(rt_struct, ROI_list, operations_list)
    rt_struct.add_roi(mask=combined_mask, name=ROI_total_string, approximate_contours=False)
    logger.info("Combination completed. %s", ROI_total_string)
    return rt_struct


def combine(dicom_bundle: DicomBundle):
    """Apply all configured ROI combinations to the DICOM bundle's RT structure."""
    rt_struct = RTStructBuilder.create_from(dicom_bundle.rt_ct_path, dicom_bundle.rt_struct_path)
    logger.info("Starting combination")
    dvh_calculations_list = _config.get("dvh-calculations")
    for item in dvh_calculations_list or []:
        rt_struct = structure_combination(item, rt_struct)
    rt_struct: DicomParser = DicomParser(rt_struct.ds)
    dicom_bundle.rt_struct = rt_struct
    return dicom_bundle
