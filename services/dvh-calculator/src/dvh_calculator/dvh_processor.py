import logging
import os
import re
import traceback
from pathlib import Path

import pandas as pd
from dicompylercore.dicomparser import DicomParser
from rt_utils import RTStructBuilder

from dvh_calculator.Config.global_var import DELETE_END, QUERY_UID, UPLOAD_DESTINATION
from dvh_calculator.DVH.dicom_bundle import DicomBundle
from dvh_calculator.DVH.dvh import DVH_calculation
from dvh_calculator.DVH.output import return_output
from dvh_calculator.PostrgresDVHdb import upload_pg
from dvh_calculator.roi_handler import check_if_roi_exist, combine_rois, roi_list, roi_operation
from dvh_calculator.XNAT_service import upload_XNAT
from imaging_common import PostgresInterface, load_yaml_config

_CONFIG_PATH = Path(os.getenv("CONFIG_PATH", str(Path(__file__).parents[2] / "config" / "config.yaml")))
_config = load_yaml_config(_CONFIG_PATH)

logger = logging.getLogger(__name__)


def verify_bundle(dicom_bundle):
    """Verify that the dicom bundle component path exist using os
    :param dicom_bundle:
    :return:
    """
    logger.info(f"Verifying DicomBundle for patient {dicom_bundle.patient_id}")
    logger.info(f"RT Plan path: {dicom_bundle.rt_plan_path}")
    logger.info(f"RT Struct path: {dicom_bundle.rt_struct_path}")
    logger.info(f"RT Dose path: {dicom_bundle.rt_dose_path}")
    if not dicom_bundle.rt_plan_path or not dicom_bundle.rt_struct_path:
        logger.warning("Missing RT Plan, RT Struct  path in the DicomBundle")
        return False
    if not os.path.exists(dicom_bundle.rt_plan_path):
        logger.warning(f"RT Plan file does not exist: {dicom_bundle.rt_plan_path}")
        return False
    if not os.path.exists(dicom_bundle.rt_struct_path):
        logger.warning(f"RT Struct file does not exist: {dicom_bundle.rt_struct_path}")
        return False
    return True


def process_message(study_uid):
    """The function use the study_uid to retrieve the data from the database.
    Verify that for each patient we have all dicom required nad start the dvh calculation
    """
    try:
        logger.info(f"Delete is : {DELETE_END}")
        db = connect_db()

        if study_uid is None:
            raise Exception(f"Study uid is : {study_uid}")
        logger.info(f"The study uid is :{study_uid}")
        result = get_all_uid(db, study_uid)

        verified = verify_full(result)
        if verified:
            logger.info(f"result is :{result}")
            dicom_bundles = collect_patients_dicom(result)
            if dicom_bundles:
                for dicom_bundle in dicom_bundles:
                    logger.info(f"Patients to analyze:{len(dicom_bundles)} ")
                    logger.info(f"{dicom_bundles[0]}")
                    try:
                        calculate_dvh_curves(dicom_bundle)
                    except Exception as e:
                        logger.warning(f"Error during calculation, Exception Message: {e}")
                        logger.warning(f"Exception Type: {type(e).__name__}")
                        logger.warning(traceback.format_exc())
                        raise e
                logger.info(DELETE_END)
                if DELETE_END:
                    logger.info(f"Deleting patient data from the database, {DELETE_END}")
                    try:
                        for dicom_bundle in dicom_bundles:
                            dicom_bundle.rm_data_patient()
                    except Exception as e:
                        logger.warning(f"Error during delete of patient data, Exception Message: {e}")
                        logger.warning(f"Exception Type: {type(e).__name__}")
                        logger.warning(traceback.format_exc())
                        raise e
            else:
                logger.info("No dicom bundles found for the study uid")
        db.disconnect()
    except Exception as e:
        logger.warning(f"Exception Type: {type(e).__name__}")
        logger.warning(f"Exception Message: {e}")
        logger.warning(traceback.format_exc())
        raise e


def connect_db():
    config_dict_db = _config["postgres"]
    host, port, user, pwd, db_name = (
        config_dict_db["host"],
        config_dict_db["port"],
        config_dict_db["username"],
        config_dict_db["password"],
        config_dict_db["db"],
    )
    db = PostgresInterface(host=host, database=db_name, user=user, password=pwd, port=port)
    db.connect()
    logger.info("Connected to the database")

    return db


def get_all_uid(db, uid):
    """:param db:
    :param uid:
    :return:
    """
    query = f"Select * from public.dicom_insert where study_instance_uid ='{uid}';"
    try:
        df = pd.read_sql_query(QUERY_UID, db.conn, params=[uid])
    except Exception as e:
        raise e
    return df


def check_if_all_in(list_v):
    list_m = ["CT", "RTSTRUCT", "RTPLAN", "RTDOSE"]
    value_ = False
    logger.info(f"Checking if all modalities are present in the list: {list_v}")
    for e in list_m:
        if e not in list_v:
            return False
        value_ = True
    return value_


def verify_full(df: pd.DataFrame) -> bool:
    """:param df:
    :return:
    """
    result = True
    list_patient = list(set(df["patient_id"].values.tolist()))
    n_patients = len(list_patient)
    if len(list_patient) > 1:
        logger.info(f"More than one patients in the database {n_patients}")
        result = any(
            check_if_all_in(list(set(df.loc[df["patient_id"] == patient_id]["modality"].values.tolist())))
            for patient_id in list_patient
        )
        logger.info(f"All dicom component received ? {result} for {n_patients} patients")
    elif len(list_patient) == 1:
        logger.info("Only one patient")
        patient_id = list_patient[0]
        result = check_if_all_in(list(set(df.loc[df["patient_id"] == patient_id]["modality"].values.tolist())))
    logger.debug(f"All dicom component received ? {result}")

    return result


def link_rt_plan_dose(df, rt_plan_uid_list, patient_id, ct, rt_struct):
    """:param df:
    :param rt_plan_uid_list:
    :param patient_id:
    :param ct:
    :param rt_struct:
    :return:
    """
    list_do = []
    for k in rt_plan_uid_list:
        rt_dose = df.loc[(df["referenced_rt_plan_uid"] == k) & (df["modality"] == "RTDOSE")][
            "file_path"
        ].values.tolist()
        rt_plan = df.loc[(df["sop_instance_uid"] == k) & (df["modality"] == "RTPLAN")]["file_path"].values.tolist()
        logger.info(f"RT dose and plan :{rt_dose}, {rt_plan}")
        logger.info(f"rt struct {rt_struct[0]}")
        logger.info(f"rt plan  {rt_plan[0]}")
        logger.info(f"ct  {ct[0]}")
        logger.info(f"rt doe   {rt_dose}")
        dicom_bundle = DicomBundle(
            patient_id=patient_id, rt_ct=ct[0], rt_plan=rt_plan[0], rt_dose=rt_dose, rt_struct=rt_struct[0]
        )
        list_do.append(dicom_bundle)
    return list_do


def collect_patients_dicom(df: pd.DataFrame):
    """:param df:
    :return:
    """
    logger.info(f"Dataframe is {df.columns}")
    list_patient = list(set(df["patient_id"].values.tolist()))
    result_list = []
    for patient_id in list_patient:
        df_o_p: pd.DataFrame = df.loc[df["patient_id"] == patient_id]
        logger.info(f"Collecting dicom for patient {patient_id}, modalities: {df_o_p['modality'].values.tolist()}")
        ref_rt_plan_uid_list = df_o_p["referenced_rt_plan_uid"].values.tolist()
        rt_struct = df_o_p.loc[df_o_p["modality"] == "RTSTRUCT"]["file_path"].values.tolist()
        ct = df_o_p.loc[df_o_p["modality"] == "CT"]["file_path"].values.tolist()
        ref_rt_plan_uid_list = [uid for uid in ref_rt_plan_uid_list if uid != "UNKNOWN"]
        dicom_bundles = link_rt_plan_dose(df_o_p, ref_rt_plan_uid_list, patient_id, ct, rt_struct)
        result_list.extend(dicom_bundles)

    return result_list


# def add_combined_structures(rt_struct):
#    """
#
#    :param rt_struct:
#    :return:
#    """
#    roi_lookup_sevice = roi_lookup_sevice()
#    standarized_name_dict = roi_lookup_sevice.get_standarized_names(rt_struct)
#    dvh_c = DVH_calculation()
#    dvh_calculations_list = _config.get("dvh-calculations")
#    for item in dvh_calculations_list:
#        for key, value in item.items():
#            roi_string = value["roi"]
#
#        string_parts = re.split(r'\s+', roi_string)
#
#        ROI_total_string = ""
#        operations_list = []
#        ROI_list = []
#        for i, parts in enumerate(string_parts, start=1):
#            ROI_total_string = ROI_total_string + parts
#            if i % 2 == 0:
#                operations_list.append(parts)
#            else:
#                ROI_list.append(standarized_name_dict[parts])
#
#        combined_mask = roi_handler.combine_rois(rt_struct, ROI_list, operations_list)
#        rt_struct.add_roi(mask=combined_mask, name=ROI_total_string, approximate_contours=False)
#        return rt_struct


def calculate_dvh_curves(dicom_bundle, str_name=None):
    dvh_c = DVH_calculation()
    logger.info(f"RTstruct {dicom_bundle.rt_struct}")
    logger.info(f"RTPlan :{dicom_bundle.rt_plan}")
    logger.info(f"RTdose :{dicom_bundle.rt_dose}")
    dicom_bundle = combine(dicom_bundle)
    structures = dicom_bundle.rt_struct.GetStructures()
    output = dvh_c.calculate_dvh_all(dicom_bundle, structures, str_name)
    if UPLOAD_DESTINATION == "gdp":
        return_output(dicom_bundle.patient_id, output)
    elif UPLOAD_DESTINATION == "xnat":
        xnat = upload_XNAT()
        xnat.run(output, dicom_bundle)
    elif UPLOAD_DESTINATION != "postgres":
        logger.warning(f"Upload destination {UPLOAD_DESTINATION} not supported, skipping external upload")
    logger.info(f"Calculation complete for {dicom_bundle.patient_id}")
    pg = upload_pg()
    pg.run(output, dicom_bundle)


def structure_combination(item, rt_struct):
    roi_string = next(iter(item.values()))["roi"]
    ROI_total_string = roi_string
    string_parts = re.split(r"\s+", roi_string)
    operations_list = roi_operation(string_parts)
    ROI_list = roi_list(string_parts)
    rt_struct_rois = rt_struct.get_roi_names()
    for k in ROI_list:
        if not check_if_roi_exist(k, rt_struct_rois):
            logger.info(f"Roi combination cancelled. {k} not in the roi list")
            return rt_struct
    combined_mask = combine_rois(rt_struct, ROI_list, operations_list)
    rt_struct.add_roi(mask=combined_mask, name=ROI_total_string, approximate_contours=False)
    logger.info(f"Combination completed. {ROI_total_string}")
    return rt_struct


def combine(dicom_bundle: DicomBundle):
    rt_struct = RTStructBuilder.create_from(dicom_bundle.rt_ct_path, dicom_bundle.rt_struct_path)
    logger.info("Starting combination")
    dvh_calculations_list = _config.get("dvh-calculations")
    for item in dvh_calculations_list or []:
        rt_struct = structure_combination(item, rt_struct)
    rt_struct: DicomParser = DicomParser(rt_struct.ds)
    dicom_bundle.rt_struct = rt_struct
    return dicom_bundle
