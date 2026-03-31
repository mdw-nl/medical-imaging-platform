import logging
import os
import sys
import threading

import uvicorn
from pynetdicom import StoragePresentationContexts, debug_logger, evt

from dicom_ingest import DicomStoreHandler
from dicom_ingest.queries import MIGRATIONS, TABLES
from dicom_ingest.settings import USE_NIFTI
from imaging_common import PostgresInterface, load_yaml_config

_log_level = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, _log_level, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger()

if _log_level == "DEBUG":
    debug_logger()
logging.getLogger("pynetdicom").setLevel(logging.WARNING)


def set_up_db(config_dict_db):
    host, port, user, pwd, db_name = (
        config_dict_db["host"],
        config_dict_db["port"],
        config_dict_db["username"],
        config_dict_db["password"],
        config_dict_db["db"],
    )
    db = PostgresInterface(host=host, database=db_name, user=user, password=pwd, port=port)
    db.connect()

    for _, ddl in TABLES:
        db.execute_query(ddl)

    for migration in MIGRATIONS:
        db.execute_query(migration)

    return db


if __name__ == "__main__":
    from pathlib import Path

    config_path = Path(__file__).parents[2] / "config" / "config.yaml"
    config = load_yaml_config(config_path)
    config_db = config["postgres"]

    database = set_up_db(config_db)

    recipes_path = str(Path(__file__).parents[2] / "recipes")

    nifti_converter = None
    if USE_NIFTI:
        from dicom_ingest.nifti_converter import NiftiConverter

        nifti_converter = NiftiConverter(database)
        logger.info("NIfTI conversion enabled")
    else:
        logger.info("NIfTI conversion disabled (USE_NIFTI=false)")

    dh = DicomStoreHandler(database, recipes_path, nifti_converter=nifti_converter)
    dh.ae.dimse_timeout = 600
    dh.ae.network_timeout = 300
    dh.ae.maximum_pdu_size = 0
    dh.ae.supported_contexts = StoragePresentationContexts

    handlers = [
        (evt.EVT_C_STORE, dh.handle_store),
        (evt.EVT_CONN_OPEN, dh.handle_assoc_open),
        (evt.EVT_CONN_CLOSE, dh.handle_assoc_close),
    ]

    threading.Thread(
        target=lambda: uvicorn.run("dicom_ingest.api:app", host="0.0.0.0", port=9000, log_level="info"),
        daemon=True,
        name="api-server",
    ).start()
    logger.info("FastAPI started on port 9000")

    logger.info("Starting DICOM Listener on port 104...")
    dh.ae.start_server(("0.0.0.0", 104), block=True, evt_handlers=handlers)
