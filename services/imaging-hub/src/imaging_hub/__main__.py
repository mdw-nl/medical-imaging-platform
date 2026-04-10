"""Entry point: start the DICOM SCP listener and the FastAPI REST server."""

import logging
import os
import sys
import threading

import uvicorn
from pynetdicom import StoragePresentationContexts, debug_logger, evt

from imaging_common import PostgresInterface, load_settings
from imaging_hub import DicomStoreHandler
from imaging_hub.queries import MIGRATIONS, TABLES
from imaging_hub.settings import USE_NIFTI

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


def set_up_db(settings):
    """Connect to PostgreSQL, create tables, and run schema migrations."""
    db = PostgresInterface.from_settings(settings.postgres)
    db.connect()

    for _, ddl in TABLES:
        db.execute_query(ddl)

    for migration in MIGRATIONS:
        db.execute_query(migration)

    return db


if __name__ == "__main__":
    from pathlib import Path

    config_path = Path(__file__).parents[2] / "config" / "config.yaml"
    settings = load_settings(config_path)

    database = set_up_db(settings)

    recipes_path = str(Path(__file__).parents[2] / "recipes")

    nifti_converter = None
    if USE_NIFTI:
        from imaging_hub.nifti_converter import NiftiConverter

        nifti_converter = NiftiConverter(database)
        logger.info("NIfTI conversion enabled")
    else:
        logger.info("NIfTI conversion disabled (USE_NIFTI=false)")

    if settings.anonymization is None:
        raise SystemExit("ERROR: 'anonymization' section missing from config — cannot start without it")

    ae_titles = settings.scp.ae_titles if settings.scp else ["MY_SCP"]
    logger.info("Accepted Called AE Titles: %s", ae_titles)
    dh = DicomStoreHandler(
        database,
        recipes_path,
        anonymization_settings=settings.anonymization,
        nifti_converter=nifti_converter,
        ae_titles=ae_titles,
    )
    dh.ae.dimse_timeout = 600
    dh.ae.network_timeout = 300
    dh.ae.maximum_pdu_size = 0
    dh.ae.supported_contexts = StoragePresentationContexts

    handlers = [
        (evt.EVT_C_STORE, dh.handle_store),
        (evt.EVT_CONN_OPEN, dh.handle_assoc_open),
        (evt.EVT_CONN_CLOSE, dh.handle_assoc_close),
        (evt.EVT_REQUESTED, dh.handle_assoc_requested),
    ]

    threading.Thread(
        target=lambda: uvicorn.run("imaging_hub.api:app", host="0.0.0.0", port=9000, log_level="info"),
        daemon=True,
        name="api-server",
    ).start()
    logger.info("FastAPI started on port 9000")

    dicom_port = int(os.getenv("DICOM_LISTEN_PORT", "104"))
    logger.info("Starting DICOM Listener on port %d...", dicom_port)
    dh.ae.start_server(("0.0.0.0", dicom_port), block=True, evt_handlers=handlers)
