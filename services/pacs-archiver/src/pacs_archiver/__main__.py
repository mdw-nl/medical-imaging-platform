import logging
import os
import sys
from pathlib import Path

import requests

from imaging_common import APIPoller, PostgresInterface, load_yaml_config
from pacs_archiver.sender import DICOMtoPACS
from pacs_archiver.verifier import XnatVerifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)

DICOM_SERVICE_URL = os.getenv("DICOM_SERVICE_URL", "http://dicom-service:9000")


def connect_db():
    config_path = Path(os.getenv("CONFIG_PATH", str(Path(__file__).parents[2] / "config" / "config.yaml")))
    config = load_yaml_config(config_path)
    cfg = config["postgres"]
    db = PostgresInterface(
        host=cfg["host"],
        database=cfg["db"],
        user=cfg["username"],
        password=cfg["password"],
        port=cfg["port"],
    )
    db.connect()
    return db


def process_archive_package(package: dict):
    file_paths = [p["file_path"] for p in package.get("sops", []) if p.get("file_path")]
    sop_uids = [p["sop_instance_uid"] for p in package.get("sops", [])]

    if not file_paths:
        logger.warning("No file paths in archive package, skipping")
        return

    sender = DICOMtoPACS()
    results = []

    try:

        def on_sent(sop_uid):
            results.append({"sop_instance_uid": sop_uid, "success": True})

        sender.send_files(file_paths, on_sent=on_sent)
        failed_uids = set(sop_uids) - {r["sop_instance_uid"] for r in results}
        results.extend({"sop_instance_uid": uid, "success": False} for uid in failed_uids)
    except Exception:
        logger.exception("PACS send failed")
        sent_uids = {r["sop_instance_uid"] for r in results}
        results.extend({"sop_instance_uid": uid, "success": False} for uid in sop_uids if uid not in sent_uids)

    try:
        requests.post(
            f"{DICOM_SERVICE_URL}/archive_callback",
            json={"results": results},
            timeout=30,
        )
    except Exception:
        logger.exception("Failed to report archive results back to dicom-ingest")


def process_poll_response(package: dict):
    sop = package
    file_path = sop.get("file_path")
    sop_uid = sop.get("sop_instance_uid")

    if not file_path:
        logger.warning("No file path for SOP %s, skipping", sop_uid)
        return

    sender = DICOMtoPACS()
    success = False
    try:
        sender.send_files([file_path])
        success = True
        logger.info("Archived SOP %s", sop_uid)
    except Exception:
        logger.exception("PACS send failed for SOP %s", sop_uid)

    try:
        requests.post(
            f"{DICOM_SERVICE_URL}/archive_callback",
            json={"results": [{"sop_instance_uid": sop_uid, "success": success}]},
            timeout=30,
        )
    except Exception:
        logger.exception("Failed to report archive result for SOP %s", sop_uid)


if __name__ == "__main__":
    db = connect_db()

    verifier = XnatVerifier(db)
    verifier.start()
    logger.info("XNAT verifier started as background thread")

    poller = APIPoller(
        endpoint="/archive_package",
        request_body={},
        callback=process_poll_response,
    )
    logger.info("Starting PACS archiver polling loop...")
    poller.poll()
