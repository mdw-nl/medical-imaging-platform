"""PACS archiver service that polls imaging-hub for DICOM files and sends them to a PACS SCP."""

import logging
import os
import sys
from pathlib import Path

import requests

from imaging_common import APIPoller, PostgresInterface
from pacs_archiver.sender import DICOMtoPACS
from pacs_archiver.verifier import XnatVerifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)

DICOM_SERVICE_URL = os.getenv("DICOM_SERVICE_URL", "http://imaging-hub:9000")
_FALLBACK_AE_TITLE = os.getenv("PACS_SCP_AE_TITLE")
_API_KEY = os.getenv("IMAGING_API_KEY")


def _api_headers() -> dict:
    """Return HTTP headers with the API key if configured."""
    headers = {}
    if _API_KEY:
        headers["X-API-Key"] = _API_KEY
    return headers


def process_archive_package(package: dict):
    """Send a batch of DICOM SOPs to PACS and report results back to imaging-hub."""
    sops = package.get("sops", [])
    file_paths = [p["file_path"] for p in sops if p.get("file_path")]
    sop_uids = [p["sop_instance_uid"] for p in sops]

    if not file_paths:
        logger.warning("No file paths in archive package, skipping")
        return

    project = sops[0].get("project") if sops else None
    ae_title = project or _FALLBACK_AE_TITLE
    if not ae_title:
        logger.error("No project in archive package and PACS_SCP_AE_TITLE not set, skipping")
        return
    sender = DICOMtoPACS(ae_title=ae_title)
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
            headers=_api_headers(),
            timeout=30,
        )
    except Exception:
        logger.exception("Failed to report archive results back to imaging-hub")


def process_poll_response(package: dict):
    """Send a single DICOM SOP to PACS and report the result back to imaging-hub."""
    sop = package
    file_path = sop.get("file_path")
    sop_uid = sop.get("sop_instance_uid")

    if not file_path:
        logger.warning("No file path for SOP %s, skipping", sop_uid)
        return

    project = sop.get("project")
    ae_title = project or _FALLBACK_AE_TITLE
    if not ae_title:
        logger.error("No project for SOP %s and PACS_SCP_AE_TITLE not set, skipping", sop_uid)
        return
    sender = DICOMtoPACS(ae_title=ae_title)
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
            headers=_api_headers(),
            timeout=30,
        )
    except Exception:
        logger.exception("Failed to report archive result for SOP %s", sop_uid)


if __name__ == "__main__":
    config_path = Path(os.getenv("CONFIG_PATH", str(Path(__file__).parents[2] / "config" / "config.yaml")))
    db = PostgresInterface.connect_from_yaml(config_path)

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
