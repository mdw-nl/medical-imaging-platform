"""Background thread that verifies archived studies exist in XNAT."""

import logging
import os
import threading

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

XNAT_API_URL = os.getenv("XNAT_API_URL", "http://xnat-web:8080")
XNAT_API_USER = os.getenv("XNAT_API_USER")
XNAT_API_PASSWORD = os.getenv("XNAT_API_PASSWORD")
XNAT_POLL_INTERVAL = int(os.getenv("XNAT_POLL_INTERVAL", "60"))

if not XNAT_API_USER or not XNAT_API_PASSWORD:
    raise ValueError("XNAT_API_USER and XNAT_API_PASSWORD environment variables are required")

UNVERIFIED_STUDIES = """
SELECT DISTINCT study_instance_uid, project
FROM pacs_archive
WHERE status = 'archived' AND xnat_status != 'verified' AND project IS NOT NULL
"""

UPDATE_XNAT_STATUS = """
UPDATE pacs_archive SET xnat_status = %s WHERE study_instance_uid = %s AND xnat_status != %s;
"""


class XnatVerifier:
    """Poll XNAT to confirm that archived studies have been ingested and flag conflicts."""

    def __init__(self, db):
        self._db = db
        self._stop = threading.Event()
        self._auth = HTTPBasicAuth(XNAT_API_USER, XNAT_API_PASSWORD)
        self._base = XNAT_API_URL.rstrip("/")

    def start(self):
        """Launch the verification loop in a daemon thread."""
        t = threading.Thread(target=self._run_loop, daemon=True, name="xnat-verifier")
        t.start()
        logger.info("XNAT verifier started (interval=%ds)", XNAT_POLL_INTERVAL)

    def _run_loop(self):
        while not self._stop.is_set():
            self._stop.wait(XNAT_POLL_INTERVAL)
            if self._stop.is_set():
                break
            try:
                self._poll()
            except Exception:
                logger.exception("XNAT verifier cycle failed")

    def _poll(self):
        rows = self._db.fetch_all(UNVERIFIED_STUDIES, ())
        if not rows:
            return

        conflict_cache: dict[str, set[str]] = {}

        for study_uid, project in rows:
            if project not in conflict_cache:
                conflict_cache[project] = self._get_prearchive_conflicts(project)

            label = study_uid.replace(".", "_")
            if label in conflict_cache[project]:
                self._db.execute_query(UPDATE_XNAT_STATUS, ("conflict", study_uid, "conflict"))
                logger.warning("XNAT prearchive conflict for study %s in project %s", study_uid, project)
                continue

            try:
                resp = requests.get(
                    f"{self._base}/data/projects/{project}/experiments",
                    params={"label": label, "format": "json"},
                    auth=self._auth,
                    timeout=15,
                )
                if resp.status_code != 200:
                    continue
                results = resp.json().get("ResultSet", {}).get("Result", [])
                if results:
                    self._db.execute_query(UPDATE_XNAT_STATUS, ("verified", study_uid, "verified"))
                    logger.info("XNAT verified study %s in project %s", study_uid, project)
            except requests.RequestException:
                logger.debug("XNAT API unreachable for study %s", study_uid)

    def _get_prearchive_conflicts(self, project: str) -> set[str]:
        try:
            resp = requests.get(
                f"{self._base}/data/prearchive/projects/{project}",
                params={"format": "json"},
                auth=self._auth,
                timeout=15,
            )
            if resp.status_code != 200:
                return set()
            results = resp.json().get("ResultSet", {}).get("Result", [])
            return {r.get("name", "") for r in results if r.get("status") == "CONFLICT"}
        except requests.RequestException:
            return set()
