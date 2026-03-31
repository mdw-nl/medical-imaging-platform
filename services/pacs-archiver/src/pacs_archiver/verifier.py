import logging
import os
import threading

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

XNAT_API_URL = os.getenv("XNAT_API_URL", "http://xnat-web:8080")
XNAT_API_USER = os.getenv("XNAT_API_USER", "admin")
XNAT_API_PASSWORD = os.getenv("XNAT_API_PASSWORD", "admin")
XNAT_PROJECT = os.getenv("XNAT_PROJECT", "PREACT")
XNAT_POLL_INTERVAL = int(os.getenv("XNAT_POLL_INTERVAL", "60"))

UNVERIFIED_STUDIES = """
SELECT DISTINCT study_instance_uid
FROM pacs_archive
WHERE status = 'archived' AND xnat_status != 'verified'
"""

UPDATE_XNAT_STATUS = """
UPDATE pacs_archive SET xnat_status = %s WHERE study_instance_uid = %s AND xnat_status != %s;
"""


class XnatVerifier:
    def __init__(self, db):
        self._db = db
        self._stop = threading.Event()
        self._auth = HTTPBasicAuth(XNAT_API_USER, XNAT_API_PASSWORD)
        self._base = XNAT_API_URL.rstrip("/")
        self._project = XNAT_PROJECT

    def start(self):
        t = threading.Thread(target=self._run_loop, daemon=True, name="xnat-verifier")
        t.start()
        logger.info("XNAT verifier started (interval=%ds, project=%s)", XNAT_POLL_INTERVAL, self._project)

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

        prearchive_conflicts = self._get_prearchive_conflicts()

        for (study_uid,) in rows:
            label = study_uid.replace(".", "_")
            if label in prearchive_conflicts:
                self._db.execute_query(UPDATE_XNAT_STATUS, ("conflict", study_uid, "conflict"))
                logger.warning("XNAT prearchive conflict for study %s", study_uid)
                continue

            try:
                resp = requests.get(
                    f"{self._base}/data/projects/{self._project}/experiments",
                    params={"label": label, "format": "json"},
                    auth=self._auth,
                    timeout=15,
                )
                if resp.status_code != 200:
                    continue
                results = resp.json().get("ResultSet", {}).get("Result", [])
                if results:
                    self._db.execute_query(UPDATE_XNAT_STATUS, ("verified", study_uid, "verified"))
                    logger.info("XNAT verified study %s", study_uid)
            except requests.RequestException:
                logger.debug("XNAT API unreachable for study %s", study_uid)

    def _get_prearchive_conflicts(self):
        try:
            resp = requests.get(
                f"{self._base}/data/prearchive/projects/{self._project}",
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
