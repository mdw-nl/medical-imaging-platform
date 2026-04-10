"""Client for uploading files to an XNAT imaging archive."""

import logging
import os
import time

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class XNATUploader:
    """Upload files to XNAT experiment resources over the REST API."""

    def __init__(
        self,
        xnat_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ):
        self.xnat_url = xnat_url or os.getenv("XNAT_URL", "http://xnat-web:8080")
        resolved_user = username or os.getenv("XNAT_USERNAME")
        resolved_pass = password or os.getenv("XNAT_PASSWORD")
        if not resolved_user or not resolved_pass:
            raise ValueError("XNAT credentials required: pass username/password or set XNAT_USERNAME/XNAT_PASSWORD")
        self.auth = HTTPBasicAuth(resolved_user, resolved_pass)

    def check_connectivity(self) -> int:
        """Return the HTTP status code from a GET to the XNAT root URL."""
        response = requests.get(self.xnat_url, auth=self.auth, timeout=10)
        logger.info("XNAT connectivity check: %s", response.status_code)
        return response.status_code

    def is_session_ready(self, url: str) -> bool:
        """Return ``True`` if the XNAT session at *url* returns HTTP 200."""
        response = requests.get(url, auth=self.auth, timeout=10)
        return response.status_code == 200

    def wait_for_session(self, url: str, timeout: int = 300, poll_interval: int = 5) -> bool:
        """Block until the session at *url* is ready, or return ``False`` on timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.is_session_ready(url):
                return True
            logger.info("DICOM session not archived yet; waiting...")
            time.sleep(poll_interval)
        logger.error("Timed out waiting for session at %s", url)
        return False

    def upload_file(
        self,
        project: str,
        subject: str,
        experiment: str,
        resource_type: str,
        filename: str,
        content: str | bytes,
        content_type: str = "text/csv",
    ) -> bool:
        """Upload a file to an experiment resource, waiting for the session first."""
        experiment_url = f"{self.xnat_url}/data/projects/{project}/subjects/{subject}/experiments/{experiment}"

        if not self.wait_for_session(experiment_url):
            return False

        upload_url = f"{experiment_url}/resources/{resource_type}/files/{filename}"
        data = content.encode("utf-8") if isinstance(content, str) else content

        response = requests.put(
            upload_url,
            data=data,
            auth=self.auth,
            headers={"Content-Type": content_type},
            timeout=60,
        )

        if response.status_code in {200, 201}:
            logger.info("Uploaded %s successfully to XNAT.", filename)
            return True

        logger.error("Failed to upload %s. Status %s: %s", filename, response.status_code, response.text)
        return False
