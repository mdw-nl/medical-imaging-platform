import logging
import os
import time

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class XNATUploader:
    def __init__(
        self,
        xnat_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ):
        self.xnat_url = xnat_url or os.getenv("XNAT_URL", "http://xnat-web:8080")
        self.auth = HTTPBasicAuth(
            username or os.getenv("XNAT_USERNAME", "admin"),
            password or os.getenv("XNAT_PASSWORD", "admin"),
        )

    def check_connectivity(self) -> int:
        response = requests.get(self.xnat_url, auth=self.auth, timeout=10)
        logger.info("XNAT connectivity check: %s", response.status_code)
        return response.status_code

    def is_session_ready(self, url: str) -> bool:
        response = requests.get(url, auth=self.auth, timeout=10)
        return response.status_code == 200

    def wait_for_session(self, url: str, timeout: int = 300, poll_interval: int = 5) -> bool:
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
