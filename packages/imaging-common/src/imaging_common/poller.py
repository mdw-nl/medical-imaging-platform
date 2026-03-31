import concurrent.futures
import logging
import os
import time
from datetime import datetime

import requests
from croniter import croniter

logger = logging.getLogger(__name__)


class APIPoller:
    def __init__(
        self,
        endpoint: str,
        request_body: dict,
        callback,
        service_url: str | None = None,
        poll_cron: str | None = None,
        max_workers: int = 5,
    ):
        self.service_url = service_url or os.getenv("DICOM_SERVICE_URL", "http://dicom-service:9000")
        self.endpoint = endpoint
        self.request_body = request_body
        self.callback = callback
        self.poll_cron = poll_cron or os.getenv("POLL_CRON", "*/5 * * * *")
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._pending: set[concurrent.futures.Future] = set()

    def _is_idle(self) -> bool:
        self._pending = {f for f in self._pending if not f.done()}
        return len(self._pending) == 0

    def poll(self):
        cron = croniter(self.poll_cron, datetime.now())
        while True:
            next_run = cron.get_next(datetime)
            sleep_seconds = (next_run - datetime.now()).total_seconds()
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

            if not self._is_idle():
                logger.info("Skipping poll: computations still running")
                continue

            try:
                response = requests.post(
                    f"{self.service_url}{self.endpoint}",
                    json=self.request_body,
                    timeout=30,
                )
                response.raise_for_status()
                packages = response.json().get("packages", [])
                for package in packages:
                    future = self.executor.submit(self.callback, package)
                    self._pending.add(future)
            except Exception:
                logger.exception("Error polling DICOM service")
