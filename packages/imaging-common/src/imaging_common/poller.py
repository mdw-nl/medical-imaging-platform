"""Cron-driven HTTP poller that dispatches work packages to a callback via a thread pool."""

import concurrent.futures
import logging
import os
import threading
from datetime import datetime

import requests
from croniter import croniter

logger = logging.getLogger(__name__)


class APIPoller:
    """Poll an HTTP endpoint on a cron schedule and fan out results to a callback."""

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
        self._api_key = os.getenv("IMAGING_API_KEY")
        self._shutdown_event = threading.Event()

    def shutdown(self):
        """Signal the poll loop to stop after the current iteration."""
        self._shutdown_event.set()

    def _is_idle(self) -> bool:
        self._pending = {f for f in self._pending if not f.done()}
        return len(self._pending) == 0

    @staticmethod
    def _log_callback_exception(future: concurrent.futures.Future):
        exc = future.exception()
        if exc is not None:
            logger.error("Callback failed", exc_info=exc)

    def poll(self):
        """Run the poll loop, blocking until ``shutdown`` is called."""
        cron = croniter(self.poll_cron, datetime.now())
        while not self._shutdown_event.is_set():
            next_run = cron.get_next(datetime)
            sleep_seconds = (next_run - datetime.now()).total_seconds()
            if sleep_seconds > 0:
                if self._shutdown_event.wait(timeout=sleep_seconds):
                    break

            if not self._is_idle():
                logger.info("Skipping poll: computations still running")
                continue

            try:
                headers = {}
                if self._api_key:
                    headers["X-API-Key"] = self._api_key
                response = requests.post(
                    f"{self.service_url}{self.endpoint}",
                    json=self.request_body,
                    headers=headers,
                    timeout=30,
                )
                response.raise_for_status()
                packages = response.json().get("packages", [])
                for package in packages:
                    future = self.executor.submit(self.callback, package)
                    future.add_done_callback(self._log_callback_exception)
                    self._pending.add(future)
            except Exception:
                logger.exception("Error polling DICOM service")

        self.executor.shutdown(wait=True)
