"""DVH calculator service entry point with FastAPI server and background poller."""

import logging
import os
import sys
import threading

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from dvh_calculator.API.retrieve_Data import DataAPI
from dvh_calculator.dvh_processor import process_message
from imaging_common import APIPoller

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)

_API_KEY = os.environ.get("IMAGING_API_KEY")
if not _API_KEY:
    raise RuntimeError("IMAGING_API_KEY environment variable must be set")

app = FastAPI()


@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    key = request.headers.get("X-API-Key")
    if key != _API_KEY:
        return JSONResponse(status_code=401, content={"detail": "Invalid or missing API key"})
    return await call_next(request)


@app.get("/calculate_DVH", tags=["DVH"], summary="Calculate DVH")
def calculate_dvh(patient_id: str, structure: str):
    """Calculate DVH for a patient and structure, returning JSON-LD."""
    dp = DataAPI()
    dp.get_data_api(patient_id)
    res = dp.dvh_api(structure_name=structure)
    logger.info(res)
    return JSONResponse(content=res, media_type="application/ld+json")


def _handle_package(package: dict):
    """Dispatch a polled RT package to the DVH processor."""
    study_uid = package["study_uid"]
    logger.info("Dispatching study_uid from API: %s", study_uid)
    process_message(study_uid)


def start_poller():
    """Start the RTDOSE package poller in a blocking loop."""
    poller = APIPoller(
        endpoint="/rt_package",
        request_body={"modality": "RTDOSE"},
        callback=_handle_package,
    )
    poller.poll()


def api_start():
    """Start the uvicorn API server on port 8000."""
    logger.info("Starting API server on port 8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    consumer_t = threading.Thread(target=start_poller, daemon=True)
    consumer_t.start()
    api_start()
