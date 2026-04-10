"""Upload JSON-LD data to a GraphDB repository."""

import logging

import requests

from dvh_calculator.config import yaml_config as _config

logger = logging.getLogger(__name__)


def upload_jsonld_to_graphdb(jsonld_data, graphdb_url):
    """Upload a JSON-LD dictionary to a GraphDB repository via the REST API."""
    config_dict_api = _config.get("API", {})
    host = config_dict_api["host"]
    port = config_dict_api["port"]
    logger.info("GraphDB URL: %s, Host: %s, Port: %s", graphdb_url, host, port)
    headers = {"Content-Type": "application/ld+json"}
    logger.info("Uploading")
    params = {"endpoint": graphdb_url}

    url = f"http://{host}:{port}/upload_json"
    response = requests.post(url, headers=headers, json=jsonld_data, params=params, timeout=30)

    if response.status_code in {200, 204}:
        logger.info("Success upload..")
    else:
        logger.warning("Failed to upload data. Status code: %s, %s", response.status_code, response.text)
        msg = f"Failed to upload data to GraphDB. Status code: {response.status_code}"
        raise RuntimeError(msg)
