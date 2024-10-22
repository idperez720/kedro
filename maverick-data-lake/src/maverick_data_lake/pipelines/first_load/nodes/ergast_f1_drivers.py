import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List

import requests
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_driver_info(
    driver: ET.Element, namespace: Dict[str, str], year: str
) -> Dict[str, Any]:
    """Helper function to extract driver information from XML element."""
    return {
        "driver_id": driver.get("driverId"),
        "code": driver.get("code"),
        "permanent_number": (
            driver.find("ns:PermanentNumber", namespace).text
            if driver.find("ns:PermanentNumber", namespace) is not None
            else None
        ),
        "given_name": driver.find("ns:GivenName", namespace).text,
        "family_name": driver.find("ns:FamilyName", namespace).text,
        "date_of_birth": driver.find("ns:DateOfBirth", namespace).text,
        "nationality": driver.find("ns:Nationality", namespace).text,
        "grid_year": year,
    }


def fetch_and_parse_url(url: str, namespace: Dict[str, str]) -> List[Dict[str, Any]]:
    """Fetches and parses XML data from the given URL."""
    response = requests.get(url, timeout=1000)
    if response.status_code == 200:  # noqa: PLR2004
        root = ET.fromstring(response.content.decode("utf-8"))
        return [
            fetch_driver_info(driver, namespace, url.split("/")[-2])
            for driver in root.findall(".//ns:Driver", namespaces=namespace)
        ]
    else:
        logging.warning(
            "Failed to get drivers data. Status code: %s", response.status_code
        )
        return []


def create_drivers_data(params: Dict[str, Any]) -> SparkDataFrame:
    """Extracts data from the Ergast API for Formula 1 drivers.

    Args:
        params (Dict[str, Any]): Parameters to be used in the function

    Returns:
        SparkDataFrame: Dataframe with the drivers data
    """
    min_year = params["min_year"]
    max_year = datetime.now().year
    base_url = params["base_url"]
    tail_url = params["tail_url"]
    namespace = params["namespace"]

    spark = SparkSession.builder.appName("GETRequestToSpark").getOrCreate()

    urls = [f"{base_url}/{year}/{tail_url}" for year in range(min_year, max_year)]
    all_drivers_data = [
        driver for url in urls for driver in fetch_and_parse_url(url, namespace)
    ]

    return spark.createDataFrame(all_drivers_data)
