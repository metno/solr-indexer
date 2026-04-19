"""
SOLR-Indexer : Tools
=================

Copyright 2021 MET Norway

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

import dateutil.parser
import validators

# Optional imports - loaded once at module init
try:
    import xarray as xr

    HAS_XARRAY = True
except ImportError:
    xr = None  # type: ignore[assignment]
    HAS_XARRAY = False

try:
    import netCDF4  # noqa: F401

    HAS_NETCDF4 = True
except ImportError:
    HAS_NETCDF4 = False

# Logging Setup
logger = logging.getLogger(__name__)

IDREPLS = [":", "/", "."]

DATETIME_REGEX = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(\.\d+)?Z$"  # NOQA: E501
)

# Canonical feature type values keyed by normalized lowercase input.
validfeaturetypes = {
    "point": "point",
    "timeseries": "timeSeries",
    "trajectory": "trajectory",
    "profile": "profile",
    "timeseriesprofile": "timeSeriesProfile",
    "trajectoryprofile": "trajectoryProfile",
}


def get_dataset(dataset_id, *, solr_client):
    """
    Fetch dataset by id using Solr realtime get when possible.

    Returns a dict containing a ``doc`` key to match existing call sites,
    e.g. ``{"doc": <doc or None>}``.
    """
    path = f"/get?id={dataset_id}"
    try:
        result = solr_client._send_request("get", path)
        docs = json.loads(result)
        logger.debug(
            "Realtime GET, found parent: %s", docs if docs["doc"] is None else docs["doc"]["id"]
        )
        return {"doc": docs["doc"] if docs["doc"] else None}
    except Exception as exc:
        logger.error("Could not fetch dataset id=%s from Solr: %s", dataset_id, exc)
        return None


def solr_add(docs, *, solr_client):
    """Add documents to solr"""
    solr_client.add(docs)


def set_parent_flag(parent_id, *, solr_client):
    """Atomically mark a parent document as isParent=true."""
    solr_add(
        [
            {
                "id": parent_id,
                "isParent": {"set": True},
            }
        ],
        solr_client=solr_client,
    )


def resolve_parent_ids(parent_ids, *, solr_client):
    """Resolve referenced parent IDs in Solr.

    Returns ``None`` when all parent IDs are resolved. Otherwise returns a
    set containing the unresolved parent IDs.
    """
    unresolved_parent_ids = set(parent_ids)
    if not unresolved_parent_ids:
        return None

    logger.info(" --- Final parent/child integrity pass --- ")
    logger.debug("Checking %d referenced parent IDs", len(unresolved_parent_ids))

    for parent_id in list(unresolved_parent_ids):
        try:
            parent = get_dataset(parent_id, solr_client=solr_client)
            if parent is None or parent.get("doc") is None:
                logger.debug("Referenced parent %s not found in Solr", parent_id)
                continue

            if parent["doc"].get("isParent") is False:
                logger.debug("Update on indexed parent %s, isParent: True", parent_id)
                set_parent_flag(parent_id, solr_client=solr_client)

            unresolved_parent_ids.discard(parent_id)
        except Exception as exc:
            logger.warning("Final parent update failed for %s: %s", parent_id, exc)

    if unresolved_parent_ids:
        return unresolved_parent_ids
    return None


def to_solr_id(metadata_id):
    """Function that translate from metadata_identifier
    to solr compatilbe id field syntax
    """
    solr_id = str(metadata_id)
    for e in IDREPLS:
        solr_id = solr_id.replace(e, "-")

    return solr_id


def parse_date(_date):
    """Function that tries to parse date from mmd
    into correct solr date format string"""

    date = str(_date).strip()

    test = checkDateFormat(date)
    if test:
        return date
    if not test:
        logger.debug("Parsing date format %s to Solr date format", date)
        try:
            parsed_date = dateutil.parser.parse(date)
            date = parsed_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logger.error("Could not parse date: %s, reason: %s", date, e)
            return None

        logger.debug(date)
        test = checkDateFormat(date)
        if test:
            logger.debug("Parsed Solr date: %s", date)
            return date
        logger.debug("Date util failed to parse date %s. fixing...", date)
        if re.search(r"\+\d\d:\d\dZ$", date) is not None:
            date = re.sub(r"\+\d\d:\d\d", "", date)
            try:
                newdate = dateutil.parser.parse(date)
                date = newdate.strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.debug("parsed solr date: %s", date)
            except Exception as e:
                logger.error("Could not parse date: %s, reason: %s", date, e)
                return None

            return date
    return None


def checkDateFormat(date):
    """Function that use regex on the provided
    datestring and return True if in solr format.
    Return False otherwise
    """
    return bool(re.match(DATETIME_REGEX, date))


def find_xml_files(directory):
    logger.debug("Creating list of files traversing %s", directory)
    try:
        output = subprocess.check_output(
            ["find", directory, "-type", "f", "-name", "*.xml"], universal_newlines=True
        )
        return output.split("\n")[:-1]  # Remove last item which is an empty string
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while finding XML files: {str(e)}")
        return []


def _check_opendap_url(tmpdoc):
    """Get first OPeNDAP URL as string, or None when missing."""
    dapurl = tmpdoc.get("data_access_url_opendap")
    if isinstance(dapurl, list):
        dapurl = dapurl[0] if dapurl else None
    if dapurl is None:
        return None
    if not validators.url(dapurl):
        logger.warning("Opendap url not valid: %s", dapurl)
        return None

    return str(dapurl).strip()


def _fix_nersc_url(dapurl):
    """Apply HTTPS fix for legacy NERSC THREDDS URLs."""
    if dapurl.startswith("http://thredds.nersc"):
        return dapurl.replace("http:", "https:", 1)
    return dapurl


def _extract_feature_type(dapurl):
    """Open remote dataset and return (featureType, error_msg).

    Uses xarray for thread-safe extraction (netCDF4 can segfault in multithreaded contexts).
    Either value in the returned tuple may be None. error_msg is only set when an actual
    exception prevented extraction (not when the attribute is simply absent).
    """
    if not HAS_XARRAY:
        error_msg = "xarray not available"
        logger.error(
            "Cannot extract featureType from %s: xarray not installed",
            dapurl,
        )
        return (None, error_msg)
    try:
        ds = xr.open_dataset(dapurl, decode_times=False)
        try:
            ft = ds.attrs.get("featureType")
        finally:
            ds.close()
        return (ft, None)
    except ImportError:
        error_msg = "xarray not available"
        logger.error(
            "Cannot extract featureType from %s: xarray not installed",
            dapurl,
        )
        return (None, error_msg)
    except AttributeError:
        return (None, None)
    except Exception as e:
        error_msg = f"Feature type extraction failed: {e}"
        logger.error(
            "Something failed extracting featureType from %s. Reason: %s",
            dapurl,
            e,
        )
        return (None, error_msg)


def _canonical_feature_type(feature_type):
    """Map extracted feature type to canonical case-sensitive value."""
    if feature_type is None:
        return None
    return validfeaturetypes.get(str(feature_type).strip().lower())


def process_feature_type(tmpdoc):
    """
    Look for feature type and update document.

    Returns a ``(tmpdoc, error_msg)`` tuple.  ``error_msg`` is ``None``
    when processing succeeded (or when the feature type is simply not
    present); it is a non-empty string when an unexpected error occurred
    during extraction.
    """
    tmpdoc_ = tmpdoc
    metadata_status = str(tmpdoc.get("metadata_status", "unknown")).lower()
    if metadata_status == "inactive":
        return (tmpdoc, None)

    dapurl = _check_opendap_url(tmpdoc)
    if dapurl is None:
        return (tmpdoc_, None)

    # Special fix for NERSC.
    dapurl = _fix_nersc_url(dapurl)

    logger.debug("Trying to open dataset: %s", dapurl)
    feature_type, error_msg = _extract_feature_type(dapurl)

    if error_msg is not None:
        return (tmpdoc_, error_msg)

    if feature_type is None:
        return (tmpdoc_, None)

    canonical_feature_type = _canonical_feature_type(feature_type)
    if canonical_feature_type is None:
        logger.warning("The featureType found - %s - is not valid", feature_type)
        return (tmpdoc_, None)

    if str(feature_type) != canonical_feature_type:
        logger.warning(
            "Fixing featureType locally: %s -> %s", feature_type, canonical_feature_type
        )

    logger.debug("feature_type found: %s", canonical_feature_type)
    tmpdoc_.update({"feature_type": canonical_feature_type})
    return (tmpdoc_, None)


def add_nbs_thumbnail(doc, config):
    NBS_PROD_RE = r"(\w\d\w)/(\d{4})/(\d{2})/(\d{2})(?:/(IW|EW))?/(.+).zip"

    # Get the configuration
    nbs_base_path = config.get("nbs-thumbnails-base-path", None)
    nbs_base_url = config.get("nbs-thumbnails-base-url", None)
    # Extract filename and path from data_access_url_opendap
    data_access_url_http = doc.get("data_access_url_http", "")[0]
    if not data_access_url_http.endswith(".zip"):
        data_access_url_http = doc.get("data_access_url_http", "")[1]
    logger.debug(data_access_url_http)
    if data_access_url_http is not None:
        match = re.search(NBS_PROD_RE, data_access_url_http)
        if match:
            product = match.group(1)
            year = match.group(2)
            month = match.group(3)
            day = match.group(4)
            mode = match.group(5)
            fname = match.group(6)
            logger.debug(mode)
            if product.startswith("S1"):
                thumb_path = f"{nbs_base_path}/{product}/{year}"
                thumb_path += f"/{month}/{day}/{mode}/ql/{fname}/thumbnail.png"
                thumbFound = os.path.isfile(thumb_path)
                if thumbFound:
                    thumbnail_url = f"{nbs_base_url}/{product}/{year}/"
                    thumbnail_url += f"{month}/{day}/{mode}/ql/{fname}/thumbnail.png"
                    logger.info("NBS thumbnail_url set to: %s", thumbnail_url)
                    doc["thumbnail_url"] = thumbnail_url
                else:
                    logger.error("NBS thumbnail not found: %s", thumb_path)

            else:
                thumb_path = f"{nbs_base_path}/{product}/{year}"
                thumb_path += f"/{month}/{day}/ql/{fname}/thumbnail.png"

                thumbFound = os.path.isfile(thumb_path)
                if thumbFound:
                    thumbnail_url = f"{nbs_base_url}/{product}/{year}/"
                    thumbnail_url += f"{month}/{day}/ql/{fname}/thumbnail.png"
                    logger.info("NBS thumbnail_url set to: %s", thumbnail_url)
                    doc["thumbnail_url"] = thumbnail_url
                else:
                    logger.error("NBS thumbnail not found: %s", thumb_path)
    return doc


def add_nbs_thumbnail_bulk(payload):
    """Bulk-processing helper that keeps multiprocess input to one argument."""
    doc, config = payload
    return add_nbs_thumbnail(doc, config)


def _first_string(value):
    """Return first value as string for Solr fields that can be list/scalar."""
    if isinstance(value, list):
        if not value:
            return None
        first = value[0]
        if first is None:
            return None
        return str(first).strip()

    if value is None:
        return None

    return str(value).strip()


def _load_adc_path_builder():
    """Load deterministic path builder from metsis-thumbnail-generator."""
    try:
        from metsis_thumbnail_generator.thumbnail_path_api import (
            build_thumbnail_relative_path,
        )

        return build_thumbnail_relative_path
    except Exception:
        pass

    local_src = Path(__file__).resolve().parents[2] / "metsis-thumbnail-generator" / "src"
    if local_src.is_dir() and str(local_src) not in sys.path:
        sys.path.insert(0, str(local_src))

    try:
        from metsis_thumbnail_generator.thumbnail_path_api import (
            build_thumbnail_relative_path,
        )

        return build_thumbnail_relative_path
    except Exception as exc:
        logger.error(
            "ADC thumbnail path resolver unavailable from metsis-thumbnail-generator: %s",
            exc,
        )
        return None


def load_adc_thumbnail_path_contract_cases():
    """Load shared deterministic thumbnail contract vectors."""
    try:
        from metsis_thumbnail_generator.thumbnail_path_api import (
            THUMBNAIL_PATH_CONTRACT_CASES,
        )

        return THUMBNAIL_PATH_CONTRACT_CASES
    except Exception:
        pass

    local_src = Path(__file__).resolve().parents[2] / "metsis-thumbnail-generator" / "src"
    if local_src.is_dir() and str(local_src) not in sys.path:
        sys.path.insert(0, str(local_src))

    try:
        from metsis_thumbnail_generator.thumbnail_path_api import (
            THUMBNAIL_PATH_CONTRACT_CASES,
        )

        return THUMBNAIL_PATH_CONTRACT_CASES
    except Exception as exc:
        logger.error(
            "ADC thumbnail contract cases unavailable from metsis-thumbnail-generator: %s",
            exc,
        )
        return ()


def add_adc_thumbnails(doc, config):
    """Set thumbnail_url from deterministic ADC pre-generated thumbnail path."""
    adc_base_path = config.get("adc-thumbnails-base-path")
    adc_base_url = config.get("adc-thumbnails-base-url")
    if not adc_base_path or not adc_base_url:
        logger.warning(
            "ADC scope enabled but adc-thumbnails-base-path/base-url are not configured"
        )
        return doc

    metadata_identifier = _first_string(doc.get("metadata_identifier"))
    if not metadata_identifier:
        logger.debug("Skipping ADC thumbnail lookup: metadata_identifier missing")
        return doc

    wms_url = _first_string(doc.get("data_access_url_ogc_wms") or doc.get("wms_url"))
    start_date = _first_string(doc.get("temporal_extent_start_date") or doc.get("start_date"))
    if not wms_url:
        logger.debug(
            "Skipping ADC thumbnail lookup for %s: WMS URL missing",
            metadata_identifier,
        )
        return doc

    build_thumbnail_relative_path = _load_adc_path_builder()
    if build_thumbnail_relative_path is None:
        return doc

    relative_path = build_thumbnail_relative_path(
        metadata_identifier=metadata_identifier,
        start_date=start_date,
        wms_url=wms_url,
    )
    thumbnail_path = Path(str(adc_base_path)) / relative_path
    if not thumbnail_path.is_file():
        logger.error("ADC thumbnail not found: %s", thumbnail_path)
        return doc

    thumbnail_url = f"{str(adc_base_url).rstrip('/')}/{relative_path.as_posix()}"
    logger.info("ADC thumbnail_url set to: %s", thumbnail_url)
    doc["thumbnail_url"] = thumbnail_url
    return doc


def add_adc_thumbnails_bulk(payload):
    """Bulk-processing helper that keeps multiprocess input to one argument."""
    doc, config = payload
    return add_adc_thumbnails(doc, config)


def main() -> None:
    logger.info("Tools Main")


if __name__ == "__main__":  # pragma: no cover
    main()
