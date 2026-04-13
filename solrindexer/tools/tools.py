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

import fnmatch
import json
import logging
import math
import os
import re
import subprocess
import sys
from threading import Lock

import dateutil.parser
import pysolr
import requests
import validators

# Logging Setup
logger = logging.getLogger(__name__)

lock = Lock()

IDREPLS = [':', '/', '.']

DATETIME_REGEX = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(\.\d+)?Z$"  # NOQA: E501
)

# Canonical feature type values keyed by normalized lowercase input.
validfeaturetypes = {
    'point': 'point',
    'timeseries': 'timeSeries',
    'trajectory': 'trajectory',
    'profile': 'profile',
    'timeseriesprofile': 'timeSeriesProfile',
    'trajectoryprofile': 'trajectoryProfile',
}

# Global thumb class
global thumbClass
thumbClass = None

# Global Solr connection
global solr_endpoint
solr_endpoint = None

global solr_pysolr
solr_pysolr = None

global authClass
authClass = None


def initThumb(thumb):
    """Initialise configured thumbnail class"""
    global thumbClass
    thumbClass = thumb


def initSolr(solrc, solrcon, auth):
    """Initialize Solr"""
    global solr_endpoint
    solr_endpoint = solrc

    global solr_pysolr
    solr_pysolr = solrcon

    global authClass
    authClass = auth


def get_dataset(id):
    """
    Use real-time get to fetch latest dataset
    based on id.
    """
    res = None
    try:
        res = requests.get(solr_endpoint + '/get?wt=json&id=' + id,
                           auth=authClass)
        res.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logger.error("Http Error: %s", errh)
    except requests.exceptions.ConnectionError as errc:
        logger.error("Error Connecting: %s", errc)
    except requests.exceptions.Timeout as errt:
        logger.error("Timeout Error: %s", errt)
    except requests.exceptions.RequestException as err:
        logger.error("OOps: Something Else went wrong: %s", err)

    if res is None:
        return None
    else:
        dataset = res.json()
        return dataset


def solr_add(docs):
    """Add documents to solr"""
    solr_pysolr.add(docs)


def solr_ping():
    """Ping Solr"""
    try:
        pong = solr_pysolr.ping()
        status = json.loads(pong)['status']
        if status == 'OK':
            logger.info('Solr ping with status %s', status)
        else:
            logger.error('Error! Solr ping with status %s', status)
            sys.exit(1)

    except pysolr.SolrError as e:
        logger.error(f"Could not contact solr server: {e}")
        sys.exit(1)


def solr_commit():
    """Commit solr transaction and open new searcher"""
    solr_pysolr.commit()


def flip(x, y):
    """Flips the x and y coordinate values"""
    return y, x


def rewrap(x):
    """Rewrap coordinates from 0-360 to -180-180"""
    return (x + 180.) % 360. - 180.

def rewrap_to_360(x):
    """Rewrap coordinates from -180-180 to 0-360"""
    return (x + 360) % 360

def to_solr_id(id):
    """Function that translate from metadata_identifier
    to solr compatilbe id field syntax
    """
    solr_id = str(id)
    for e in IDREPLS:
        solr_id = solr_id.replace(e, '-')

    return solr_id


def parse_date(_date):
    """Function that tries to parse date from mmd
    into correct solr date format string"""

    date = str(_date).strip()

    test = checkDateFormat(date)
    if test:
        return date
    elif not test:
        logger.debug("Parsing date format %s to Solr date format", date)
        try:
            parsed_date = dateutil.parser.parse(date)
            date = parsed_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            logger.error("Could not parse date: %s, reason: %s", date, e)
            return None

        logger.debug(date)
        test = checkDateFormat(date)
        if test:
            logger.debug("Parsed Solr date: %s", date)
            return date
        else:
            logger.debug("Date util failed to parse date %s. fixing...", date)
            if re.search(r'\+\d\d:\d\dZ$', date) is not None:
                date = re.sub(r'\+\d\d:\d\d', '', date)
                try:
                    newdate = dateutil.parser.parse(date)
                    date = newdate.strftime('%Y-%m-%dT%H:%M:%SZ')
                    logger.debug("parsed solr date: %s", date)
                except Exception as e:
                    logger.error("Could not parse date: %s, reason: %s", date, e)
                    return None

                return date
    else:
        return None


def getZones(lon, lat):
    "get UTM zone number from latitude and longitude"
    if lat >= 72.0 and lat < 84.0:
        if lon >= 0.0 and lon < 9.0:
            return 31
        if lon >= 9.0 and lon < 21.0:
            return 33
        if lon >= 21.0 and lon < 33.0:
            return 35
        if lon >= 33.0 and lon < 42.0:
            return 37
    if lat >= 56 and lat < 64.0 and lon >= 3 and lon <= 12:
        return 32
    return math.floor((lon + 180) / 6) + 1


def checkDateFormat(date):
    """Function that use regex on the provided
    datestring and return True if in solr format.
    Return False otherwise
    """
    return bool(re.match(DATETIME_REGEX, date))


def getListOfFiles(dirName):
    """
    create a list of file and sub directories
    names in the given directory
    """
    logger.debug("Creating list of files traversing %s", dirName)
    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(dirName):
        for filename in fnmatch.filter(filenames, '*.xml'):
            listOfFiles.append(os.path.join(dirpath, filename))
    logger.debug("Found %d files.", len(listOfFiles))
    if len(listOfFiles) == 0:
        return None
    return listOfFiles


def find_xml_files(directory):
    logger.debug("Creating list of files traversing %s", directory)
    try:
        output = subprocess.check_output(
            ["find", directory, "-type", "f", "-name", "*.xml"],
            universal_newlines=True
        )
        return output.split('\n')[:-1]  # Remove last item which is an empty string
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while finding XML files: {str(e)}")
        return []


def flatten(mylist):
    """Flatten a multi-dementional list"""
    return [item for sublist in mylist for item in sublist]


def _check_opendap_url(tmpdoc):
    """Get first OPeNDAP URL as string, or None when missing."""
    dapurl = tmpdoc.get('data_access_url_opendap')
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
    try:
        import xarray as xr
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
            dapurl, e,
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
    metadata_status = str(tmpdoc.get('metadata_status', 'unknown')).lower()
    if metadata_status == 'inactive':
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
        logger.warning("Fixing featureType locally: %s -> %s", feature_type, canonical_feature_type)

    logger.debug('feature_type found: %s', canonical_feature_type)
    tmpdoc_.update({'feature_type': canonical_feature_type})
    return (tmpdoc_, None)


def add_nbs_thumbnail(doc, config):
    NBS_PROD_RE = r"(\w\d\w)/(\d{4})/(\d{2})/(\d{2})(?:/(IW|EW))?/(.+).zip"

    # Get the configuration
    nbs_base_path = config.get('nbs-thumbnails-base-path', None)
    nbs_base_url = config.get('nbs-thumbnails-base-url', None)
    # Extract filename and path from data_access_url_opendap
    data_access_url_http = doc.get('data_access_url_http', '')[0]
    if not data_access_url_http.endswith('.zip'):
        data_access_url_http = doc.get('data_access_url_http', '')[1]
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
                    doc['thumbnail_url'] = thumbnail_url
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
                    doc['thumbnail_url'] = thumbnail_url
                else:
                    logger.error("NBS thumbnail not found: %s", thumb_path)
    return doc


def add_nbs_thumbnail_bulk(doc):
    NBS_PROD_RE = r"(\w\d\w)/(\d{4})/(\d{2})/(\d{2})(?:/(IW|EW))?/(.+).zip"

    # Get the configuration
    nbs_base_path = thumbClass.get('nbs_base_path', None)
    nbs_base_url = thumbClass.get('nbs_base_url', None)
    # Extract filename and path from data_access_url_opendap
    data_access_url_http = doc.get('data_access_url_http', '')[0]
    if not data_access_url_http.endswith('.zip'):
        data_access_url_http = doc.get('data_access_url_http', '')[1]
    logger.debug(data_access_url_http)

    title = doc.get('title', [])[0]
    logger.debug(title)
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
                    logger.debug("NBS thumbnail_url set to: %s", thumbnail_url)
                    doc['thumbnail_url'] = thumbnail_url
                else:
                    logger.error("NBS thumbnail not found: %s", thumb_path)

            else:
                thumb_path = f"{nbs_base_path}/{product}/{year}"
                thumb_path += f"/{month}/{day}/ql/{fname}/thumbnail.png"

                thumbFound = os.path.isfile(thumb_path)
                if thumbFound:
                    thumbnail_url = f"{nbs_base_url}/{product}/{year}/"
                    thumbnail_url += f"{month}/{day}/ql/{fname}/thumbnail.png"
                    logger.debug("NBS thumbnail_url set to: %s", thumbnail_url)
                    doc['thumbnail_url'] = thumbnail_url
                else:
                    logger.error("NBS thumbnail not found: %s", thumb_path)
    return doc


def main():
    logger.info("Tools Main")


if __name__ == "__main__":  # pragma: no cover
    main()
