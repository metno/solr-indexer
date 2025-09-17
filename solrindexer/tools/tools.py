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
import netCDF4
import pysolr
import requests
import shapely
import validators
from shapely import wkt
from shapely.ops import transform

from solrindexer.thumb.thumbnail_api import create_wms_thumbnail_api

# Logging Setup
logger = logging.getLogger(__name__)

lock = Lock()

IDREPLS = [':', '/', '.']

DATETIME_REGEX = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(\.\d+)?Z$"  # NOQA: E501
)

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

    logger.debug("parsing date: %s", date)
    test = checkDateFormat(date)
    if test:
        logger.debug("date already solr compatible.")
        return date
    elif not test:
        try:
            parsed_date = dateutil.parser.parse(date)
            date = parsed_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            logger.error("Could not parse date: %s, reason: %s", date, e)
            return None

        logger.debug(date)
        test = checkDateFormat(date)
        if test:
            logger.debug("parsed solr date: %s", date)
            return date
        else:
            logger.debug("dateformat not solr compatible. fixing...")
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


def process_feature_type(tmpdoc):
    """
    Look for feature type and update document
    """
    dapurl = None
    tmpdoc_ = tmpdoc
    metadata_status = tmpdoc.get('metadata_status', 'unknown')
    if metadata_status == 'Inactive' or metadata_status == 'inactive':
        return tmpdoc
    if 'data_access_url_opendap' in tmpdoc:
        dapurl = tmpdoc['data_access_url_opendap']
        if (isinstance(dapurl, list)):
            dapurl = dapurl[0]
        dapurl = str(dapurl).strip()
        valid = validators.url(dapurl)
        # Special fix for nersc.
        if dapurl.startswith("http://thredds.nersc"):
            dapurl.replace("http:", "https:")

        if not valid:
            logger.warn("Opendap url not valid: %s", dapurl)
            return tmpdoc_

    # if 'storage_information_file_location' in tmpdoc:
    #     fileloc = str(tmpdoc['storage_information_file_location']).strip()
    #     if os.path.isfile(fileloc):
    #         dapurl = fileloc
    #         logger.debug("Setting dapurl to read from lustre location: %s", dapurl)
    if dapurl is not None:
        logger.debug("Trying to open netCDF file: %s", dapurl)
        # lock.acquire()
        ds = None
        try:
            ds = netCDF4.Dataset(dapurl, 'r')
        except Exception as e:
            logger.error("Something failed reading netcdf %s. Reason: %s", dapurl, e)
            if ds is not None:
                ds.close()
            # Set to inactive if file not found.
            # if str(e).ststartswith('[Errno -90] NetCDF: file not found:'):
            #     logger.info("Setting dataset %s to Inactive", tmpdoc_['metadata_identifier'])
            #     tmpdoc_.update({"metadata_status": "Inactive"})
            return tmpdoc_

        # Try to get the global attribute featureType
        featureType = None
        try:
            featureType = ds.getncattr('featureType')
        except AttributeError:
            pass
        except Exception as e:
            logger.error("Something failed extracting featureType: %s", str(e))
            ds.close()
            # lock.release()
            return tmpdoc_

        if featureType is not None:
            logger.debug("Got featuretype: %s", featureType)
            validfeaturetypes = {'point' : 'point', 'timeseries' : 'timeSeries',
                             'trajectory' : 'trajectory', 'profile' : 'profile',
                             'timeseriesprofile' : 'timeSeriesProfile', 'trajectoryprofile' : 'trajectoryProfile'}

            if featureType not in validfeaturetypes.values():
                logger.warning(
                    "The featureType found - %s - is not valid", featureType)
                logger.warning("Fixing this locally")
                if featureType.lower() in validfeaturetypes.keys():
                    featureType = validfeaturetypes[featureType.lower()]
                else:
                    print("The featureType cannot be mapped to any valid value")
                    featureType = None

            if featureType is not None:
                logger.debug('feature_type found: %s', featureType)
                tmpdoc_.update({'feature_type': featureType})
            else:
                logger.debug('Neither gridded nor discrete sampling \
                            geometry found in this record...')

        polygon = None
        try:
            polygon = ds.getncattr('geospatial_bounds')
        except AttributeError:
            pass
        except Exception as e:
            logger.error("Something failed extracting geospatial_bounds: %s", str(e))
            ds.close()
            # lock.release()
            return tmpdoc_
            # Check if we have plogon.

        if polygon is not None:
            logger.debug("Reading geospatial_bounds")
            try:
                polygon_ = wkt.loads(polygon)
            except Exception as e:
                logger.warning("Could not parse geospatial_bounds: %s, Reason: %s", polygon, e)
                ds.close()
                return tmpdoc_
            geom_type = polygon_.geom_type
            logger.debug("Got geospatial type %s with bounds: %s", geom_type, polygon)
            if geom_type == 'Point':
                point_ = polygon_
                point = polygon_.wkt
                if shapely.has_z(point_):
                    point_ = shapely.force_2d(point_)
                    point = point_.wkt
                if 'polygon_rpt' in tmpdoc_:
                    parsed_point = wkt.loads(tmpdoc_['polygon_rpt'])
                    if not parsed_point.equals(point_):
                        point = transform(flip, point_).wkt
                tmpdoc_.update({'geospatial_bounds': point})

            elif geom_type == 'MultiPoint':
                mpoint_ = polygon_
                mpoint = polygon_.wkt
                if shapely.has_z(polygon_):
                    mpoint_ = shapely.force_2d(polygon_)
                    mpoint = mpoint_.wkt
                mpoint = transform(flip, mpoint_).wkt
                tmpdoc_.update({'geospatial_bounds': mpoint})

            else:
                try:
                    polygon = transform(flip, polygon_)
                except Exception:
                    logger.warning("Could not transform incoming geospatial bounds: %s", polygon_)
                    pass
                else:
                    tmpdoc_.update({'geospatial_bounds': polygon.wkt})
                    tmpdoc_.update({'polygon_rpt': polygon.wkt})

        bounds_crs = None
        try:
            bounds_crs = ds.getncattr('geospatial_bounds_crs')
        except AttributeError:
            pass
        except Exception as e:
            logger.error("Something failed extracting geospatial_bounds_crs: %s", str(e))
            ds.close()
            # lock.release()
            return tmpdoc_
            # Check if we have plogon.
        if bounds_crs is not None:
            crs = str(bounds_crs).strip()
            logger.debug("Got geospatial bounds CRS: %s", crs)
            tmpdoc_.update({'geographic_extent_polygon_srsName': crs})

        logger.debug("Closing netCDF file.")
        ds.close()
        # lock.release()
        return tmpdoc_

    return tmpdoc_


def create_wms_thumbnail(doc):
    """ Add thumbnail to SolR
        Args:
            type: solr document
        Returns:
            solr document with thumbnail
    """
    # global thumbClass
    doc_ = doc.copy()
    ogc_wms_url = doc['data_access_url_ogc_wms']
    if isinstance(ogc_wms_url, list):
        ogc_wms_url = ogc_wms_url[0]
    url = str(ogc_wms_url).strip()
    id = str(doc['id']).strip()
    logger.debug("adding thumbnail for %s with url: %s", id, url)
    wms_layers_mmd = []
    if 'data_access_wms_layers' in doc:
        wms_layers_mmd = doc['data_access_wms_layers']
        logger.debug("wms_layers_mmd is: %s", wms_layers_mmd)
    try:
        thumbnail_data = thumbClass.create_wms_thumbnail(url, id, wms_layers_mmd)
        doc_.update({'thumbnail_data': thumbnail_data})
    except Exception as e:
        logger.error("Thumbnail creation from OGC WMS failed: %s, id: %s", e, id)
        # raise Exception("Thumbnail creation from OGC WMS failed: %s, id: %s", e, id)
        pass
    finally:
        return doc_


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


def create_wms_thumbnail_api_wrapper(doc):
    """ Add thumbnail to SolR using API wms generator
        Args:
            type: solr document
        Returns:
            solr document with thumbnail
    """
    # global thumbClass
    wmsconfig = thumbClass.copy()
    doc_ = doc.copy()
    url = str(doc['data_access_url_ogc_wms']).strip()
    id = str(doc['id']).strip()
    logger.debug("adding thumbnail for %s with url: %s", id, url)
    wms_layers_mmd = []
    if 'data_access_wms_layers' in doc:
        wms_layers_mmd = doc['data_access_wms_layers']
        logger.debug("wms_layers_mmd is: %s", wms_layers_mmd)
    wmsconfig.update({'wms_url': url})
    wmsconfig.update({'wms_layers_mmd': wms_layers_mmd})
    wmsconfig.update({"id": id})
    logger.debug("Calling WMS ThumbnailAPI with settings: %s", wmsconfig)
    response = create_wms_thumbnail_api(wmsconfig)
    logger.debug("WMS api response: %s", response)
    error = response.get('error')
    status_code = response.get('status_code')
    if error is None and status_code == 200:
        thumbnail_url = response.get("data", None).get("thumbnail_url", None)
        if thumbnail_url is not None:
            logger.debug("Adding thumbnail_url field with value: %s",
                         thumbnail_url)
            doc_.update({'thumbnail_url': thumbnail_url})
        else:
            logger.warning("Could not properly generate thumbnail")
        #     # If WMS is not available, remove this data_access element
        #     # from the XML that is indexed
        #     del input_record['data_access_url_ogc_wms']
    else:
        logger.error("Could not generate thumbnail, reason: %s, status_code %s",
                     error, status_code)
    return doc_


def main():
    logger.info("Tools Main")


if __name__ == "__main__":  # pragma: no cover
    main()
