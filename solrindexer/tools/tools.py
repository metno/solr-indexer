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

import os
import re
import math
import fnmatch
import logging
import dateutil.parser

# Logging Setup
logger = logging.getLogger(__name__)

IDREPLS = [':', '/', '.']

DATETIME_REGEX = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(\.\d+)?Z$"  # NOQA: E501
)


def flip(x, y):
    """Flips the x and y coordinate values"""
    return y, x


def rewrap(x):
    """Rewrap coordinates from 0-360 to -180-180"""
    return (x + 180) % 360 - 180


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
        parsed_date = dateutil.parser.parse(_date)
        date = parsed_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        logger.debug(date)
        test = checkDateFormat(date)
        if test:
            logger.debug("parsed solr date: %s", date)
            return date
        else:
            logger.debug("dateformat not solr compatible. fixing...")
            if re.search(r'\+\d\d:\d\dZ$', date) is not None:
                date = re.sub(r'\+\d\d:\d\d', '', date)
                newdate = dateutil.parser.parse(date)
                date = newdate.strftime('%Y-%m-%dT%H:%M:%SZ')
                logger.debug("parsed solr date: %s", date)
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


def flatten(mylist):
    """Flatten a multi-dementional list"""
    return [item for sublist in mylist for item in sublist]
