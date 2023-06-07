"""
SOLR-indexer : Multithreaded Bulkindexer IO - Helper functions
================================

Copyright MET Norway

Licensed under the GNU GENERAL PUBLIC LICENSE, Version 3; you may not
use this file except in compliance with the License. You may obtain a
copy of the License at

    https://www.gnu.org/licenses/gpl-3.0.en.html

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.
"""

import logging
import xmltodict
from pathlib import Path

from concurrent.futures import ThreadPoolExecutor

# Logging Setup
logger = logging.getLogger(__name__)


def load_file(filename):
    """
    Load xml file and convert to dict using xmltodict
    """
    filename = str(filename).strip().rstrip()
    try:
        file = Path(filename)
    except Exception as e:
        logger.error('Not a valid filepath %s error was %s' % (filename, e))
        return None
    with open(file, encoding='utf-8') as fd:
        try:
            xmlfile = fd.read()
        except Exception as e:
            logger.error('Could not read file %s error was %s' % (filename, e))
            return None
        try:
            mmddict = xmltodict.parse(xmlfile)
        except Exception as e:
            logger.error('Could not parse the xmlfile: %s  with error %s' % (filename, e))
            return None
        return mmddict


def load_files(filelist, threads=10):
    """
    Multithreaded function to load files using the load_file function
    """
    with ThreadPoolExecutor(threads) as exe:
        # load files
        futures = [exe.submit(load_file, name) for name in filelist]
        # collect data
        mmd_list = [future.result() for future in futures]
        # return data and file paths
        return (mmd_list)
