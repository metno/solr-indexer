"""
SOLR-indexer : Tools Package Init
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

from .tools import flip
from .tools import flatten
from .tools import rewrap
from .tools import to_solr_id
from .tools import getZones
from .tools import solr_ping
from .tools import parse_date
from .tools import checkDateFormat
from .tools import getListOfFiles
from .tools import process_feature_type
from .tools import initThumb
from .tools import initSolr, find_xml_files
from .tools import create_wms_thumbnail
from .tools import create_wms_thumbnail_api_wrapper
from .tools import get_dataset, solr_add, solr_commit


__package__ = "tools"
__version__ = "2.0.2"
__date__ = "2024-01-23"
__all__ = ["flip", "rewrap", "to_solr_id",
           "parse_date", "getZones", "checkDateFormat",
           "getListOfFiles", "flatten", "process_feature_type",
           "initThumb", "create_wms_thumbnail", "initSolr",
           "get_dataset", "solr_add", "solr_commit",
           "create_wms_thumbnail_api_wrapper", "find_xml_files",
           "solr_ping"]
