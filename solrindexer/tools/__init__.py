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

from .mmd_solr_spatial import handle_solr_spatial
from .tools import (
           add_nbs_thumbnail,
           add_nbs_thumbnail_bulk,
           checkDateFormat,
           create_wms_thumbnail,
           create_wms_thumbnail_api_wrapper,
           find_xml_files,
           flatten,
           flip,
           get_dataset,
           getListOfFiles,
           getZones,
           initSolr,
           initThumb,
           parse_date,
           process_feature_type,
           rewrap,
           rewrap_to_360,
           solr_add,
           solr_commit,
           solr_ping,
           to_solr_id,
)

__package__ = "tools"
__version__ = "2.0.2"
__date__ = "2024-01-23"
__all__ = ["flip", "rewrap", "rewrap_to_360", "to_solr_id",
           "parse_date", "getZones", "checkDateFormat",
           "getListOfFiles", "flatten", "process_feature_type",
           "initThumb", "create_wms_thumbnail", "initSolr",
           "get_dataset", "solr_add", "solr_commit", "handle_solr_spatial",
           "create_wms_thumbnail_api_wrapper", "find_xml_files",
           "solr_ping", "add_nbs_thumbnail", "add_nbs_thumbnail_bulk"]
