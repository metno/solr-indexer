"""
SOLR-indexer : Main Package Init
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

from .thumbnail import WMSThumbNail

__package__ = "wmsthumbnail"
__version__ = "2.0.2"
__date__ = "2024-01-23"
__all__ = ["WMSThumbNail"]
