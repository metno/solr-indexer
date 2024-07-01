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

import os
import logging
import sys

from .indexdata import IndexMMD
from .indexdata import MMD4SolR
from .bulkindexer import BulkIndexer

__package__ = "solrindexer"
__version__ = "2.0.2"
__date__ = "2024-01-23"
__all__ = ["IndexMMD", "MMD4SolR", "BulkIndexer"]


class InfoFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == logging.INFO


def _init_logging(log_obj):
    """Call to initialise logging."""
    # Read environment variables
    want_level = os.environ.get("SOLRINDEXER_LOGLEVEL", "INFO")
    log_file = os.environ.get("SOLRINDEXER_LOGFILE", None)

    # Determine log level and format
    if hasattr(logging, want_level):
        log_level = getattr(logging, want_level)
    else:
        print(
            "Invalid logging level '%s' in environment variable SOLRINDEXER_LOGLEVEL" % want_level)
        log_level = logging.INFO

    if log_level < logging.INFO:
        msg_format = "[{asctime:}] [{thread:d}] [{threadName:s}]"
        msg_format += " {name:>28}:{lineno:<4d} {levelname:8s} {message:}"
    else:
        msg_format = "[{asctime:}] [{processName:s}] [{threadName:s}] {levelname:8s} {message:}"

    log_format = logging.Formatter(fmt=msg_format, style="{")
    log_obj.setLevel(log_level)

    # Create stream handlers
    # h_stdout = logging.StreamHandler()
    # h_stdout.setLevel(log_level)
    # h_stdout.setFormatter(log_format)
    # log_obj.addHandler(h_stdout)

    if log_file is not None:
        h_file = logging.FileHandler(log_file, encoding="utf-8")
        h_file.setLevel(log_level)
        h_file.setFormatter(log_format)
        log_obj.addHandler(h_file)

    # Create a handler for stdout, set its level to INFO.
    stdout_format = "[{asctime:}] [{processName:s}] [{threadName:s}] {message:}"
    stdout_log_format = logging.Formatter(fmt=stdout_format, style="{")
    stdout_handler = logging.StreamHandler(sys.stdout)
    if log_level == logging.INFO:
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.addFilter(InfoFilter())
    stdout_handler.setFormatter(stdout_log_format)

    # Create a handler for stderr, set its level to WARNING.
    stderr_format = "[{asctime:}] [{thread:d}] [{threadName:s}]"
    stderr_format += " {name:>28}:{lineno:<4d} {levelname:8s} {message:}"
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_log_format = logging.Formatter(fmt=stderr_format, style="{")
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(stderr_log_format)

    # Add the handlers to the logger.
    log_obj.addHandler(stdout_handler)
    log_obj.addHandler(stderr_handler)

    # Set logger level to the lowest level, this level is used to determine
    # whether a incoming message should be processed.
    # logger.setLevel(logging.INFO)

    return


# Logging Setup
logger = logging.getLogger(__name__)
_init_logging(logger)
