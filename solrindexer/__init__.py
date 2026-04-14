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

import logging
import os
import sys

from .bulkindexer import BulkIndexer
from .indexdata import IndexMMD, MMD4SolR

try:
    from importlib.metadata import PackageNotFoundError, version
except ImportError:
    from importlib_metadata import PackageNotFoundError, version  # type: ignore

try:
    __version__ = version("solrindexer")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__package__ = "solrindexer"
__date__ = "2025-12-04"
__all__ = ["IndexMMD", "MMD4SolR", "BulkIndexer"]


class InfoFilter(logging.Filter):
    def filter(self, rec: logging.LogRecord) -> bool:
        return rec.levelno == logging.INFO


def _init_logging(log_obj: logging.Logger) -> None:
    """Initialize package logging from environment variables.

    Behavior:
    - Reads log level from SOLRINDEXER_LOGLEVEL (default: INFO)
    - Reads optional logfile path from SOLRINDEXER_LOGFILE
    - Uses rich.logging.RichHandler for console output when rich is installed,
      otherwise falls back to a plain stdout/stderr split handler setup
    - Sends DEBUG/INFO to stdout and WARNING+ to stderr (no duplicates)
    """
    want_level = os.environ.get("SOLRINDEXER_LOGLEVEL", "INFO").upper()
    log_file = os.environ.get("SOLRINDEXER_LOGFILE")

    log_level = getattr(logging, want_level, logging.INFO)
    if not hasattr(logging, want_level):
        print(
            f"Invalid logging level '{want_level}' in environment variable SOLRINDEXER_LOGLEVEL",
            file=sys.stderr,
        )

    debug_fmt = "[{asctime:}]  [{processName:s}] [{threadName:s}] [{levelname:7s}] {name}:{lineno:<4d} : {message:}"
    info_fmt = "[{asctime:}] {levelname:8s}: {message:}"
    plain_format = logging.Formatter(
        fmt=debug_fmt if log_level == logging.DEBUG else info_fmt, style="{"
    )

    # Make init idempotent (important for tests/import cycles)
    log_obj.handlers.clear()
    log_obj.setLevel(log_level)
    log_obj.propagate = False

    try:
        from rich.console import Console
        from rich.logging import RichHandler

        show_path = log_level == logging.DEBUG

        # DEBUG/INFO → stdout
        stdout_rich = RichHandler(
            level=log_level,
            console=Console(file=sys.stdout),
            rich_tracebacks=True,
            show_path=show_path,
        )
        stdout_rich.addFilter(lambda record: record.levelno <= logging.INFO)
        log_obj.addHandler(stdout_rich)

        # WARNING+ → stderr (respects 2>/dev/null)
        stderr_rich = RichHandler(
            level=logging.WARNING,
            console=Console(file=sys.stderr, stderr=True),
            rich_tracebacks=True,
            show_path=show_path,
        )
        log_obj.addHandler(stderr_rich)
    except ImportError:
        # Fallback: stdout for DEBUG/INFO, stderr for WARNING+
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(log_level)
        stdout_handler.setFormatter(plain_format)
        stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
        log_obj.addHandler(stdout_handler)

        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.WARNING)
        stderr_handler.setFormatter(plain_format)
        log_obj.addHandler(stderr_handler)

    # Optional logfile: always plain text regardless of rich availability
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(log_level)
        file_handler.setFormatter(plain_format)
        log_obj.addHandler(file_handler)


# Logging Setup
logger = logging.getLogger(__name__)
_init_logging(logger)
