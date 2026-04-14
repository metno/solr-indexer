#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
SOLR-indexer : Bulkindexer - DEPRECATED wrapper
================================================

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

DEPRECATION NOTICE:
===================
This script is deprecated. Please use the unified `indexdata` command instead.

The `bulkindexer` script will be removed in a future release.
All functionality has been consolidated into the `indexdata` command which
supports both single-file and bulk indexing with automatic threshold-based
worker selection.

Migration:
  Old:  python -m solrindexer.script.bulkindexer -c config.yml -d /data
  New:  python -m solrindexer.script.indexdata -c config.yml -d /data

All arguments are compatible with the new command.
"""

import logging
import sys

from solrindexer.script import indexdata

logger = logging.getLogger(__name__)


def main():
    """
    Deprecation wrapper that delegates all arguments to the new unified indexdata command.

    This function prints a deprecation warning and then forwards all command-line arguments
    to the new indexdata.main() function. All functionality is preserved; users should
    simply switch to using the indexdata command directly.
    """
    # Print deprecation warning to stderr
    print(
        "\n" + "=" * 80,
        file=sys.stderr,
    )
    print(
        "DEPRECATION WARNING: The 'bulkindexer' command is deprecated.",
        file=sys.stderr,
    )
    print(
        file=sys.stderr,
    )
    print(
        "Please use the unified 'indexdata' command instead:",
        file=sys.stderr,
    )
    print(
        "  python -m solrindexer.script.indexdata -c config.yml [options]",
        file=sys.stderr,
    )
    print(
        file=sys.stderr,
    )
    print(
        "All arguments are compatible. The bulkindexer command will be removed",
        file=sys.stderr,
    )
    print(
        "in a future release.",
        file=sys.stderr,
    )
    print(
        "=" * 80 + "\n",
        file=sys.stderr,
    )

    # Delegate to new unified indexdata command
    return indexdata.main()


def _main():  # pragma: no cover
    """Compatibility entry point used by console_scripts in setup.cfg."""
    try:
        main()
    except ValueError as exc:
        print(exc)
    except AttributeError as exc:
        print(exc)


if __name__ == "__main__":
    _main()
