"""
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

PURPOSE:
    This searches SolR for specific records and optionally deletes
    them. It can also optionally create a list of identifiers to
    delete. Search is done in ID for now.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2021-02-10

"""

import argparse
import json
import logging
import os
import sys

import lxml.etree as ET
import pysolr
import requests
import yaml
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)
if os.getenv("SOLRINDEXER_LOGLEVEL", "INFO") == "DEBUG":
    logger.setLevel(logging.DEBUG)
    logger.debug("Loglevel was set to DEBUG")


SOLR_FL = "*,personnel_json:[json],data_access_json:[json],platform_json:[json],geometry_geojson:[json],related_information_json:[json],last_metadata_update_json:[json]"
SOLR_MMD_FL = "mmd_xml_file:[xml]"


def _print_pretty_docs(docs):
    """Print docs using rich JSON if available, otherwise plain pretty JSON."""
    if not docs:
        return

    fields_to_remove = {"_version_", "_root_", "mmd_xml_file"}
    filtered_docs = []
    for doc in docs:
        filtered_doc = {
            k: v for k, v in doc.items() if k not in fields_to_remove and not k.endswith("_facet")
        }
        filtered_docs.append(filtered_doc)

    pretty = json.dumps(filtered_docs, ensure_ascii=False, indent=2)
    try:
        from rich import print_json as rich_print_json

        rich_print_json(pretty)
        return
    except ImportError:
        pass
    print(pretty)


def _format_xml_for_display(xml_text):
    """Return pretty-printed XML when parsing succeeds, otherwise the original text."""
    if not xml_text:
        return xml_text
    try:
        root = ET.fromstring(xml_text.encode("utf-8"))
        return ET.tostring(root, pretty_print=True, encoding="unicode")
    except Exception:
        return xml_text


def _print_pretty_xml(xml_text):
    """Print XML using Rich syntax highlighting if available, otherwise plain text."""
    formatted_xml = _format_xml_for_display(xml_text)
    try:
        from rich.console import Console
        from rich.syntax import Syntax

        Console().print(Syntax(formatted_xml, "xml", word_wrap=True))
        return
    except ImportError:
        pass
    print(formatted_xml)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--cfg", dest="cfgfile", help="Configuration file", required=True)
    parser.add_argument(
        "-s", "--searchstringst", dest="string", help="String to search for", required=True
    )
    parser.add_argument("-d", "--delete", action="store_true", help="Flag to delete records")
    parser.add_argument(
        "-a", "--always_commit", action="store_true", help="Flag to commit directly"
    )
    parser.add_argument(
        "--mmd",
        action="store_true",
        help="Return mmd_xml_file using Solr XML response writer and xml transformer",
    )

    args = parser.parse_args()

    if args.cfgfile is None or args.string is None:
        parser.print_help()
        parser.exit()

    return args


def build_search_request(args):
    """Build Solr search parameters for standard and raw MMD XML modes."""
    q_string = str(args.string).strip()
    if args.mmd:
        return {
            "q": q_string,
            "wt": "xml",
            "rows": 10,
            "fl": SOLR_MMD_FL,
        }
    return {
        "q": q_string,
        "wt": "json",
        "rows": 10,
        "fl": SOLR_FL,
    }


def parse_cfg(cfgfile):
    """Parse configuration file. Raises FileNotFoundError with helpful message if config file does not exist."""
    logger.info("Reading configuration: %s", cfgfile)
    try:
        with open(cfgfile) as ymlfile:
            cfgstr = yaml.full_load(ymlfile)
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Configuration file not found: {cfgfile}\n"
            f"Please check that the path is correct and the file exists."
        ) from e
    except yaml.YAMLError as e:
        raise ValueError(
            f"Failed to parse configuration file '{cfgfile}': {str(e)}\n"
            f"Please check that the file is valid YAML."
        ) from e

    return cfgstr


class IndexMMD:
    """requires a list of dictionaries representing MMD as input"""

    def __init__(self, mysolrserver, commit, authentication):
        """
        Connect to SolR core
        """
        self.solr_url = mysolrserver
        self.authentication = authentication
        try:
            self.solrc = pysolr.Solr(
                mysolrserver, always_commit=commit, timeout=1020, auth=authentication
            )
        except Exception as e:
            logger.info("Something failed in SolR init", str(e))
        logger.info("Connection established to: " + str(mysolrserver))

        try:
            pong = self.solrc.ping()
            status = json.loads(pong)["status"]
            if status == "OK":
                logger.info("Solr ping with status %s", status)
            else:
                logger.error("Error! Solr ping with status %s", status)
                sys.exit(1)

        except pysolr.SolrError as e:
            logger.error(f"Could not contact solr server: {e}")
            sys.exit(1)

    def delete_item(self, datasetid, commit):
        """Require ID as input"""
        """ Rewrite to take full metadata record as input """
        logger.info("Deleting ", datasetid, " from Level 1")
        try:
            self.solrc.delete(id=datasetid)
        except Exception as e:
            logger.info("Something failed in SolR delete %s", str(e))

        logger.info("Record successfully deleted from core")

    def search(self, myargs):
        """Require Id as input"""
        results = None
        q_string = str(myargs.string).strip()
        if not q_string:
            logger.error("Search string cannot be empty")
            return None

        try:
            logger.info("Searching with q=%s", q_string)
            params = build_search_request(myargs)
            if myargs.mmd:
                results = requests.get(
                    self.solr_url + "/select",
                    params=params,
                    auth=self.authentication,
                    timeout=1020,
                )
                results.raise_for_status()
            else:
                results = self.solrc.search(
                    q_string, **{k: v for k, v in params.items() if k != "q"}
                )
        except Exception as e:
            logger.info("Something failed: %s", str(e))

        return results


def main() -> int:

    #  Parse command line arguments
    try:
        args = parse_arguments()
    except Exception as e:
        logger.error("Something failed in parsing arguments: %s", str(e))
        return 1

    #  Parse configuration file
    try:
        cfg = parse_cfg(args.cfgfile)
    except (FileNotFoundError, ValueError) as e:
        logger.error("%s", str(e))
        return 1

    SolrServer = cfg["solrserver"]
    myCore = cfg["solrcore"]

    mySolRc = SolrServer + myCore
    # Enable basic authentication if configured.
    if "auth-basic-username" in cfg and "auth-basic-password" in cfg:
        username = cfg["auth-basic-username"]
        password = cfg["auth-basic-password"]
        logger.info("Setting up basic authentication from config")
        if username == "" or password == "":
            raise Exception(
                "Authentication username and/or password are configured,but have blank strings"
            )
        else:
            logger.info("Got username and password. Creating HTTPBasicAuth object")
            authentication = HTTPBasicAuth(username, password)
    elif "dotenv_path" in cfg:
        dotenv_path = cfg["dotenv_path"]
        if not os.path.exists(dotenv_path):
            raise FileNotFoundError(f"The file {dotenv_path} does not exist.")
        logger.info("Setting up basic authentication from dotenv_path")
        try:
            load_dotenv(dotenv_path)
        except Exception as e:
            raise Exception(f"Failed to load dotenv {dotenv_path}, Reason {e}")
        username = os.getenv("SOLR_USERNAME", default="")
        password = os.getenv("SOLR_PASSWORD", default="")
        if username == "" or password == "":
            raise Exception(
                "Authentication username and/or password are configured,but have blank strings"
            )
        else:
            logger.info("Got username and password. Creating HTTPBasicAuth object")
            authentication = HTTPBasicAuth(username, password)
    else:
        logger.info("Setting up basic authentication from dotenv")
        try:
            load_dotenv()
        except Exception as e:
            raise Exception(f"Failed to load dotenv {dotenv_path}, Reason {e}")
        username = os.getenv("SOLR_USERNAME", default="")
        password = os.getenv("SOLR_PASSWORD", default="")
        if username == "" and password == "":
            authentication = None
            logger.info("Authentication disabled")
        else:
            logger.info("Got username and password. Creating HTTPBasicAuth object")
            authentication = HTTPBasicAuth(username, password)

    # Search for records
    mysolr = IndexMMD(mySolRc, args.always_commit, authentication)

    if args.mmd and args.delete:
        logger.error(
            "--delete cannot be used together with --mmd because XML mode returns only mmd_xml_file"
        )
        return 1

    myresults = mysolr.search(args)
    if myresults is not None:
        if args.mmd:
            _print_pretty_xml(myresults.text)
            return 0

        logger.info("Found %d matches", myresults.hits)
        logger.info("Looping through matches:")
        i = 0
        docs = []
        for doc in myresults:
            logger.info("%d : %s", i, doc["id"])
            docs.append(doc)
            deleteid = doc["id"]
            if args.delete:
                mysolr.delete_item(deleteid, commit=None)
            i += 1
        _print_pretty_docs(docs)
        logger.info("Found %d matches", myresults.hits)
    else:
        logger.info("Search contained no results")

    return 0


def _main() -> None:  # pragma: no cover
    try:
        main()  # entry point in setup.cfg
    except ValueError as e:
        logger.error("%s", str(e))
    except AttributeError as e:
        logger.error("%s", str(e))
    except FileNotFoundError as e:
        logger.error("%s", str(e))


if __name__ == "__main__":  # pragma: no cover
    main()
