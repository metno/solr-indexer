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

import os
import argparse
import pysolr
import yaml
import sys
import json
import logging
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)
if os.getenv("SOLRINDEXER_LOGLEVEL", "INFO") == "DEBUG":
    logger.setLevel(logging.DEBUG)
    logger.debug("Loglevel was set to DEBUG")


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--cfg", dest="cfgfile",
                        help="Configuration file", required=True)
    parser.add_argument("-s", "--searchstringst", dest="string",
                        help="String to search for", required=True)
    parser.add_argument('-d', '--delete', action='store_true', help="Flag to delete records")
    parser.add_argument('-a', '--always_commit', action='store_true',
                        help="Flag to commit directly")

    args = parser.parse_args()

    if args.cfgfile is None or args.string is None:
        parser.logger.info_help()
        parser.exit()

    return args


def parse_cfg(cfgfile):
    # Read config file
    logger.info("Reading configuration: %s", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfgstr = yaml.full_load(ymlfile)

    return cfgstr


class IndexMMD:
    """ requires a list of dictionaries representing MMD as input """

    def __init__(self, mysolrserver, commit, authentication):
        """
        Connect to SolR core
        """
        try:
            self.solrc = pysolr.Solr(mysolrserver, always_commit=commit, timeout=1020,
                                     auth=authentication)
        except Exception as e:
            logger.info("Something failed in SolR init", str(e))
        logger.info("Connection established to: " + str(mysolrserver))

        try:
            pong = self.solrc.ping()
            status = json.loads(pong)['status']
            if status == 'OK':
                logger.info('Solr ping with status %s', status)
            else:
                logger.error('Error! Solr ping with status %s', status)
                sys.exit(1)

        except pysolr.SolrError as e:
            logger.error(f"Could not contact solr server: {e}")
            sys.exit(1)

    def delete_item(self, datasetid, commit):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        logger.info("Deleting ", datasetid, " from Level 1")
        try:
            self.solrc.delete(id=datasetid)
        except Exception as e:
            logger.info("Something failed in SolR delete %s", str(e))

        logger.info("Record successfully deleted from core")

    def search(self, myargs):
        """ Require Id as input """
        results = None
        qString, sep, resQ = str(myargs.string).partition(':')
        logger.debug("Input Query string: %s", qString)
        if sep == '':
            qString = 'full_text:' + qString
            logger.debug("separator: %s", sep)
        elif sep == ':':
            qString = qString + sep + resQ
        else:
            logger.error("%s is not a valid search string", myargs.string)
        logger.debug("Result query string: %s", resQ)
        # args = ast.literal_eval()
        try:
            logger.info("Searching with q=%s", qString)
            results = self.solrc.search(qString, **{'wt': 'python', 'rows': 10})
        except Exception as e:
            logger.info("Something failed: %s", str(e))

        return results


def main():

    #  Parse command line arguments
    try:
        args = parse_arguments()
    except Exception as e:
        logger.error("Something failed in parsing arguments: %s", str(e))
        return 1

    #  Parse configuration file
    cfg = parse_cfg(args.cfgfile)

    SolrServer = cfg['solrserver']
    myCore = cfg['solrcore']

    mySolRc = SolrServer+myCore
    # Enable basic authentication if configured.
    if 'auth-basic-username' in cfg and 'auth-basic-password' in cfg:
        username = cfg['auth-basic-username']
        password = cfg['auth-basic-password']
        logger.info("Setting up basic authentication from config")
        if username == '' or password == '':
            raise Exception('Authentication username and/or password are configured,'
                            'but have blank strings')
        else:
            logger.info("Got username and password. Creating HTTPBasicAuth object")
            authentication = HTTPBasicAuth(username, password)
    elif 'dotenv_path' in cfg:
        dotenv_path = cfg['dotenv_path']
        if not os.path.exists(dotenv_path):
            raise FileNotFoundError(f"The file {dotenv_path} does not exist.")
        logger.info("Setting up basic authentication from dotenv_path")
        try:
            load_dotenv(dotenv_path)
        except Exception as e:
            raise Exception(f"Failed to load dotenv {dotenv_path}, Reason {e}")
        username = os.getenv('SOLR_USERNAME', default='')
        password = os.getenv('SOLR_PASSWORD', default='')
        if username == '' or password == '':
            raise Exception('Authentication username and/or password are configured,'
                            'but have blank strings')
        else:
            logger.info("Got username and password. Creating HTTPBasicAuth object")
            authentication = HTTPBasicAuth(username, password)
    else:
        logger.info("Setting up basic authentication from dotenv")
        try:
            load_dotenv()
        except Exception as e:
            raise Exception(f"Failed to load dotenv {dotenv_path}, Reason {e}")
        username = os.getenv('SOLR_USERNAME', default='')
        password = os.getenv('SOLR_PASSWORD', default='')
        if username == '' and password == '':
            authentication = None
            logger.info("Authentication disabled")
        else:
            logger.info("Got username and password. Creating HTTPBasicAuth object")
            authentication = HTTPBasicAuth(username, password)

    # Search for records
    mysolr = IndexMMD(mySolRc, args.always_commit, authentication)
    myresults = mysolr.search(args)
    if myresults is not None:
        logger.info('Found %d matches', myresults.hits)
        logger.info('Looping through matches:')
        i = 0
        for doc in myresults:
            logger.info('%d : %s', i, doc['id'])
            deleteid = doc['id']
            if args.delete:
                mysolr.delete_item(deleteid, commit=None)
            i += 1
        logger.info('Found %d matches',  myresults.hits)
    else:
        logger.info("Search contained no results")

    return


def _main():  # pragma: no cover
    try:
        main()  # entry point in setup.cfg
    except ValueError as e:
        logger.info(e)
    except AttributeError as e:
        logger.info(e)


if __name__ == "__main__":  # pragma: no cover
    main()
