#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
SOLR-indexer : Main script
==========================

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
import sys
import logging
import argparse
import cartopy.crs as ccrs
from datetime import datetime

from requests.auth import HTTPBasicAuth
from solrindexer.indexdata import MMD4SolR, IndexMMD
from solrindexer.tools import to_solr_id
from solrindexer.searchindex import parse_cfg

from solrindexer.thumb.thumbnail import WMSThumbNail

logger = logging.getLogger(__name__)
if os.getenv("SOLRINDEXER_LOGLEVEL", "INFO") == "DEBUG":
    logger.setLevel(logging.DEBUG)
    logger.debug("Loglevel was set to DEBUG")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--always_commit', action='store_true',
                        help='Specification of whether always commit or not to SolR')
    parser.add_argument('-c', '--cfg', dest='cfgfile', required=True,
                        help='Configuration file')
    parser.add_argument('-i', '--input_file',
                        help='Individual file to be ingested.')
    parser.add_argument('-l', '--list_file',
                        help='File with datasets to be ingested specified.')
    parser.add_argument('-d', '--directory',
                        help='Directory to ingest')
    parser.add_argument('-parent', '--mark_parent', required=False,
                        help="Enter metadata id of existing solr document to mark as parent")
    parser.add_argument('-t', '--thumbnail', action='store_true',
                        help='Create and index thumbnail, do not update the main content.')
    parser.add_argument('-n', '--no_thumbnail', action='store_true',
                        help='Do not index thumbnails (done automatically if WMS available).')

    # Thumbnail parameters
    parser.add_argument('-m', '--map_projection', required=False,
                        help='Specify map projection for thumbnail '
                        '(e.g. Mercator, PlateCarree, PolarStereographic).')
    parser.add_argument('-t_layer', '--thumbnail_layer', required=False,
                        help='Specify wms_layer for thumbnail.')
    parser.add_argument('-t_style', '--thumbnail_style', required=False,
                        help='Specify the style (colorscheme) for the thumbnail.')
    parser.add_argument('-t_zl', '--thumbnail_zoom_level', type=float, required=False,
                        help='Specify the zoom level for the thumbnail.')
    parser.add_argument('-ac', '--add_coastlines', const=True, nargs='?', required=False,
                        help='Add coastlines too the thumbnail (True/False). Default True')
    parser.add_argument('-t_extent', '--thumbnail_extent', required=False, nargs='+',
                        help='Spatial extent of thumbnail in lat/lon degrees like "x0 x1 y0 y1"')

    args = parser.parse_args()

    if args.cfgfile is None:
        parser.print_help()
        parser.exit()
    if not args.input_file and not args.directory and not args.list_file and not args.mark_parent:
        parser.print_help()
        parser.exit()

    return args


def main():
    logger.debug("-- DEBUG LogLevel --")
    # Parse command line arguments
    try:
        args = parse_arguments()
    except Exception as e:
        logger.error("Something failed in parsing arguments: %s", str(e))
        return 1

    # Parse configuration file
    cfg = parse_cfg(args.cfgfile)

    # CONFIG START
    # Read config file, can be done as a CONFIG class, such that argparser can overwrite duplicates
    # with open(args.cfgfile, 'r') as ymlfile:
    #   cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    # Specify map projection
    mapprojection = ccrs.PlateCarree()  # Fallback
    if args.map_projection:
        map_projection = args.map_projection
    else:
        map_projection = cfg['wms-thumbnail-projection']
    if map_projection == 'Mercator':
        mapprojection = ccrs.Mercator()
    elif map_projection == 'PlateCarree':
        mapprojection = ccrs.PlateCarree()
    elif map_projection == 'PolarStereographic':
        mapprojection = ccrs.Stereographic(central_longitude=0.0, central_latitude=90.,
                                           true_scale_latitude=60.)
    else:
        raise Exception('Map projection is not properly specified in config')

    # Enable basic authentication if configured.
    if 'auth-basic-username' in cfg and 'auth-basic-password' in cfg:
        username = cfg['auth-basic-username']
        password = cfg['auth-basic-password']
        logger.info("Setting up basic authentication")
        if username == '' or password == '':
            raise Exception('Authentication username and/or password are configured,'
                            'but have blank strings')
        else:
            authentication = HTTPBasicAuth(username, password)
    else:
        authentication = None
        logger.info("Authentication disabled")
    # Get solr server config
    SolrServer = cfg['solrserver']
    myCore = cfg['solrcore']

    # Set up connection to SolR server
    mySolRc = SolrServer+myCore
    logger.info("Connecting to solr %s",  mySolRc)
    mysolr = IndexMMD(mySolRc, args.always_commit, authentication)

    end_solr_commit = False
    if 'end-solr-commit' in cfg:
        end_solr_commit = cfg['end-solr-commit']

    # CONFIG DONE

    # HANDLE ARGUMENTS
    if args.mark_parent:
        meta_id = str(args.mark_parent).strip()
        logger.debug("Got mark parent argument with meta id: %s", meta_id)
        status, msg = mysolr.update_parent(to_solr_id(meta_id))
        sys.exit()

    # Find files to process
    if args.input_file:
        myfiles = [args.input_file]
    elif args.list_file:
        try:
            f2 = open(args.list_file, "r")
        except IOError as e:
            logger.error('Could not open file: %s %e', args.list_file, e)
            return
        myfiles = f2.readlines()
        f2.close()
    elif args.directory:
        try:
            myfiles_ = os.listdir(args.directory)
            myfiles = [file for file in myfiles_ if os.path.splitext(file)[1] == '.xml']

        except Exception as e:
            logger.error(
                "Something went wrong in decoding cmd arguments: %s", e)
            return 1

    """Handeling thumbnail command line arguments"""
    # FIXME, need a better way of handling this, WMS layers should be interpreted
    # automatically, this way we need to know up front whether WMS makes sense or not and
    # that won't work for harvesting
    if args.thumbnail_layer:
        wms_layer = args.thumbnail_layer
    else:
        wms_layer = None
    if args.thumbnail_style:
        wms_style = args.thumbnail_style
    else:
        wms_style = None
    if args.thumbnail_zoom_level:
        wms_zoom_level = args.thumbnail_zoom_level
    else:
        wms_zoom_level = 0
    if args.add_coastlines:
        wms_coastlines = args.add_coastlines
    else:
        wms_coastlines = True
    if args.thumbnail_extent:
        thumbnail_extent = [int(i)
                            for i in args.thumbnail_extent[0].split(' ')]
    else:
        thumbnail_extent = None

    """Creating thumbnail generator class for use"""
    if not args.no_thumbnail:
        tflg = True
    else:
        tflg = False
    if tflg:
        thumbClass = WMSThumbNail(projection=mapprojection,
                                  wms_layer=wms_layer, wms_style=wms_style,
                                  wms_zoom_level=wms_zoom_level, add_coastlines=wms_coastlines,
                                  wms_timeout=cfg['wms-timeout'], thumbnail_extent=thumbnail_extent
                                  )
    else:
        thumbClass = None
    # EndCreatingThumbnail

    """Log when we start the processing"""
    now = datetime.now()
    logger.info("Starting processing at: %s", now.strftime("%Y-%m-%d %H:%M:%S"))

    fileno = 1
    files2ingest = []
    parentids = set()
    logger.info("Got %d input files.", len(myfiles))
    # logger.debug(myfiles)
    for myfile in myfiles:
        myfile = myfile.strip()
        # Decide files to operate on
        if not myfile.endswith('.xml'):
            continue
        if args.list_file:
            myfile = myfile.rstrip()
        if args.directory:
            myfile = os.path.join(args.directory, myfile)

        # Index files
        logger.info('-- Processing file: %d - %s', fileno, myfile)

        try:
            mydoc = MMD4SolR(filename=myfile)
        except Exception as e:
            logger.warning('Could not handle file: %s. Error: %s', myfile, e)
            continue
        logger.info('Checking MMD elements.')

        try:
            mydoc.check_mmd()
        except Exception as e:
            logger.error(
                'File: %s is not compliant with MMD specification. Error: %s', myfile, e)
            continue
        fileno += 1

        """
        Convert to the SolR format needed
        """
        logger.info('Converting to SolR format.')
        try:
            newdoc = mydoc.tosolr()
        except Exception as e:
            logger.error(
                'Could not convert file %s to solr document.  Reason: %s', myfile, e)
            continue

        """
        Checking datasets to see if they are children.
        Datasets that are not children are all set to parents.
        Make some corrections based on experience for harvested records...
        """
        if 'related_dataset' in newdoc:
            logger.info('Parsing parent/child relations.')
            logger.info("Got child dataset id %s.", newdoc['id'])
            # Special fix for NPI
            newdoc['related_dataset'] = newdoc['related_dataset'].replace(
                'https://data.npolar.no/dataset/', '')
            newdoc['related_dataset'] = newdoc['related_dataset'].replace(
                'http://data.npolar.no/dataset/', '')
            newdoc['related_dataset'] = newdoc['related_dataset'].replace(
                'http://api.npolar.no/dataset/', '')
            newdoc['related_dataset'] = newdoc['related_dataset'].replace(
                '.xml', '')
            # Skip if DOI is used to refer to parent, that isn't consistent.
            if 'doi.org' in newdoc['related_dataset']:
                continue
            # Create solr id from identifier
            myparentid = newdoc['related_dataset']
            parentid_solr = to_solr_id(myparentid)
            # If related_dataset is present,
            # set this dataset as a child using isChild and dataset_type
            logger.debug("Marking as child.")
            newdoc.update({"isChild": True})
            newdoc.update({"dataset_type": "Level-2"})
            parentids.add(parentid_solr)
        else:
            newdoc.update({"dataset_type": "Level-1"})

        # Update list of files to process
        files2ingest.append(newdoc)

    # Check if parents are in the existing list
    pending = parentids.copy()
    for id in parentids:
        if not any(d['id'] == id for d in files2ingest):
            # Check if already ingested and update if so
            logger.info("Checking index for parent %s", id)
            status, msg = mysolr.update_parent(id, fail_on_missing=False,
                                               handle_missing_status=False)

            if status is True:
                logger.info(msg)
                pending.remove(id)
            else:
                logger.error(msg)

        else:
            # Assuming found in the current batch of files, then set to parent...
            # Not sure if this is needed onwards, but discussion on how isParent works is needed
            # Øystein Godøy, METNO/FOU, 2023-03-31
            i = 0
            logger.info("Update parents in batch.")
            for rec in files2ingest:
                if rec['id'] == id:
                    if 'isParent' in rec:
                        if rec['isParent'] is True:
                            logger.info("Parent %s already updated.", id)
                        else:
                            files2ingest[i].update({'isParent': True})
                            files2ingest[i].update({'dataset_type': 'Level-1'})
                            logger.info("Parent %s updated." % id)
                            pending.remove(id)
                i += 1

    if len(files2ingest) == 0:
        logger.warn('No files to ingest.')
        return 1

    # Do the ingestion FIXME
    # Check if thumbnail specification need to be changed
    logger.info("Indexing datasets")
    """
    Split list into sublists before indexing (and retrieving WMS thumbnails etc)
    """
    mystep = 2500
    myrecs = 0
    for i in range(0, len(files2ingest), mystep):
        mylist = files2ingest[i:i+mystep]
        myrecs += len(mylist)
        try:
            mysolr.index_record(mylist, addThumbnail=tflg, thumbClass=thumbClass)
        except Exception as e:
            logger.warning('Something failed during indexing:s %s', e)
        logger.info('%d records out of %d have been ingested...',
                    myrecs, len(files2ingest))
        del mylist

    if myrecs != len(files2ingest):
        logger.warning('Inconsistent number of records processed.')
    # Report status
    logger.info("Number of files processed were: %d", len(myfiles))

    if len(myfiles) - len(files2ingest) > 0:
        logger.warning("One or more files could not be processed. Check the logs.")

    # Check for missing parents in batch or index
    if len(pending) > 0:
        logger.warning("Missing parents in input and/or index")
        logger.info(pending)

    if end_solr_commit is True:
        # Add a commit to solr at end of run
        logger.info("Committing the input to SolR. This may take some time.")
        mysolr.commit()


def _main():  # pragma: no cover
    try:
        main()  # entry point in setup.cfg
    except ValueError as e:
        print(e)
    except AttributeError as e:
        print(e)


if __name__ == "__main__":  # pragma: no cover
    main()
