#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
SOLR-indexer : Bulkindexer - main script
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
import time
import pysolr
import logging
import argparse
from dotenv import load_dotenv
import cartopy.crs as ccrs

from datetime import datetime
from requests.auth import HTTPBasicAuth
from solrindexer.tools import find_xml_files, flatten, initThumb, initSolr
from solrindexer.tools import solr_commit, solr_add, get_dataset, solr_ping
from solrindexer.script.searchindex import parse_cfg
from solrindexer.bulkindexer import BulkIndexer
from solrindexer.indexdata import IndexMMD

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
from concurrent import futures as Futures

from solrindexer.thumb.thumbnail import WMSThumbNail

logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--always_commit', action='store_true',
                        help='Specification of whether always commit or not to SolR')
    parser.add_argument('-c', '--cfg', dest='cfgfile', required=True,
                        help='Configuration file')
    parser.add_argument('-l', '--list_file',
                        help='File with datasets to be ingested specified.')
    parser.add_argument('-d', '--directory',
                        help='Directory to ingest recursivly')
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
    if not args.directory and not args.list_file:
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
    if args.map_projection:
        map_projection = args.map_projection
    else:
        map_projection = cfg.get('wms-thumbnail-projection', None)
    # Specify map projection
    thumb_impl = cfg.get('thumbnail_impl', None)
    if thumb_impl is None or thumb_impl == 'legacy':
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
        logger.debug("Using legacy thumbnail implementation")
    if thumb_impl == 'fastapi':
        mapprojection = 'PlateCarree'
        if type(map_projection) is str:
            mapprojection = map_projection
        logger.debug("Using new thumbnail implementation with projection: %s", map_projection)

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

    # Get solr server config
    SolrServer = cfg['solrserver']
    myCore = cfg['solrcore']

    # Set up connection to SolR server
    mySolRc = SolrServer+myCore

    # Bulkindexer Defaults
    chunksize = 2500
    threads = 20
    workers = 10

    # Initialize Solr
    logger.info("Connecting to solr: %s", mySolRc)
    initSolr(mySolRc,
             pysolr.Solr(mySolRc, always_commit=False, timeout=1020, auth=authentication),
             authentication)

    solr_ping()

    # Bulkinder defaults override from config
    if 'batch-size' in cfg:
        chunksize = cfg["batch-size"]

    if 'workers' in cfg:
        workers = cfg["workers"]

    if 'threads' in cfg:
        threads = cfg["threads"]

    # Should we commit to solr at the end of execution?
    end_solr_commit = False
    if 'end-solr-commit' in cfg:
        if cfg['end-solr-commit'] is True:
            end_solr_commit = cfg['end-solr-commit']
    # CONFIG DONE

    # Find files to process
    if args.list_file:
        try:
            f2 = open(args.list_file, "r")
        except IOError as e:
            logger.error('Could not open file: %s %e', args.list_file, e)
            return
        myfiles = f2.readlines()
        f2.close()
    elif args.directory:
        if not os.path.exists(args.directory):
            logger.error("Directory not found %s", args.directory)
            sys.exit(1)
        try:
            myfiles = find_xml_files(args.directory)
            if myfiles is not None:
                logger.info("Processing directory: %s",
                            args.directory)
        except Exception as e:
            logger.error(
                "Something went wrong in decoding cmd arguments: %s", e)
            sys.exit(1)
    else:
        logger.error("No valid inputlist or input directory given")
        sys.exit(1)

    if myfiles is None:
        logger.error('No files to process. exiting')
        sys.exit(1)

    """ Do some extra input list validation"""
    if len(myfiles) == 0 or myfiles is None:
        logger.error('No files to process. exiting')
        sys.exit(1)

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

    # Get new thumbnail config from etc config
    thumbnail_api_host = cfg.get('thumbnail_api_host', None)
    thumbnail_api_endpoint = cfg.get('thumbnail_api_endpoint', None)

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
        initThumb(thumbClass)
    else:
        thumbClass = None

    # Create a dict instead of object, if we use the new api, so the code does
    # not need to import cartopy/matplotlib etc.
    if thumb_impl == 'fastapi' and tflg:
        del thumbClass
        thumbClass = {"host":  thumbnail_api_host,
                      "endpoint": thumbnail_api_endpoint,
                      "wms_layer": wms_layer,
                      "wms_style": wms_style,
                      "wms_zoom_level": wms_zoom_level,
                      "wms_timeout": cfg.get('wms-timeout', 120),
                      "add_coastlines": wms_coastlines,
                      "projection": mapprojection,
                      "thumbnail_extent": thumbnail_extent}
        initThumb(thumbClass)

    logger.debug("Create Thumbnails?  %s", tflg)
    logger.debug("Thumb class is %s", thumbClass)
    logger.debug("Thumbnail projection %s", mapprojection)
    # EndCreatingThumbnail

    """ Start timer"""
    st = time.perf_counter()
    pst = time.process_time()

    """Log when we start the processing"""
    now = datetime.now()
    logger.info("Starting processing at: %s", now.strftime("%Y-%m-%d %H:%M:%S"))

    """ Create an instance of the BulkIndexer"""
    logger.info("Creating bulkindexer with chunks %d and threads/processes %d.",
                chunksize, threads)
    bulkindexer = BulkIndexer(myfiles, mySolRc, threads=threads,
                              chunksize=chunksize, auth=authentication,
                              tflg=tflg, thumbClass=thumbClass)
    """
    Indexing start. The inputlist is split into as many lists as input workers.
    Each worker will process the lists and return back the information needed to track the
    progress and parent ids
    """
    # Define some lists to keep track of the processing
    parent_ids_pending = set()  # Keep track of pending parent ids
    parent_ids_processed = set()  # Keep track parent ids already processed
    parent_ids_found = set()    # Keep track of parent ids found
    doc_ids_processed = set()    # Keep track of all doc ids processed
    processed = 0
    docs_failed = 0
    docs_indexed = 0

    # Start the indexing
    logger.info("Got %d input files.", len(myfiles))

    # We run only one worker if input files are less than 1000
    if len(myfiles) <= 500:
        workers = 1
        chunksize = 500

    logger.info(
        "Indexing with batch size %d and %d worker processes with %d threads",
        chunksize, workers, threads)

    # We only do multiprocessing if workers is 2 or more
    if workers > 1:
        workerlistsize = round(len(myfiles)/workers)
        logger.debug("Using multiple processes.")
        # Split the inputfiles into lists for each worker.
        workerFileLists = [
            myfiles[i: i + workerlistsize] for i in range(0, len(myfiles), workerlistsize)]
        logger.debug("Input list: %s" % len(flatten(workerFileLists)))
        futures_list = list()
        job = 1
        with ProcessPoolExecutor(max_workers=workers) as executor:
            for fileList in workerFileLists:
                logger.info("Submitting worker job %d - with %d files", job, len(fileList))
                bulkidx = BulkIndexer(fileList, mySolRc, threads=threads,
                                      chunksize=chunksize, auth=authentication,
                                      tflg=tflg, thumbClass=thumbClass)
                future = executor.submit(bulkidx.bulkindex, fileList)

                futures_list.append(future)
                job = job+1
                time.sleep(2)

            for f in as_completed(futures_list):
                if f.exception():
                    logger.error("Process failed with: %s", f.exception())
                elif f.done():
                    (parent_ids_found_,
                        parent_ids_pending_,
                        parent_ids_processed_,
                        doc_ids_processed_,
                        docs_failed_,
                        docs_indexed_,
                        files_processed_) = f.result()
                    # logger.debug(f.result())
                    parent_ids_found.update(parent_ids_found_)
                    parent_ids_pending.update(parent_ids_pending_)
                    parent_ids_processed.update(parent_ids_processed_)
                    doc_ids_processed.update(doc_ids_processed_)
                    processed += files_processed_
                    docs_failed += docs_failed_
                    docs_indexed += docs_indexed_
                    logger.info("%s docs indexed so far." % docs_indexed)
        Futures.ALL_COMPLETED
    # Bulkindex using main process.
    else:
        logger.debug("Using ONE process.")
        (parent_ids_found_,
         parent_ids_pending_,
         parent_ids_processed_,
         doc_ids_processed_,
         docs_failed_,
         docs_indexed_,
         files_processed_) = bulkindexer.bulkindex(myfiles)

        parent_ids_found.update(parent_ids_found_)
        parent_ids_pending.update(parent_ids_pending_)
        parent_ids_processed.update(parent_ids_processed_)
        doc_ids_processed.update(doc_ids_processed_)
        processed += files_processed_
        docs_failed += docs_failed_
        docs_indexed += docs_indexed_

    Futures.ALL_COMPLETED
    # TODO: Add last parent missing index check here. after refactor this logic
    # summary of possible missing parents
    missing = list(set(parent_ids_found) - set(parent_ids_processed))
    if len(missing) > 0:
        logger.info("The last parents should be in index. Checking...")
        for pid in missing:
            myparent = None
            myparent = get_dataset(pid)

            if myparent['doc'] is not None:
                logger.debug(
                    "parent found in index: %s, isParent: %s",
                    myparent['doc']['id'], myparent['doc']['isParent'])
                # Check if already flagged
                if myparent['doc']['isParent'] is False:
                    logger.debug('Update on indexed parent %s, isParent: True' % pid)
                    mydoc = IndexMMD._solr_update_parent_doc(myparent['doc'])
                    doc_ = mydoc
                    try:
                        solr_add([doc_])
                    except Exception as e:
                        logger.error("Could not update parent on index. reason %s", e)

                    # Update lists
                    parent_ids_processed.add(pid)

                    # Remove from pending list
                    if pid in parent_ids_pending:
                        parent_ids_pending.remove(pid)
                else:
                    logger.debug("Parent %s present and marked as parent", pid)
                    # Update lists
                    parent_ids_processed.add(pid)

                    # Remove from pending list
                    if pid in parent_ids_pending:
                        try:
                            parent_ids_pending.remove(pid)
                        except KeyError:
                            pass
    # LOOP END
    missing = list(set(parent_ids_found) - set(parent_ids_processed))
    if len(missing) > 0:
        logger.warning("Make sure to index the missing parents and then index the children")

    # Update parent_ids_pending
    ppending_ = parent_ids_pending.copy()
    for pid in ppending_:
        if pid in parent_ids_processed:
            parent_ids_pending.remove(pid)
        else:
            myparent = None
            myparent = get_dataset(pid)

            if myparent['doc'] is not None:
                logger.debug(
                    "parent found in index: %s, isParent: %s",
                    myparent['doc']['id'], myparent['doc']['isParent'])
                # Check if already flagged
                if myparent['doc']['isParent'] is False:
                    logger.debug('Update on indexed parent %s, isParent: True' % pid)
                    mydoc = IndexMMD._solr_update_parent_doc(myparent['doc'])
                    doc_ = mydoc
                    try:
                        solr_add([doc_])
                    except Exception as e:
                        logger.errors("Could not update parent on index. reason %s", e)

                    # Update lists
                    parent_ids_processed.add(pid)

                    # Remove from pending list
                    if pid in parent_ids_pending:
                        try:
                            parent_ids_pending.remove(pid)
                        except KeyError:
                            pass
                else:
                    logger.debug("Parent %s present and marked as parent", pid)
                    # Update lists
                    parent_ids_processed.add(pid)

                    # Remove from pending list
                    if pid in parent_ids_pending:
                        try:
                            parent_ids_pending.remove(pid)
                        except KeyError:
                            pass

    if len(parent_ids_pending) > 0:
        logger.warning("parent ids pending not empty")
        logger.debug(parent_ids_pending)

    logger.info("====== INDEX END ===== %s files processed with %s workers and batch size %s ==",
                len(myfiles), workers, chunksize)
    logger.info("Parent ids found: %s" % len(parent_ids_found))
    logger.info("Parent ids processed: %s" % len(parent_ids_processed))
    logger.info("Parent ids pending: %s" % len(parent_ids_pending))
    logger.info("Document ids processed: %s" % len(doc_ids_processed))
    logger.info("===============================================================================")

    # summary of possible missing parents
    missing = list(set(parent_ids_found) - set(parent_ids_processed))
    if len(missing) != 0:
        logger.warning('Missing parents in input. %s' % missing)
        logger.info('Could not find the following parents: %s' % missing)
    docs_failed = len(myfiles) - docs_indexed
    if docs_failed != 0:
        logger.warning('%d documents could not be indexed. check output and logfile.', docs_failed)

    logger.info("===================================================================")
    logger.info("%s files processed and %s documents indexed. %s documents was skipped",
                processed, docs_indexed, docs_failed)
    logger.info("===================================================================")
    logger.info("Total files given as input: %d " % len(myfiles))

    """ Stop timer"""
    et = time.perf_counter()
    pet = time.process_time()
    elapsed_time = et - st
    pelt = pet - pst
    logger.info("Processed %s documents" % processed)
    logger.info("Files / documents failed: %s" % docs_failed)
    logger.info('Execution time: %s', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
    logger.info('CPU time: %s', time.strftime("%H:%M:%S", time.gmtime(pelt)))
    if end_solr_commit:
        solr_commit()


def _main():  # pragma: no cover
    try:
        main()  # entry point in setup.cfg
    except ValueError as e:
        print(e)
    except AttributeError as e:
        print(e)


if __name__ == "__main__":  # pragma: no cover
    main()
