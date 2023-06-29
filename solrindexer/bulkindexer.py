"""
SOLR-indexer : Multithreaded Bulkindexer
===========================

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
import threading

from solrindexer.indexdata import MMD4SolR
from solrindexer.indexdata import IndexMMD

from solrindexer.tools import to_solr_id, process_feature_type
from solrindexer.tools import create_wms_thumbnail, get_dataset, solr_add
from solrindexer.multithread.io import load_file
from solrindexer.multithread.threads import concurrently, multiprocess

from concurrent.futures import ThreadPoolExecutor
from concurrent import futures as Futures

logger = logging.getLogger(__name__)


class BulkIndexer(object):
    """ Do multithreaded bulkindexing given a list of file names.
    ...

    Attributes
    ----------
    inputList : list
        A list of filepaths to be ingested. Absolute or relative.
    solr_url : str
        Full SolR url to ingest to
    threads : int
        number of threads
    chunksize : int
        number of documents to process in each batch
    auth : obj
        valid authentication object for SolR
    """

    def __init__(self, inputList, solr_url, threads=20, chunksize=2500, auth=None,
                 tflg=False, thumbClass=None):
        """ Initialize BulkIndexer"""
        logger.debug("Initializing BulkIndexer.")
        self.inputList = inputList
        self.threads = threads
        self.chunksize = chunksize
        self.total_in = len(inputList)

        # self.solrcon = pysolr.Solr(solr_url, always_commit=False, timeout=1020, auth=auth)
        # self.  = IndexMMD(solr_url, False, authentication=auth)

        """Initialize thumbnail generator"""
        self.tflg = tflg
        #     self.thumb = thumbClass
        #     initThumb(thumbClass)

    def mmd2solr(self, mmd, status, file):
        """
        Convert mmd dict to solr dict

        Check for presence of children and mark them as children.
        If children found return parentid together with the solrdoc
        """

        if mmd is None:
            logger.warning("File %s was not parsed" % file)
            return (None, status)
        mydoc = MMD4SolR(filename=file, mydoc=mmd, bulkFile=file)
        try:
            mydoc.check_mmd()
        except Exception as e:
            logger.error(
                "File %s did not pass the mmd check, cannot index. Reason: %s" % (file, e))
            return (None, status)

        # Convert mmd xml dict to solr dict
        try:
            tmpdoc = mydoc.tosolr()
        except Exception as e:
            logger.error(
                "File %s could not be converted to solr document. Reason: %s" % (file, e))
            return (None, status)

        """ Do some sanity checking of the documents and skip docs with problems"""
        if tmpdoc is None:
            logger.warning("Solr document for file %s was empty" % (file))
            return (None, status)

        if 'id' not in tmpdoc:
            logger.warning("File %s have no id. Missing metadata_identifier?" % file)
            return (None, status)

        if tmpdoc['id'] is None or tmpdoc['id'] == 'Unknown':
            logger.warning(
                "Skipping process file %s. Metadata identifier: Unknown, or missing" % file)
            return (None, status)

        if 'temporal_extent_start_date' not in tmpdoc:
            logger.error("Could not find start date in  %s.", file)
            return (None, status)

        if 'related_dataset' in tmpdoc:
            logger.debug("got related dataset")
            if isinstance(tmpdoc['related_dataset'], str):
                logger.debug("processing child")
                # Manipulate the related_dataset id to solr id
                # Special fix for NPI
                tmpdoc['related_dataset'] = tmpdoc['related_dataset'].replace(
                    'https://data.npolar.no/dataset/', '')
                tmpdoc['related_dataset'] = tmpdoc['related_dataset'].replace(
                    'http://data.npolar.no/dataset/', '')
                tmpdoc['related_dataset'] = tmpdoc['related_dataset'].replace(
                    'http://api.npolar.no/dataset/', '')
                tmpdoc['related_dataset'] = tmpdoc['related_dataset'].replace(
                    '.xml', '')
                # Skip if DOI is used to refer to parent, that isn't consistent.
                if 'doi.org' not in tmpdoc['related_dataset']:
                    # Update document with child specific fields
                    tmpdoc.update({'dataset_type': 'Level-2'})
                    tmpdoc.update({'isChild': True})
                    # tmpdoc.update({'isParent': False})

                    # Fix special characters that SolR doesn't like
                    myparentid = tmpdoc['related_dataset']
                    tmpdoc.update({'related_dataset': myparentid.strip()})
                    mysolrparentid = to_solr_id(myparentid)
                    tmpdoc.update({'related_dataset_id': mysolrparentid})
                    status = mysolrparentid

        else:
            # Assume we have level-1 doc that are not parent
            tmpdoc.update({'dataset_type': 'Level-1'})
            tmpdoc.update({'isParent': False})

        return (tmpdoc, status)

    def process_mmd(self, mmd_list, status_list):
        """
        Mutithreaded processing of mmd2solr conversion
        """
        with ThreadPoolExecutor(self.threads) as exe:
            arglist = zip(mmd_list, status_list)
            # convert mmd to solr doc
            futures = [exe.submit(self.mmd2solr, item) for item in arglist]
            # collect data
            result = [future.result() for future in futures]
            solr_docs, status = zip(*result)
            return solr_docs, status

    def add2solr(self, docs, msg_callback):
        """ Add documents to SolR"""
        try:
            solr_add(docs)
        except Exception as e:
            logger.error("Some documents failed to be added to solr. reason: %s" % e)
        msg_callback("%s, PID: %s completed indexing %s documents!" % (
            threading.current_thread().name, threading.get_native_id(), len(docs)))

    def msg_callback(self, msg):
        """Message logging callback function"""
        logger.info(msg)

    def bulkindex(self, filelist):
        """Main bulkindexer function"""
        chunksize = self.chunksize
        logger.debug("Got %d input files", len(filelist))
        logger.debug("Processing with batchsize %d", chunksize)
        # Define some lists to keep track of the processing
        parent_ids_pending = set()  # Keep track of pending parent ids
        parent_ids_processed = set()  # Keep track parent ids already processed
        parent_ids_found = set()    # Keep track of parent ids found

        # keep track of batch process
        indexthreads = list()
        files_processed = 0
        docs_indexed = 0
        docs_skipped = 0
        it = 1
        doc_ids_processed = set()
        # print("######### BATCH START ###########################")
        batch_run = 1
        for i in range(0, len(filelist), chunksize):
            logger.info("---- Batch run %d ----", batch_run)
            # select a chunk
            files = filelist[i:(i + chunksize)]
            docs = list()
            statuses = list()

            """######################## STARTING THREADS ########################
            # Load each file using multiple threads, and process documents as files are loaded
            ###################################################################
            """
            logger.info("---- Reading files concurrently ----")
            for (file, mmd) in concurrently(fn=load_file, inputs=files,
                                            max_concurrency=self.threads):

                # Get the processed document and its status
                doc, status = self.mmd2solr(mmd, None, file)

                # Add the document and the status to the document-list
                docs.append(doc)
                statuses.append(status)
            """################################## THREADS FINISHED ##################"""
            Futures.ALL_COMPLETED
            # Check if we got some children in the batch pointing to a parent id
            parentids = set(
                [element for element in statuses if element is not None])
            logger.debug(parentids)

            # Check if the parent(s) of the children(s) was found before.
            # If not, we add them to found.
            for pid in parentids:
                if pid not in parent_ids_found:
                    parent_ids_found.add(pid)
                if pid not in parent_ids_pending and pid not in parent_ids_processed:
                    parent_ids_pending.add(pid)

            # Check if the parent(s) of the children(s) we found was processed.
            # If so, we do not process agian
            for pid in parent_ids_processed:
                if pid in parentids:
                    parentids.remove(pid)
            for pid in parent_ids_found:
                if pid in parentids:
                    parentids.remove(pid)

            # Files processed so far
            files_processed += len(files)

            # Gnereate a list of documents to send to solr.
            # Documents that could not be opened, parsed or converted to solr documents are skipped
            docs_ = len(docs)  # Number of documents processed
            # List of documents that can be indexed
            docs = [el for el in docs if el is not None]
            # Update # of skipped documents
            docs_skipped += (docs_ - len(docs))

            # keep track of all document ids we have indexed, so we do not have to check solr
            # for a parent more than we need
            docids_ = set([doc['id'] for doc in docs])
            doc_ids_processed.update(docids_)

            # Process feature types here, using the concurrently function,
            dap_docs = [
                doc for doc in docs if 'data_access_url_opendap' in doc]
            """######################## STARTING THREADS ########################
            # Load each file using multiple threads, and process documents as files are loaded
            ###################################################################"""
            logger.info("---- Process featureType concurrently ----")
            for (doc, newdoc) in concurrently(fn=process_feature_type,
                                              inputs=dap_docs,
                                              max_concurrency=self.threads):
                docs.remove(doc)
                docs.append(newdoc)
            """################################## THREADS FINISHED ##################"""
            Futures.ALL_COMPLETED
            """TODO: Add wms thumbnail batch creation here."""
            if self.tflg is True:
                thumb_docs = [
                    doc for doc in docs if 'data_access_url_ogc_wms' in doc]
                """######################## STARTING THREADS ########################
                # Load each file using multiple threads, and process documents as files are loaded
                ###################################################################"""
                logger.debug("---- Creating thumbnails concurrently ----")
                for (doc, newdoc) in multiprocess(fn=create_wms_thumbnail,
                                                  inputs=thumb_docs,
                                                  max_concurrency=self.threads):
                    docs.remove(doc)
                    docs.append(newdoc)
                """################################## THREADS FINISHED ##################"""
            Futures.ALL_COMPLETED
            # Run over the list of parentids found in this chunk, and look for the parent
            parent_found = False
            for pid in parentids:
                logger.debug("checking parent: %s" % pid)
                # Firs we check if the parent dataset are in our jobs
                myparent = None
                parent = [el for el in docs if el['id'] == pid]
                logger.debug("parents found in this chunk: %s" % parent)

                # Check if we have the parent in this chunk
                if len(parent) > 0:
                    myparent = parent.pop()
                    myparent_ = myparent
                    logger.debug("parent found in current chunk: %s " % myparent['id'])
                    parent_found = True
                    if myparent['isParent'] is False:
                        logger.debug('found pending parent %s in this job.' % pid)
                        logger.debug('updating parent')

                        docs.remove(myparent)  # Remove original
                        myparent_.update({'isParent': True})
                        docs.append(myparent_)

                        # Remove from pending list
                        if pid in parent_ids_pending:
                            parent_ids_pending.remove(pid)

                        # add to processed list for reference
                        parent_ids_processed.add(pid)

                # Check if the parent is already in the index, and flag
                # it as parent if not done already
                if pid in doc_ids_processed and not parent_found:
                    myparent = get_dataset(pid)

                    if myparent is not None:
                        # if not found in the index, we store it for later
                        if myparent['doc'] is None:
                            if pid not in parent_ids_pending:
                                logger.debug(
                                    'parent %s not found in index. storing it for later' % pid)
                                parent_ids_pending.add(pid)

                        # If found in index we update the parent
                        else:
                            if myparent['doc'] is not None:
                                logger.debug(
                                    "parent found in index: %s, isParent: %s",
                                    (myparent['doc']['id'], myparent['doc']['isParent']))
                                # Check if already flagged
                                if myparent['doc']['isParent'] is False:
                                    logger.debug(
                                        'Update on indexed parent %s, isParent: True', pid)
                                    mydoc = IndexMMD._solr_update_parent_doc(myparent['doc'])
                                    # print(mydoc)
                                    doc_ = mydoc
                                    try:
                                        solr_add([doc_])
                                    except Exception as e:
                                        logger.error(
                                            "Could update parent on index. reason %s", e)

                                    # Update lists
                                    parent_ids_processed.add(pid)

                                    # Remove from pending list
                                    if pid in parent_ids_pending:
                                        parent_ids_pending.remove(pid)

            # Last we check if parents pending previous chunks is in this chunk
            ppending = set(parent_ids_pending)
            if len(ppending) > 0:
                logger.debug(" == Checking Pending == ")
                for pid in ppending:
                    # Firs we check if the parent dataset are in our jobs
                    myparent = None
                    parent = [el for el in docs if el['id'] == pid]

                    if len(parent) > 0:
                        myparent = parent.pop()
                        myparent_ = myparent
                        logger.debug("pending parent found in current chunk: %s ", myparent['id'])
                        parent_found = True
                        if myparent['isParent'] is False:
                            logger.debug('found unprocessed pending parent %s in this job.' % pid)
                            logger.debug('updating parent')

                            docs.remove(myparent)  # Remove original
                            myparent_.update({'isParent': True})
                            docs.append(myparent_)

                            # Remove from pending list
                            if pid in parent_ids_pending:
                                parent_ids_pending.remove(pid)

                            # add to processed list for reference
                            parent_ids_processed.add(pid)

                    # If the parent was proccesd, asume it was indexed before flagged
                    if pid in doc_ids_processed and not parent_found:
                        myparent = get_dataset(pid)

                        # If we did not find the parent in this job, check the index
                        if myparent['doc'] is not None:
                            logger.debug("pending parent found in index: %s, isParent: %s",
                                         (myparent['doc']['id'], myparent['doc']['isParent']))

                            if myparent['doc']['isParent'] is False:
                                logger.debug('Update on indexed parent %s, isParent: True' % pid)
                                mydoc_ = IndexMMD._solr_update_parent_doc(myparent['doc'])
                                mydoc = mydoc_
                                # doc = {'id': pid, 'isParent': True}
                                try:
                                    solr_add([mydoc])
                                except Exception as e:
                                    logger.error(
                                        "Could not update parent on index. reason %s", e)

                                # Update lists
                                parent_ids_processed.add(pid)

                                # Remove from pending list
                                if pid in parent_ids_pending:
                                    parent_ids_pending.remove(pid)

            # TODO: Add posibility to not index datasets that are already in the index
                # 1. Generate a list of doc ids from the docs to be indexed.
                # 2. Search in solr for the ids
                # 3. If the document was indexed
                    # remove document from docs to be indexed

            # Keep track of docs indexed and batch iteration
            docs_indexed += len(docs)
            it += 1
            batch_run += 1

            # Send processed documents to solr  for indexing as a new thread.
            # max threads is set in config
            indexthread = threading.Thread(target=self.add2solr, name="Index thread %s" % (
                len(indexthreads)+1), args=(docs, self.msg_callback))
            indexthreads.append(indexthread)
            logger.debug("Starting thread: %s", indexthread.getName())
            indexthread.start()

            # If we have reached maximum threads, we wait until finished
            # if len(indexthreads) >= self.threads:
            #     thr = indexthreads.pop(0)
            #     thr.join()
            # if len(indexthreads) >= self.threads:
            #    for thr in indexthreads:
            #        thr.join()

        #   print("===================================")
        #   print("Added %s documents to solr. Total: %s" % (len(docs),docs_indexed))
        #   print("===================================")

        """############### BATCH LOOP END  ############################
        # wait for any threads still running to complete"""
        for thr in indexthreads:
            thr.join()

        # Last we assume all pending parents are in the index
        ppending = set(parent_ids_pending)
        if len(ppending) > 0:
            logger.debug(
                "The last parents should be in index, or was processed by another worker.")
            for pid in ppending:

                myparent = None
                myparent = get_dataset(pid)
                if myparent['doc'] is not None:
                    logger.debug("pending parent found in index: %s, isParent: %s",
                                 myparent['doc']['id'], myparent['doc']['isParent'])

                    if myparent['doc']['isParent'] is False:
                        logger.debug('Update on indexed parent %s, isParent: True' % pid)
                        mydoc_ = IndexMMD._solr_update_parent_doc(myparent['doc'])

                        # doc = {'id': pid, 'isParent': True}
                        try:
                            solr_add([mydoc_])
                        except Exception as e:
                            logger.error("Could not update parent on index. reason %s", e)
                            # Update lists
                        parent_ids_processed.add(pid)

                        # Remove from pending list
                        if pid in parent_ids_pending:
                            parent_ids_pending.remove(pid)

        Futures.ALL_COMPLETED
        # Store the tracking information and return back to calling script
        parent_ids_found_ = parent_ids_found.copy()
        parent_ids_pending_ = parent_ids_pending.copy()
        parent_ids_processed_ = parent_ids_processed.copy()
        doc_ids_processed_ = doc_ids_processed.copy()
        docs_failed_ = docs_skipped
        docs_indexed_ = docs_indexed
        files_processed_ = files_processed

        # Close the connection
        # session.close()
        # self.mysolr.solrc.session.close()

        return (parent_ids_found_,
                parent_ids_pending_,
                parent_ids_processed_,
                doc_ids_processed_,
                docs_failed_,
                docs_indexed_,
                files_processed_)
