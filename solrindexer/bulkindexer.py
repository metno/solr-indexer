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
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from solrindexer.failure_tracker import FailureTracker
from solrindexer.indexdata import MMD4SolR
from solrindexer.multithread.io import load_file
from solrindexer.multithread.threads import multiprocess
from solrindexer.tools import (
    add_nbs_thumbnail_bulk,
    get_dataset,
    process_feature_type,
    set_parent_flag,
    solr_add,
    to_solr_id,
)
from solrindexer.vocabulary import create_vocabulary_loader

logger = logging.getLogger(__name__)


class BulkIndexer:
    """Do multithreaded bulkindexing given a list of file names.
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

    def __init__(
        self,
        inputList,
        solr_url,
        threads=20,
        chunksize=2500,
        auth=None,
        tflg=False,
        solr_client=None,
        config=None,
        failure_tracker=None,
    ):
        """Initialize BulkIndexer"""
        logger.debug("Initializing BulkIndexer.")
        self.inputList = inputList
        self.threads = threads
        self.chunksize = chunksize
        self.total_in = len(inputList)
        self.indexthreads = []
        self.solr_url = solr_url
        self.auth = auth
        self.solr_client = solr_client
        self.config = config
        self.failure_tracker = failure_tracker or FailureTracker()

        # Initialize vocabulary loader if configured
        self.vocabulary_loader = None
        if self.config:
            vocabulary_backend = self.config.get("vocabulary-backend", "native")
            vocabulary_ttl_path = self.config.get("vocabulary-ttl-path")
            vocabulary_endpoint_base_url = self.config.get(
                "vocabulary-endpoint-base-url",
                "https://vocab.met.no/mmd",
            )
            vocabulary_endpoint_timeout = float(
                self.config.get("vocabulary-endpoint-timeout", 20.0)
            )
            try:
                self.vocabulary_loader = create_vocabulary_loader(
                    ttl_path=vocabulary_ttl_path,
                    backend=vocabulary_backend,
                    endpoint_base_url=vocabulary_endpoint_base_url,
                    endpoint_timeout=vocabulary_endpoint_timeout,
                )
                if self.vocabulary_loader is not None:
                    logger.info(
                        f"Vocabulary loader initialized with backend: {vocabulary_backend}"
                    )
            except Exception as e:
                logger.error(f"Failed to initialize vocabulary loader: {e}")
                logger.warning("Continuing without vocabulary validation")

        self.tflg = tflg

    def mmd2solr(self, mmd, status, file):
        """
        Convert mmd dict to solr dict

        Check for presence of children and mark them as children.
        If children found return parentid together with the solrdoc
        """

        if mmd is None:
            logger.warning(f"File {file} was not parsed")
            self.failure_tracker.add_failure(
                filename=file,
                error_message="File was not parsed (XML parsing failed)",
                error_stage="parsing",
            )
            return (None, status)
        if file is not None and file.endswith("\n"):
            file = file[:-1]
        xsd_path = self.config.get("mmd-xsd-path") if self.config else None
        metadata_id = None
        validation_messages = []

        def _clean_validation_message(message):
            """Remove leading icon tokens from warning text for summary readability."""
            return re.sub(r"^(?:\[FAIL\]|\[WARN\]|❌|⚠️)\s*", "", message).strip()

        def _warning_callback(message, warning_stage="validation"):
            nonlocal metadata_id
            if metadata_id is None:
                metadata_id = mydoc.get_metadata_identifier()
            if warning_stage == "validation":
                validation_messages.append(message)
                # Missing required-field messages are promoted to failure summary
                # and should not be duplicated under warnings.
                if "check_mmd missing required" in message:
                    return
            self.failure_tracker.add_warning(
                filename=file,
                warning_message=message,
                warning_stage=warning_stage,
                metadata_identifier=metadata_id,
            )

        mydoc = MMD4SolR(
            filename=None,
            mydoc=mmd,
            bulkFile=file,
            xsd_path=xsd_path,
            warning_callback=_warning_callback,
            vocabulary_loader=self.vocabulary_loader,
        )
        if not mydoc.check_mmd():
            logger.error(f"File {file} did not pass the mmd check, cannot index.")
            # Try to extract metadata_identifier even if validation failed
            metadata_id = mydoc.get_metadata_identifier()

            missing_required_messages = [
                _clean_validation_message(msg)
                for msg in validation_messages
                if "check_mmd missing required" in msg
            ]
            if missing_required_messages:
                # Keep message order, drop duplicates.
                missing_required_messages = list(dict.fromkeys(missing_required_messages))
                failure_message = "; ".join(missing_required_messages)
            else:
                xsd_line_messages = [
                    _clean_validation_message(msg)
                    for msg in validation_messages
                    if msg.strip().startswith("line ")
                ]
                if xsd_line_messages:
                    xsd_line_messages = list(dict.fromkeys(xsd_line_messages))
                    failure_message = "XSD validation failed: " + "; ".join(xsd_line_messages)
                else:
                    failure_message = "Failed MMD validation checks"

            self.failure_tracker.add_failure(
                filename=file,
                error_message=failure_message,
                error_stage="validation",
                metadata_identifier=metadata_id,
            )
            return (None, status)

        # Convert mmd xml dict to solr dict
        try:
            tmpdoc = mydoc.tosolr()
        except Exception as e:
            logger.error(f"File {file} could not be converted to solr document. Reason: {e}")
            metadata_id = mydoc.get_metadata_identifier()
            self.failure_tracker.add_failure(
                filename=file,
                error_message=f"Solr document conversion failed: {str(e)}",
                error_stage="conversion",
                metadata_identifier=metadata_id,
            )
            return (None, status)

        """ Do some sanity checking of the documents and skip docs with problems"""
        if tmpdoc is None:
            logger.warning(f"Solr document for file {file} was empty")
            self.failure_tracker.add_failure(
                filename=file,
                error_message="Generated Solr document was empty",
                error_stage="conversion",
            )
            return (None, status)

        if "id" not in tmpdoc:
            logger.warning("File %s have no id. Missing metadata_identifier?" % file)
            self.failure_tracker.add_failure(
                filename=file,
                error_message="Missing 'id' field in Solr document (missing metadata_identifier)",
                error_stage="conversion",
            )
            return (None, status)

        if tmpdoc["id"] is None or tmpdoc["id"] == "Unknown":
            logger.warning(
                "Skipping process file %s. Metadata identifier: Unknown, or missing" % file
            )
            self.failure_tracker.add_failure(
                filename=file,
                error_message="Metadata identifier is None or 'Unknown'",
                error_stage="conversion",
                metadata_identifier=tmpdoc["id"],
            )
            return (None, status)

        if "temporal_extent_start_date" not in tmpdoc:
            logger.error("Could not find start date in  %s." % file)
            self.failure_tracker.add_failure(
                filename=file,
                error_message="Missing temporal_extent_start_date field",
                error_stage="conversion",
                metadata_identifier=tmpdoc.get("id"),
            )
            return (None, status)

        if "related_dataset" in tmpdoc:
            logger.debug("got related dataset")
            if isinstance(tmpdoc["related_dataset"], str):
                logger.debug("processing child")
                # Manipulate the related_dataset id to solr id
                # Special fix for NPI
                tmpdoc["related_dataset"] = tmpdoc["related_dataset"].replace(
                    "https://data.npolar.no/dataset/", ""
                )
                tmpdoc["related_dataset"] = tmpdoc["related_dataset"].replace(
                    "http://data.npolar.no/dataset/", ""
                )
                tmpdoc["related_dataset"] = tmpdoc["related_dataset"].replace(
                    "http://api.npolar.no/dataset/", ""
                )
                tmpdoc["related_dataset"] = tmpdoc["related_dataset"].replace(".xml", "")
                # Skip if DOI is used to refer to parent, that isn't consistent.
                if "doi.org" not in tmpdoc["related_dataset"]:
                    # Update document with child specific fields
                    tmpdoc.update({"dataset_type": "Level-2"})
                    tmpdoc.update({"isChild": True})
                    # tmpdoc.update({'isParent': False})

                    # Fix special characters that SolR doesn't like
                    myparentid = tmpdoc["related_dataset"]
                    tmpdoc.update({"related_dataset": myparentid.strip()})
                    mysolrparentid = to_solr_id(myparentid)
                    tmpdoc.update({"related_dataset_id": mysolrparentid})
                    status = mysolrparentid

        else:
            # Assume we have level-1 doc that are not parent
            tmpdoc.update({"dataset_type": "Level-1"})
            tmpdoc.update({"isParent": False})

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

    def add2solr(self, docs, file_ids=None):
        """Add documents to SolR

        Parameters
        ----------
        docs : list
            List of Solr documents to index
        file_ids : dict, optional
            Dict mapping document IDs to originating filenames, for failure tracking
        """

        """ Start timer"""
        st = time.perf_counter()
        pst = time.process_time()
        file_ids = file_ids or {}

        if self.solr_client is None:
            raise ValueError("BulkIndexer requires a configured Solr client")

        try:
            solr_add(docs, solr_client=self.solr_client)
        # Handle failing indexing
        except Exception as e:
            tname = threading.current_thread().name
            tid = threading.get_native_id()
            error_msg = f"{tname}, PID: {tid} Some documents failed to be added to solr. \
                reason: {e}"
            logger.error(error_msg)
            # Track each document that failed to index
            for doc in docs:
                filename = file_ids.get(doc.get("id"), "unknown")
                self.failure_tracker.add_failure(
                    filename=filename,
                    error_message=f"Solr indexing failed: {str(e)}",
                    error_stage="indexing",
                    metadata_identifier=doc.get("id"),
                )

        # If success
        et = time.perf_counter()
        pet = time.process_time()
        elapsed_time = et - st
        pelt = pet - pst
        etime = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        ctime = time.strftime("%H:%M:%S", time.gmtime(pelt))
        logger.info(
            "-- Indexed %d documents to SolR. Elapsed time: %s, CPU time: %s",
            len(docs),
            etime,
            ctime,
        )

    def msg_callback(self, msg):
        """Message logging callback function"""
        logger.info(msg)

    def _should_use_process_pool(self, doc_count):
        """Decide whether process-pool overhead is worth it for this stage.

        For tiny batches, inline processing is faster than spinning up workers.
        """
        if self.threads <= 1:
            return False
        min_docs = int((self.config or {}).get("process-pool-min-docs", 100))
        return doc_count >= max(2, min_docs)

    def _log_parent_integrity(self, parent_ids_found, parent_ids_processed, parent_ids_pending):
        """Log parent/child integrity summary after a bulk run."""
        if parent_ids_found:
            logger.info(" --- Parent/child integrity summary --- ")
            logger.info("Parents found: %d", len(parent_ids_found))

            if parent_ids_pending:
                logger.warning("Unresolved parent IDs referenced by child documents:")
                for parent_id in sorted(parent_ids_pending):
                    logger.warning("  %s", parent_id)
            else:
                logger.info("✅ all parents resolved.")

    def _resolve_parent_child(
        self,
        docs,
        file_ids,
        parent_ids_pending,
        parent_ids_processed,
        parent_ids_found,
        doc_ids_processed,
        statuses,
    ):
        """Resolve parent/child relationships for a completed chunk.

        Updates ``docs`` in-place (marks parents as isParent=True), and
        updates the four tracking sets.  Any Solr queries needed to locate
        already-indexed parents are made here on the calling thread.

        Parameters
        ----------
        docs : list
            Mutable list of Solr documents in the current chunk.
        file_ids : dict
            Maps doc['id'] -> source filename for the current chunk.
        parent_ids_pending : set
            Globally accumulated set of parent IDs not yet resolved.
        parent_ids_processed : set
            Globally accumulated set of parent IDs already resolved.
        parent_ids_found : set
            Globally accumulated set of all parent IDs ever seen.
        doc_ids_processed : set
            All document IDs ever indexed (across all chunks).
        statuses : list
            Status values returned by mmd2solr; non-None means the doc is
            a child whose parent has the given ID.
        """
        parentids = {s for s in statuses if s is not None}
        if parentids:
            logger.debug("parent ids in this chunk: %s", parentids)

        for pid in parentids:
            parent_ids_found.add(pid)
            if pid not in parent_ids_pending and pid not in parent_ids_processed:
                parent_ids_pending.add(pid)

        # Strip pids already resolved in previous chunks
        parentids -= parent_ids_processed
        parentids -= parent_ids_found - parentids  # keep only truly new ones
        # Re-derive: keep pids that are still pending (not yet processed)
        parentids = {
            pid
            for pid in {s for s in statuses if s is not None}
            if pid not in parent_ids_processed
        }

        for pid in parentids:
            parent_found = False
            logger.debug("checking parent: %s", pid)
            parent = [el for el in docs if el["id"] == pid]
            logger.debug("parents found in this chunk: %s", parent)

            if parent:
                myparent = parent[0]
                logger.debug("parent found in current chunk: %s", myparent["id"])
                parent_found = True
                if myparent["isParent"] is False:
                    logger.debug("found pending parent %s in this job — updating", pid)
                    myparent.update({"isParent": True})
                    parent_ids_pending.discard(pid)
                    parent_ids_processed.add(pid)

            if pid in doc_ids_processed and not parent_found:
                myparent = get_dataset(pid, solr_client=self.solr_client)
                if myparent is not None:
                    if myparent["doc"] is None:
                        if pid not in parent_ids_pending:
                            logger.debug("parent %s not found in index, storing for later", pid)
                            parent_ids_pending.add(pid)
                    elif myparent["doc"]["isParent"] is False:
                        logger.debug("Update on indexed parent %s, isParent: True", pid)
                        try:
                            set_parent_flag(pid, solr_client=self.solr_client)
                        except Exception as e:
                            logger.error("Could not update parent in index. reason: %s", e)
                        parent_ids_processed.add(pid)
                        parent_ids_pending.discard(pid)

        # Resolve parents that were pending from previous chunks
        ppending = set(parent_ids_pending)
        if ppending:
            logger.info(" --- Checking parent/child integrity --- ")
            for pid in ppending:
                parent_found = False
                parent = [el for el in docs if el["id"] == pid]
                if parent:
                    myparent = parent[0]
                    logger.debug("pending parent found in current chunk: %s", myparent["id"])
                    parent_found = True
                    if myparent["isParent"] is False:
                        logger.debug("found unprocessed pending parent %s — updating", pid)
                        myparent.update({"isParent": True})
                        parent_ids_pending.discard(pid)
                        parent_ids_processed.add(pid)

                if pid in doc_ids_processed and not parent_found:
                    myparent = get_dataset(pid, solr_client=self.solr_client)
                    if myparent is not None and myparent["doc"] is not None:
                        logger.debug(
                            "pending parent found in index: %s, isParent: %s",
                            myparent["doc"]["id"],
                            myparent["doc"]["isParent"],
                        )
                        if myparent["doc"]["isParent"] is False:
                            logger.debug("Update on indexed parent %s, isParent: True", pid)
                            try:
                                set_parent_flag(pid, solr_client=self.solr_client)
                            except Exception as e:
                                logger.error("Could not update parent. reason: %s", e)
                            parent_ids_processed.add(pid)
                            parent_ids_pending.discard(pid)

    def bulkindex(self, filelist):
        """Index MMD files to Solr using a two-phase pipeline.

        Phase 1: load/parse/convert all files concurrently.
        Phase 2: process feature_type/thumbnails and index in chunks.

        Parent/child integrity is resolved once globally (not per chunk).
        """

        chunksize = self.chunksize
        skip_feature_type = self.config.get("skip-feature-type", False)
        nbs_scope = self.config.get("scope", "") == "NBS"

        logger.debug("-- Got %d input file(s)", len(filelist))

        # Tracking sets (mutated by _resolve_parent_child on the main thread)
        parent_ids_pending = set()
        parent_ids_processed = set()
        parent_ids_found = set()
        doc_ids_processed = set()

        files_processed = 0
        docs_indexed = 0
        docs_skipped = 0

        # Stage timing (wall-clock seconds)
        t_bulk_start = time.perf_counter()
        t_phase1 = 0.0
        t_parent_prepare = 0.0
        t_feature_type = 0.0
        t_thumbnail = 0.0
        t_index_dispatch = 0.0
        t_final_parent = 0.0
        t_index_join = 0.0

        # ------------------------------------------------------------------ #
        # Per-file closure: runs entirely inside a worker thread.             #
        # Returns (doc, status); never raises — exceptions go to tracker.     #
        # ------------------------------------------------------------------ #
        def _pipeline(file_path):
            # Stage 1: load XML
            mmd = load_file(file_path)

            # Stage 2: parse, validate, convert to Solr doc
            doc, status = self.mmd2solr(mmd, None, file_path)
            return (doc, status)

        docs: list = []
        statuses: list = []
        file_ids: dict = {}

        # ------------------------------------------------------------------ #
        # Phase 1: Read and convert all files concurrently                    #
        # ------------------------------------------------------------------ #
        t_phase1_start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = {executor.submit(_pipeline, f): f for f in filelist}
            total = len(futures)
            completed = 0

            for future in as_completed(futures):
                file_path = futures[future]
                files_processed += 1
                completed += 1

                try:
                    doc, status = future.result()
                except Exception as e:
                    logger.error("Unexpected pipeline error for %s: %s", file_path, e)
                    self.failure_tracker.add_failure(
                        filename=file_path,
                        error_message=f"Unexpected pipeline error: {e}",
                        error_stage="pipeline",
                    )
                    docs_skipped += 1
                    continue

                if doc is None:
                    docs_skipped += 1
                else:
                    docs.append(doc)
                    statuses.append(status)
                    file_ids[doc["id"]] = file_path

                if completed % chunksize == 0 or completed == total:
                    logger.info("Progress: completed %d / %d files", completed, total)
        t_phase1 = time.perf_counter() - t_phase1_start
        logger.debug(
            "timing.phase1_load_convert=%.3fs files=%d docs_ok=%d docs_skipped=%d",
            t_phase1,
            files_processed,
            len(docs),
            docs_skipped,
        )

        # Build parent-tracking state once for the full document set.
        t_parent_prepare_start = time.perf_counter()
        doc_ids_processed.update(d["id"] for d in docs)
        parentids = {s for s in statuses if s is not None}
        parent_ids_found.update(parentids)
        for pid in parentids:
            if pid in doc_ids_processed:
                # Parent present in this run: mark directly before indexing.
                parent_docs = [el for el in docs if el["id"] == pid]
                if parent_docs:
                    parent_doc = parent_docs[0]
                    if parent_doc.get("isParent") is False:
                        parent_doc.update({"isParent": True})
                    parent_ids_processed.add(pid)
                else:
                    parent_ids_pending.add(pid)
            else:
                parent_ids_pending.add(pid)
        t_parent_prepare = time.perf_counter() - t_parent_prepare_start
        logger.debug(
            "timing.parent_prepare=%.3fs parent_refs=%d pending=%d processed=%d",
            t_parent_prepare,
            len(parentids),
            len(parent_ids_pending),
            len(parent_ids_processed),
        )

        # ------------------------------------------------------------------ #
        # Phase 2: Process and index in chunks                                #
        # ------------------------------------------------------------------ #
        if skip_feature_type is True:
            logger.info("skip-feature-type is True in config. Skipping feature type..")

        for i in range(0, len(docs), chunksize):
            chunk_docs = list(docs[i : i + chunksize])
            if not chunk_docs:
                continue

            chunk_file_ids = {doc["id"]: file_ids.get(doc["id"], "unknown") for doc in chunk_docs}

            if skip_feature_type is not True:
                dap_docs = [doc for doc in chunk_docs if "data_access_url_opendap" in doc]
                if dap_docs:
                    t_feature_chunk_start = time.perf_counter()
                    if self._should_use_process_pool(len(dap_docs)):
                        logger.info(
                            "---- Process featureType with processes %d ----", self.threads
                        )
                        for doc, (newdoc, ft_error) in multiprocess(
                            fn=process_feature_type,
                            inputs=dap_docs,
                            max_concurrency=self.threads,
                        ):
                            chunk_docs.remove(doc)
                            chunk_docs.append(newdoc)
                            if ft_error is not None:
                                self.failure_tracker.add_warning(
                                    filename=chunk_file_ids.get(doc.get("id"), "unknown"),
                                    warning_message=ft_error,
                                    warning_stage="feature_type",
                                    metadata_identifier=doc.get("id"),
                                )
                    else:
                        logger.info(
                            "---- Process featureType inline for small batch (%d docs) ----",
                            len(dap_docs),
                        )
                        for doc in dap_docs:
                            newdoc, ft_error = process_feature_type(doc)
                            chunk_docs.remove(doc)
                            chunk_docs.append(newdoc)
                            if ft_error is not None:
                                self.failure_tracker.add_warning(
                                    filename=chunk_file_ids.get(doc.get("id"), "unknown"),
                                    warning_message=ft_error,
                                    warning_stage="feature_type",
                                    metadata_identifier=doc.get("id"),
                                )
                    t_feature_chunk = time.perf_counter() - t_feature_chunk_start
                    t_feature_type += t_feature_chunk
                    logger.debug(
                        "timing.chunk_feature_type=%.3fs chunk_size=%d dap_docs=%d",
                        t_feature_chunk,
                        len(chunk_docs),
                        len(dap_docs),
                    )

            if self.tflg is True:
                thumb_docs = [doc for doc in chunk_docs if "data_access_url_ogc_wms" in doc]
                if thumb_docs and nbs_scope:
                    t_thumb_chunk_start = time.perf_counter()
                    thumb_inputs = [(doc, self.config or {}) for doc in thumb_docs]
                    if self._should_use_process_pool(len(thumb_docs)):
                        logger.info("---- Creating thumbnails concurrently %d ----", self.threads)
                        for (doc, _), newdoc in multiprocess(
                            fn=add_nbs_thumbnail_bulk,
                            inputs=thumb_inputs,
                            max_concurrency=self.threads,
                        ):
                            chunk_docs.remove(doc)
                            chunk_docs.append(newdoc)
                    else:
                        logger.info(
                            "---- Creating thumbnails inline for small batch (%d docs) ----",
                            len(thumb_docs),
                        )
                        for doc in thumb_docs:
                            newdoc = add_nbs_thumbnail_bulk((doc, self.config or {}))
                            chunk_docs.remove(doc)
                            chunk_docs.append(newdoc)
                    t_thumb_chunk = time.perf_counter() - t_thumb_chunk_start
                    t_thumbnail += t_thumb_chunk
                    logger.debug(
                        "timing.chunk_thumbnail=%.3fs chunk_size=%d thumb_docs=%d",
                        t_thumb_chunk,
                        len(chunk_docs),
                        len(thumb_docs),
                    )
                elif thumb_docs:
                    logger.warning(
                        "Thumbnail flag enabled, but only NBS thumbnail generation is supported"
                    )

            docs_indexed += len(chunk_docs)
            logger.info("---- Indexing %d documents ----", len(chunk_docs))
            t_index_dispatch_start = time.perf_counter()
            indexthread = threading.Thread(
                target=self.add2solr,
                name="Index thread %d" % (len(self.indexthreads) + 1),
                args=(chunk_docs, chunk_file_ids),
            )
            indexthread.start()
            self.indexthreads.append(indexthread)
            logger.debug("Started %s", indexthread.name)
            t_index_dispatch += time.perf_counter() - t_index_dispatch_start

        # Resolve any parents that remained pending until the very end
        ppending = set(parent_ids_pending)
        t_final_parent_start = time.perf_counter()
        if ppending:
            logger.info(" --- Final parent/child integrity pass --- ")
            logger.debug("Checking %d unresolved parent IDs", len(ppending))
            for pid in ppending:
                myparent = get_dataset(pid, solr_client=self.solr_client)
                if myparent is not None and myparent.get("doc") is not None:
                    logger.debug(
                        "pending parent found in index: %s, isParent: %s",
                        myparent["doc"]["id"],
                        myparent["doc"]["isParent"],
                    )
                    if myparent["doc"]["isParent"] is False:
                        logger.debug("Update on indexed parent %s, isParent: True", pid)
                        try:
                            set_parent_flag(pid, solr_client=self.solr_client)
                        except Exception as e:
                            logger.error("Could not update parent. reason: %s", e)
                        parent_ids_processed.add(pid)
                        parent_ids_pending.discard(pid)
        t_final_parent = time.perf_counter() - t_final_parent_start

        # Wait for all Solr index threads to finish
        t_index_join_start = time.perf_counter()
        for thr in self.indexthreads:
            thr.join()
        t_index_join = time.perf_counter() - t_index_join_start

        t_bulk_total = time.perf_counter() - t_bulk_start
        logger.debug(
            "timing.bulkindex total=%.3fs phase1=%.3fs parent_prepare=%.3fs feature_type=%.3fs "
            "thumbnail=%.3fs index_dispatch=%.3fs final_parent=%.3fs index_join=%.3fs",
            t_bulk_total,
            t_phase1,
            t_parent_prepare,
            t_feature_type,
            t_thumbnail,
            t_index_dispatch,
            t_final_parent,
            t_index_join,
        )

        self._log_parent_integrity(
            parent_ids_found=parent_ids_found,
            parent_ids_processed=parent_ids_processed,
            parent_ids_pending=parent_ids_pending,
        )

        return (
            parent_ids_found.copy(),
            parent_ids_pending.copy(),
            parent_ids_processed.copy(),
            doc_ids_processed.copy(),
            docs_skipped,  # position 4: docs_failed
            docs_indexed,  # position 5: docs_indexed
            files_processed,  # position 6: files_processed
            self.failure_tracker,  # position 7
        )
