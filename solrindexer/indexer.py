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
import os
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone

from solrindexer.failure_tracker import FailureTracker
from solrindexer.io import load_file
from solrindexer.mmd import MMD4SolR
from solrindexer.tools import (
    add_adc_thumbnails_bulk,
    add_nbs_thumbnail_bulk,
    process_feature_type,
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
        if self.solr_client is not None:
            try:
                _ = self.solr_client.get_session()  # Eagerly initialize session
                logger.debug("Solr session initialized")
            except Exception as e:
                logger.warning("Failed to eagerly initialize Solr session: %s", e)
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
            vocabulary_cache_dir = self.config.get("vocabulary-cache-dir")
            if isinstance(vocabulary_cache_dir, str):
                vocabulary_cache_dir = vocabulary_cache_dir.strip()
                if vocabulary_cache_dir:
                    vocabulary_cache_dir = os.path.expanduser(
                        os.path.expandvars(vocabulary_cache_dir)
                    )
                else:
                    vocabulary_cache_dir = None
            try:
                self.vocabulary_loader = create_vocabulary_loader(
                    ttl_path=vocabulary_ttl_path,
                    backend=vocabulary_backend,
                    endpoint_base_url=vocabulary_endpoint_base_url,
                    endpoint_timeout=vocabulary_endpoint_timeout,
                    cache_dir=vocabulary_cache_dir,
                )
                if self.vocabulary_loader is not None:
                    logger.info(
                        "Vocabulary loader initialized with backend: %s",
                        vocabulary_backend,
                    )
            except Exception as e:
                logger.error("❌ Failed to initialize vocabulary loader: %s", e)
                logger.warning("⚠️  Continuing without vocabulary validation")

        self.tflg = tflg

    def mmd2solr(self, mmd, status, file):
        """
        Convert mmd dict to solr dict

        Check for presence of children and mark them as children.
        If children found return parentid together with the solrdoc
        """

        if mmd is None:
            logger.warning("File %s was not parsed", file)
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

        def _as_list(value):
            if value is None:
                return []
            if isinstance(value, list):
                return value
            return [value]

        def _parse_iso_datetime(value):
            if value is None:
                return None
            text = str(value).strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                return None
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed

        def _validate_temporal_ranges(solr_doc):
            starts = _as_list(solr_doc.get("temporal_extent_start_date"))
            ends = _as_list(solr_doc.get("temporal_extent_end_date"))

            for index, start_value in enumerate(starts):
                end_value = ends[index] if index < len(ends) else None
                if end_value is None or str(end_value).strip() == "":
                    # Open-ended ranges are valid.
                    continue
                start_dt = _parse_iso_datetime(start_value)
                end_dt = _parse_iso_datetime(end_value)
                if start_dt is None or end_dt is None:
                    continue
                if start_dt > end_dt:
                    return (
                        "Invalid temporal extent range at index "
                        f"{index}: start_date '{start_value}' is newer than end_date "
                        f"'{end_value}'"
                    )

            for period_value in _as_list(solr_doc.get("temporal_extent_period_dr")):
                if period_value is None:
                    continue
                period_text = str(period_value).strip()
                if not period_text:
                    continue
                match = re.match(r"^\[\s*(.*?)\s+TO\s+(.*?)\s*\]$", period_text)
                if not match:
                    continue
                period_start = match.group(1).strip()
                period_end = match.group(2).strip()
                if "*" in period_start or "*" in period_end:
                    # Ignore open-ended wildcard date ranges.
                    continue

                start_dt = _parse_iso_datetime(period_start)
                end_dt = _parse_iso_datetime(period_end)
                if start_dt is None or end_dt is None:
                    continue
                if start_dt > end_dt:
                    return (
                        "Invalid temporal_extent_period_dr range: start_date "
                        f"'{period_start}' is newer than end_date '{period_end}'"
                    )

            return None

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
                warning_message=_clean_validation_message(message),
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
            logger.error("File %s did not pass the mmd check, cannot index.", file)
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
            logger.error("File %s could not be converted to solr document. Reason: %s", file, e)
            metadata_id = mydoc.get_metadata_identifier()
            self.failure_tracker.add_failure(
                filename=file,
                error_message=f"Solr document conversion failed: {str(e)}",
                error_stage="conversion",
                metadata_identifier=metadata_id,
            )
            return (None, status)

        # Do some sanity checking of the documents and skip docs with problems.
        if tmpdoc is None:
            logger.warning("Solr document for file %s was empty", file)
            self.failure_tracker.add_failure(
                filename=file,
                error_message="Generated Solr document was empty",
                error_stage="conversion",
            )
            return (None, status)

        if "id" not in tmpdoc:
            logger.warning("File %s have no id. Missing metadata_identifier?", file)
            self.failure_tracker.add_failure(
                filename=file,
                error_message="Missing 'id' field in Solr document (missing metadata_identifier)",
                error_stage="conversion",
            )
            return (None, status)

        if tmpdoc["id"] is None or tmpdoc["id"] == "Unknown":
            logger.warning(
                "Skipping process file %s. Metadata identifier: Unknown, or missing",
                file,
            )
            self.failure_tracker.add_failure(
                filename=file,
                error_message="Metadata identifier is None or 'Unknown'",
                error_stage="conversion",
                metadata_identifier=tmpdoc["id"],
            )
            return (None, status)

        if "temporal_extent_start_date" not in tmpdoc:
            logger.error("Could not find start date in %s.", file)
            self.failure_tracker.add_failure(
                filename=file,
                error_message="Missing temporal_extent_start_date field",
                error_stage="conversion",
                metadata_identifier=tmpdoc.get("id"),
            )
            return (None, status)

        temporal_range_error = _validate_temporal_ranges(tmpdoc)
        if temporal_range_error is not None:
            logger.error(
                "File %s failed temporal range sanity checks: %s", file, temporal_range_error
            )
            self.failure_tracker.add_failure(
                filename=file,
                error_message=temporal_range_error,
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

        # Start timer.
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

    def bulkindex(self, filelist):
        """Index MMD files using a unified streaming pipeline.

        All futures (pipeline + feature_type) are tracked in a single set.
        As each future completes it is processed immediately — feature_type
        futures are added to the same set on the fly, so chunk flushing is
        continuous rather than deferred to a second phase.
        """

        chunksize = self.chunksize
        skip_feature_type = self.config.get("skip-feature-type", False)
        nbs_scope = self.config.get("scope", "") == "NBS"
        adc_scope = self.config.get("scope", "") == "ADC"

        logger.debug("-- Got %d input file(s)", len(filelist))

        parent_ids_referenced = set()

        files_processed = 0
        docs_indexed = 0
        docs_skipped = 0

        t_bulk_start = time.perf_counter()
        t_feature_type = 0.0
        t_index_dispatch = 0.0
        t_index_join = 0.0

        # ------------------------------------------------------------------ #
        # _pipeline: load XML, validate, convert to Solr doc, add thumbnails.#
        # Returns (doc, status); never raises.                                #
        # ------------------------------------------------------------------ #
        def _pipeline(file_path):
            mmd = load_file(file_path)
            doc, status = self.mmd2solr(mmd, None, file_path)
            if doc is None:
                return (None, None)
            if self.tflg and (nbs_scope or adc_scope):
                if "data_access_url_ogc_wms" in doc:
                    if nbs_scope:
                        doc = add_nbs_thumbnail_bulk((doc, self.config or {}))
                    elif adc_scope:
                        doc = add_adc_thumbnails_bulk((doc, self.config or {}))
            return (doc, status)

        # ------------------------------------------------------------------ #
        # Streaming chunk state (accessed only from the main loop thread).   #
        # ------------------------------------------------------------------ #
        current_chunk: list = []
        current_chunk_file_ids: dict = {}

        def _flush_chunk_if_full():
            nonlocal docs_indexed, t_index_dispatch, chunk_count, docs_dispatched
            if len(current_chunk) < chunksize:
                return
            chunk_count += 1
            chunk_start = docs_dispatched + 1
            docs_dispatched += len(current_chunk)
            docs_indexed += len(current_chunk)
            logger.info(
                "---- Indexing chunk %d: docs %d–%d (feature_type pending=%d) ----",
                chunk_count,
                chunk_start,
                docs_dispatched,
                feature_pending - feature_completed,
            )
            t0 = time.perf_counter()
            indexthread = threading.Thread(
                target=self.add2solr,
                name=f"Index thread {chunk_count}",
                args=(list(current_chunk), dict(current_chunk_file_ids)),
            )
            indexthread.start()
            self.indexthreads.append(indexthread)
            logger.debug("Started %s", indexthread.name)
            t_index_dispatch += time.perf_counter() - t0
            current_chunk.clear()
            current_chunk_file_ids.clear()

        def _add_doc_to_chunk(doc, file_path):
            current_chunk.append(doc)
            current_chunk_file_ids[doc["id"]] = file_path
            _flush_chunk_if_full()

        # ------------------------------------------------------------------ #
        # Unified futures loop: pipeline futures + feature_type futures share #
        # one `remaining` set.  Feature_type futures are added mid-loop.     #
        # ------------------------------------------------------------------ #
        # Tag each future so we know how to handle its result:
        #   all_futures[future] = ("pipeline", file_path)
        #   all_futures[future] = ("feature", original_doc, file_path)
        all_futures: dict = {}
        remaining: set = set()

        total_pipeline = len(filelist)
        pipeline_completed = 0
        feature_pending = 0
        feature_completed = 0
        chunk_count = 0
        docs_dispatched = 0

        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            for f in filelist:
                fut = executor.submit(_pipeline, f)
                all_futures[fut] = ("pipeline", f)
                remaining.add(fut)

            while remaining:
                done, remaining = wait(remaining, return_when=FIRST_COMPLETED)

                for future in done:
                    tag = all_futures.pop(future)
                    kind = tag[0]

                    if kind == "pipeline":
                        file_path = tag[1]
                        files_processed += 1
                        pipeline_completed += 1

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
                            if status is not None:
                                parent_ids_referenced.add(status)

                            if not skip_feature_type and "data_access_url_opendap" in doc:
                                # Submit feature_type and keep it in the same loop
                                t0 = time.perf_counter()
                                ft_fut = executor.submit(process_feature_type, doc)
                                t_feature_type += time.perf_counter() - t0
                                all_futures[ft_fut] = ("feature", doc, file_path)
                                remaining.add(ft_fut)
                                feature_pending += 1
                            else:
                                _add_doc_to_chunk(doc, file_path)

                        if (
                            pipeline_completed % chunksize == 0
                            or pipeline_completed == total_pipeline
                        ):
                            logger.info(
                                "Progress: parsed %d / %d files (feature_type pending=%d)",
                                pipeline_completed,
                                total_pipeline,
                                feature_pending - feature_completed,
                            )

                    else:  # kind == "feature"
                        original_doc, file_path = tag[1], tag[2]
                        feature_completed += 1
                        t0 = time.perf_counter()
                        try:
                            newdoc, ft_error = future.result()
                            if ft_error is not None:
                                self.failure_tracker.add_warning(
                                    filename=file_path,
                                    warning_message=ft_error,
                                    warning_stage="feature_type",
                                    metadata_identifier=original_doc.get("id"),
                                )
                            _add_doc_to_chunk(newdoc, file_path)
                        except Exception as e:
                            logger.error("Feature type future failed for %s: %s", file_path, e)
                        t_feature_type += time.perf_counter() - t0

        # Flush remaining docs that didn't fill a full chunk
        if current_chunk:
            chunk_count += 1
            chunk_start = docs_dispatched + 1
            docs_dispatched += len(current_chunk)
            docs_indexed += len(current_chunk)
            logger.info(
                "---- Indexing final chunk %d: docs %d–%d ----",
                chunk_count,
                chunk_start,
                docs_dispatched,
            )
            t0 = time.perf_counter()
            indexthread = threading.Thread(
                target=self.add2solr,
                name=f"Index thread {chunk_count}",
                args=(current_chunk, current_chunk_file_ids),
            )
            indexthread.start()
            self.indexthreads.append(indexthread)
            t_index_dispatch += time.perf_counter() - t0

        # Wait for all Solr index threads to finish
        t_index_join_start = time.perf_counter()
        for thr in self.indexthreads:
            thr.join()
        t_index_join = time.perf_counter() - t_index_join_start

        t_bulk_total = time.perf_counter() - t_bulk_start
        logger.debug(
            "timing.bulkindex total=%.3fs feature_type=%.3fs "
            "index_dispatch=%.3fs index_join=%.3fs",
            t_bulk_total,
            t_feature_type,
            t_index_dispatch,
            t_index_join,
        )

        return (
            parent_ids_referenced.copy(),
            docs_skipped,
            docs_indexed,
            files_processed,
            self.failure_tracker,
        )
