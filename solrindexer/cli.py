#!/usr/bin/env python3
"""Unified indexdata CLI for single and bulk MMD ingestion."""

import argparse
import logging
import multiprocessing as mp
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pysolr
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

from solrindexer import _init_logging
from solrindexer.failure_tracker import FailureTracker
from solrindexer.indexer import BulkIndexer
from solrindexer.mmd import IndexMMD, _get_cached_schema
from solrindexer.search import parse_cfg
from solrindexer.tools import resolve_parent_ids, to_solr_id

logger = logging.getLogger(__name__)

DEFAULT_THREADS = 20
DEFAULT_CHUNKSIZE = 2500
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_WARNINGS = 3


def parse_arguments():
    parser = argparse.ArgumentParser(description="Index MMD XML files into Solr")
    parser.add_argument("-c", "--cfg", dest="cfgfile", required=True, help="Configuration file")
    parser.add_argument("-a", "--always_commit", action="store_true", help="Always commit to Solr")
    parser.add_argument("-i", "--input_file", help="Individual file to ingest")
    parser.add_argument("-l", "--list_file", help="File containing xml file paths to ingest")
    parser.add_argument("-d", "--directory", help="Directory with xml files to ingest")
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Recursively search for XML files in directory and subdirectories (requires -d)",
    )
    parser.add_argument(
        "-parent",
        "--mark_parent",
        required=False,
        help="Metadata identifier of existing Solr document to mark as parent",
    )

    parser.add_argument("--threads", type=int, default=None, help="Number of worker threads")
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Number of BulkIndexer processes for large bulk runs",
    )
    parser.add_argument("--chunksize", type=int, default=None, help="Batch size for bulk indexing")

    parser.add_argument("-t", "--thumbnail", action="store_true", help="Enable thumbnail indexing")
    parser.add_argument(
        "-n", "--no_thumbnail", action="store_true", help="Disable thumbnail indexing"
    )
    parser.add_argument("-nbs", "--nbs", action="store_true", help="Enable NBS thumbnail mode")
    parser.add_argument("-adc", "--adc", action="store_true", help="Enable ADC thumbnail mode")

    args = parser.parse_args()
    if args.nbs and args.adc:
        parser.error("Use either --nbs or --adc, not both")

    if not args.input_file and not args.list_file and not args.directory and not args.mark_parent:
        parser.print_help()
        parser.exit(EXIT_USAGE)
    return args


def main():
    _init_logging(logger)
    start_dt = datetime.now().astimezone()
    start_wall = time.perf_counter()
    start_cpu = time.process_time()
    exit_code = EXIT_SUCCESS

    try:
        args = parse_arguments()
        try:
            cfg = parse_cfg(args.cfgfile)
        except (FileNotFoundError, ValueError) as e:
            logger.error("%s", str(e))
            sys.exit(1)

        if args.nbs:
            cfg["scope"] = "NBS"
        elif args.adc:
            cfg["scope"] = "ADC"
        else:
            cfg["scope"] = cfg.get("scope")

        solr_url = _build_solr_url(cfg)
        authentication = _resolve_authentication(cfg)
        solr_client = pysolr.Solr(solr_url, always_commit=False, timeout=1020, auth=authentication)

        logger.info("Solr connection establised: %s", solr_url)
        pong = solr_client.ping()
        status = pong if isinstance(pong, str) else str(pong)
        logger.info("Solr ping response: %s", status)

        # Keep parent marking as a focused operation.
        if args.mark_parent:
            indexer = IndexMMD(solr_url, args.always_commit, authentication, cfg)
            status, msg = indexer.update_parent(to_solr_id(args.mark_parent.strip()))
            logger.info("Parent update status=%s message=%s", status, msg)
            sys.exit(EXIT_SUCCESS if status else EXIT_FAILURE)

        files = _resolve_input_files(args)
        if not files:
            raise ValueError("No input files found")

        configured_threads = (
            args.threads if args.threads is not None else int(cfg.get("threads", DEFAULT_THREADS))
        )
        # Use single worker for single file input, multiple workers for batch inputs
        if args.input_file:
            workers = 1
            logger.debug("Single file input: using 1 worker (sequential processing)")
        else:
            workers = configured_threads
            logger.debug(
                "Multiple file input: using %d workers (concurrent processing)",
                workers,
            )
        chunksize = (
            args.chunksize
            if args.chunksize is not None
            else int(cfg.get("batch-size", DEFAULT_CHUNKSIZE))
        )
        process_count = max(1, int(args.processes or 1))
        if args.input_file and process_count > 1:
            logger.warning("Single input file mode: forcing --processes=1")
            process_count = 1

        thumbnails_enabled = _resolve_thumbnail_flags(args, cfg)

        logger.info(
            "Starting indexing with files=%d workers=%d chunksize=%d processes=%d thumbnails=%s",
            len(files),
            workers,
            chunksize,
            process_count,
            thumbnails_enabled,
        )

        result = None
        if process_count > 1 and len(files) > 1:
            result = _run_bulkindex_multiprocess(
                files=files,
                process_count=process_count,
                solr_url=solr_url,
                cfg=cfg,
                workers=workers,
                chunksize=chunksize,
                thumbnails_enabled=thumbnails_enabled,
            )

        if result is None:
            bulk = BulkIndexer(
                files,
                solr_url,
                threads=workers,
                chunksize=chunksize,
                auth=authentication,
                tflg=thumbnails_enabled,
                solr_client=solr_client,
                config=cfg,
            )
            result = bulk.bulkindex(files)

        if result and len(result) >= 5:
            parent_ids_referenced = set(result[0])
            docs_failed = result[1]
            docs_indexed = result[2]
            files_processed = result[3]
            failure_tracker = result[4]

            unresolved_parent_ids = _resolve_referenced_parents(
                solr_url=solr_url,
                authentication=authentication,
                parent_ids_referenced=parent_ids_referenced,
            )
            _report_parent_integrity(
                parent_ids_referenced=parent_ids_referenced,
                unresolved_parent_ids=unresolved_parent_ids,
                failure_tracker=failure_tracker,
            )

            logger.info(
                "Indexing summary: files_processed=%d  docs_indexed=%d  docs_skipped/failed=%d",
                files_processed,
                docs_indexed,
                docs_failed,
            )
            failure_tracker.log_summary(total_input_files=len(files))
            exit_code = _determine_exit_code(failure_tracker)
        else:
            logger.info("Processed %s files.", len(files))

        if cfg.get("end-solr-commit", False):
            indexer = IndexMMD(solr_url, args.always_commit, authentication, cfg)
            indexer.commit()
            logger.info("Final Solr commit sent")

        end_dt = datetime.now().astimezone()
        wall_elapsed = time.perf_counter() - start_wall
        cpu_elapsed = time.process_time() - start_cpu
        cpu_util_pct = (cpu_elapsed / wall_elapsed * 100.0) if wall_elapsed > 0 else 0.0
        peak_memory_mb = _get_peak_memory_mb()

        if peak_memory_mb is None:
            logger.info(
                "Done | start=%s end=%s wall=%s cpu=%s cpu_util=%.1f%% peak_rss_mb=unavailable",
                start_dt.isoformat(timespec="seconds"),
                end_dt.isoformat(timespec="seconds"),
                _format_duration(wall_elapsed),
                _format_duration(cpu_elapsed),
                cpu_util_pct,
            )
        else:
            logger.info(
                "Done | start=%s end=%s wall=%s cpu=%s cpu_util=%.1f%% peak_rss_mb=%.1f",
                start_dt.isoformat(timespec="seconds"),
                end_dt.isoformat(timespec="seconds"),
                _format_duration(wall_elapsed),
                _format_duration(cpu_elapsed),
                cpu_util_pct,
                peak_memory_mb,
            )
            sys.exit(exit_code)
    except Exception as exc:
        # args may not always be defined if exception occurs during arg parsing
        # but it should be defined for most cases since parse_arguments is first
        error_msg = _format_error_message(exc, args if "args" in locals() else None)
        logger.error("%s", error_msg)
        sys.exit(EXIT_FAILURE)


def _format_duration(seconds):
    """Format duration as HH:MM:SS."""
    return time.strftime("%H:%M:%S", time.gmtime(max(0.0, seconds)))


def _get_peak_memory_mb():
    """Return peak process memory (RSS) in MB when available."""
    try:
        import resource

        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports KB, macOS reports bytes.
        if sys.platform == "darwin":
            return peak_rss / (1024 * 1024)
        return peak_rss / 1024
    except Exception:
        return None


def _determine_exit_code(failure_tracker):
    """Return CLI exit code based on tracked failures and warnings."""
    if failure_tracker.failures:
        return EXIT_FAILURE
    if failure_tracker.warnings:
        return EXIT_WARNINGS
    return EXIT_SUCCESS


def _split_files_for_processes(files, process_count):
    """Split files into near-even shards using round-robin assignment."""
    if process_count <= 1 or len(files) <= 1:
        return [files]

    shards = [[] for _ in range(min(process_count, len(files)))]
    for idx, file_path in enumerate(files):
        shards[idx % len(shards)].append(file_path)
    return [shard for shard in shards if shard]


def _bulkindex_worker(
    worker_id,
    shard_files,
    solr_url,
    cfg,
    workers,
    chunksize,
    thumbnails_enabled,
    result_queue,
):
    """Run one BulkIndexer instance inside a dedicated process."""
    try:
        authentication = _resolve_authentication(cfg)
        solr_client = pysolr.Solr(solr_url, always_commit=False, timeout=1020, auth=authentication)

        # Warm up XSD schema cache in this process if validation is enabled.
        # This eliminates per-document schema compilation overhead.
        xsd_path = cfg.get("xsd_path")
        if xsd_path:
            try:
                _get_cached_schema(xsd_path)
                logger.debug("XSD schema preloaded for worker %d", worker_id)
            except Exception as e:
                # Non-fatal: schema will be loaded on-demand if warm-up fails
                logger.warning("Schema warm-up failed for worker %d: %s", worker_id, e)

        bulk = BulkIndexer(
            shard_files,
            solr_url,
            threads=workers,
            chunksize=chunksize,
            auth=authentication,
            tflg=thumbnails_enabled,
            solr_client=solr_client,
            config=cfg,
        )
        result = bulk.bulkindex(shard_files)
        if not result or len(result) < 5:
            result_queue.put(
                {
                    "worker_id": worker_id,
                    "ok": False,
                    "error": "BulkIndexer returned no result tuple",
                }
            )
            return

        failure_tracker = result[4]
        result_queue.put(
            {
                "worker_id": worker_id,
                "ok": True,
                "parent_ids_referenced": list(result[0]),
                "docs_failed": result[1],
                "docs_indexed": result[2],
                "files_processed": result[3],
                "failures": [vars(f) for f in failure_tracker.failures],
                "warnings": [vars(w) for w in failure_tracker.warnings],
            }
        )
    except Exception as exc:
        result_queue.put(
            {
                "worker_id": worker_id,
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )


def _resolve_referenced_parents(solr_url, authentication, parent_ids_referenced):
    """Resolve referenced parent IDs after indexing completes."""
    if not parent_ids_referenced:
        return None

    try:
        solr_client = pysolr.Solr(solr_url, always_commit=False, timeout=1020, auth=authentication)
    except Exception as exc:
        logger.warning("Could not reinitialize Solr client for final parent pass: %s", exc)
        return set(parent_ids_referenced)

    return resolve_parent_ids(parent_ids_referenced, solr_client=solr_client)


def _report_parent_integrity(parent_ids_referenced, unresolved_parent_ids, failure_tracker):
    """Log final parent/child integrity summary and warnings."""
    if not parent_ids_referenced:
        return

    logger.info(" --- Parent/child integrity summary --- ")
    logger.info("Parents referenced: %d", len(parent_ids_referenced))

    if not unresolved_parent_ids:
        logger.info("All referenced parents resolved.")
        return

    logger.warning("Unresolved parent IDs referenced by child documents:")
    for parent_id in sorted(unresolved_parent_ids):
        logger.warning("  %s", parent_id)
        failure_tracker.add_warning(
            filename="unknown",
            warning_message="Referenced parent not found or could not be updated in Solr",
            warning_stage="parent_integrity",
            metadata_identifier=parent_id,
        )


def _run_bulkindex_multiprocess(
    files,
    process_count,
    solr_url,
    cfg,
    workers,
    chunksize,
    thumbnails_enabled,
):
    """Run multiple BulkIndexer instances in parallel processes and merge results."""
    shards = _split_files_for_processes(files, process_count)
    if len(shards) <= 1:
        return None

    logger.info(
        "Multi-process mode enabled: processes=%d shards=%d",
        process_count,
        len(shards),
    )

    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()
    processes = []

    for worker_id, shard in enumerate(shards, start=1):
        proc = ctx.Process(
            target=_bulkindex_worker,
            args=(
                worker_id,
                shard,
                solr_url,
                cfg,
                workers,
                chunksize,
                thumbnails_enabled,
                result_queue,
            ),
            name=f"BulkIndexerProcess-{worker_id}",
        )
        proc.start()
        processes.append(proc)

    worker_payloads = []
    for _ in processes:
        worker_payloads.append(result_queue.get())

    for proc in processes:
        proc.join()

    merged_failure_tracker = FailureTracker()
    parent_ids_referenced = set()
    docs_failed = 0
    docs_indexed = 0
    files_processed = 0

    worker_errors = []
    for payload in worker_payloads:
        if not payload.get("ok"):
            worker_errors.append(
                f"worker {payload.get('worker_id', '?')}: {payload.get('error', 'unknown error')}"
            )
            continue

        parent_ids_referenced.update(payload.get("parent_ids_referenced", []))
        docs_failed += int(payload.get("docs_failed", 0))
        docs_indexed += int(payload.get("docs_indexed", 0))
        files_processed += int(payload.get("files_processed", 0))

        for failure in payload.get("failures", []):
            merged_failure_tracker.add_failure(
                filename=failure.get("filename", "unknown"),
                error_message=failure.get("error_message", ""),
                error_stage=failure.get("error_stage", ""),
                metadata_identifier=failure.get("metadata_identifier"),
            )

        for warning in payload.get("warnings", []):
            merged_failure_tracker.add_warning(
                filename=warning.get("filename", "unknown"),
                warning_message=warning.get("warning_message", ""),
                warning_stage=warning.get("warning_stage", ""),
                metadata_identifier=warning.get("metadata_identifier"),
            )

    if worker_errors:
        raise RuntimeError("Multi-process bulkindex failed: " + "; ".join(worker_errors))

    return (
        parent_ids_referenced,
        docs_failed,
        docs_indexed,
        files_processed,
        merged_failure_tracker,
    )


def _resolve_authentication(cfg):
    if "auth-basic-username" in cfg and "auth-basic-password" in cfg:
        username = cfg["auth-basic-username"]
        password = cfg["auth-basic-password"]
        if username and password:
            return HTTPBasicAuth(username, password)
        raise ValueError("Configured auth-basic-username/auth-basic-password cannot be empty")

    dotenv_path = cfg.get("dotenv_path")
    if dotenv_path:
        if not os.path.exists(dotenv_path):
            raise FileNotFoundError(f"The file {dotenv_path} does not exist")
        load_dotenv(dotenv_path)
    else:
        load_dotenv()

    username = os.getenv("SOLR_USERNAME", "")
    password = os.getenv("SOLR_PASSWORD", "")
    if username and password:
        return HTTPBasicAuth(username, password)
    return None


def _resolve_input_files(args):
    if args.input_file:
        return [args.input_file]

    if args.list_file:
        with open(args.list_file, encoding="utf-8") as handle:
            return [line.strip() for line in handle if line.strip()]

    if args.directory:
        files = []
        directory_path = Path(args.directory)

        if args.recursive:
            for xml_file in sorted(directory_path.rglob("*.xml")):
                if xml_file.is_file():
                    files.append(str(xml_file))
        else:
            for name in os.listdir(args.directory):
                if name.lower().endswith(".xml"):
                    files.append(os.path.join(args.directory, name))
            files.sort()

        return files

    return []


def _resolve_thumbnail_flags(args, cfg):
    if args.no_thumbnail:
        return False
    enabled = bool(args.thumbnail or cfg.get("tflg", False))
    if enabled and cfg.get("scope") not in {"NBS", "ADC"}:
        logger.warning("Thumbnail generation is only supported for NBS or ADC scope")
        return False
    return enabled


def _build_solr_url(cfg):
    """Build and validate full Solr core URL from config.

    Requires `solrserver` to end with `/` so that concatenating
    `solrcore` yields a valid core URL.
    """
    solr_server = str(cfg.get("solrserver", "")).strip()
    solr_core = str(cfg.get("solrcore", "")).strip()

    if not solr_server:
        raise ValueError("Missing required config key 'solrserver'.")
    if not solr_core:
        raise ValueError("Missing required config key 'solrcore'.")

    if not solr_server.endswith("/"):
        raise ValueError(
            "Invalid 'solrserver' in config: missing trailing '/'. "
            "Use a value like 'http://localhost:8983/solr/'."
        )

    solr_url = solr_server + solr_core
    parsed = urlparse(solr_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(
            f"Invalid Solr URL built from config keys 'solrserver' + 'solrcore': '{solr_url}'."
        )

    return solr_url


def _format_error_message(exc, args):
    """Format error messages with context for common failures."""
    exc_type = type(exc).__name__
    exc_str = str(exc)

    if isinstance(exc, FileNotFoundError):
        # Check if it's an input file error
        if args and args.input_file and args.input_file in exc_str:
            return f"Input file not found: {args.input_file}\nPlease check that the file exists and the path is correct."
        if args and args.list_file and args.list_file in exc_str:
            return f"List file not found: {args.list_file}\nPlease check that the file exists and the path is correct."
        if args and args.directory and args.directory in exc_str:
            return f"Directory not found: {args.directory}\nPlease check that the directory exists and the path is correct."
        # Generic file not found
        return f"File not found: {exc_str}\nPlease check that the path is correct and the file exists."

    if isinstance(exc, ValueError):
        return f"Configuration error: {exc_str}"

    if isinstance(exc, ConnectionError):
        return f"Failed to connect to Solr:\n{exc_str}\nPlease check the Solr server URL in the configuration."

    if "Connection refused" in exc_str or "Failed to connect" in exc_str:
        return f"Cannot connect to Solr server:\n{exc_str}\nPlease ensure the Solr server is running and accessible."

    # Generic error with type info
    return f"Indexing failed ({exc_type}): {exc_str}"


if __name__ == "__main__":
    main()  # type: ignore[no-untyped-call]
