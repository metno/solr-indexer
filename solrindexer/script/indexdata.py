#!/usr/bin/env python3
"""Unified indexdata CLI for single and bulk MMD ingestion."""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pysolr
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from solrindexer.bulkindexer import BulkIndexer
from solrindexer.indexdata import IndexMMD
from solrindexer.script.searchindex import parse_cfg
from solrindexer.tools import initSolr, solr_ping, to_solr_id

logger = logging.getLogger(__name__)

DEFAULT_THREADS = 20
DEFAULT_CHUNKSIZE = 2500


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


def parse_arguments():
    parser = argparse.ArgumentParser(description="Index MMD XML files into Solr")
    parser.add_argument("-c", "--cfg", dest="cfgfile", required=True, help="Configuration file")
    parser.add_argument("-a", "--always_commit", action="store_true", help="Always commit to Solr")
    parser.add_argument("-i", "--input_file", help="Individual file to ingest")
    parser.add_argument("-l", "--list_file", help="File containing xml file paths to ingest")
    parser.add_argument("-d", "--directory", help="Directory with xml files to ingest")
    parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="Recursively search for XML files in directory and subdirectories (requires -d)"
    )
    parser.add_argument(
        "-parent",
        "--mark_parent",
        required=False,
        help="Metadata identifier of existing Solr document to mark as parent",
    )

    parser.add_argument("--threads", type=int, default=None, help="Number of worker threads")
    parser.add_argument("--chunksize", type=int, default=None, help="Batch size for bulk indexing")

    parser.add_argument("-t", "--thumbnail", action="store_true", help="Enable thumbnail indexing")
    parser.add_argument("-n", "--no_thumbnail", action="store_true", help="Disable thumbnail indexing")
    parser.add_argument("-nbs", "--nbs", action="store_true", help="Enable NBS thumbnail mode")

    parser.add_argument(
        "--vocabulary-ttl-path",
        help="Path to TTL vocabulary file (overrides config)",
    )
    parser.add_argument(
        "--vocabulary-backend",
        choices=["native", "legacy-metvocab"],
        help="Vocabulary backend to use (overrides config)",
    )

    args = parser.parse_args()
    if not args.input_file and not args.list_file and not args.directory and not args.mark_parent:
        parser.print_help()
        parser.exit(2)
    return args


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
            # Use rglob for efficient recursive search of XML files
            for xml_file in sorted(directory_path.rglob("*.xml")):
                if xml_file.is_file():
                    files.append(str(xml_file))
        else:
            # Non-recursive: only look in top-level directory
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
    if enabled and cfg.get("scope") != "NBS":
        logger.warning("Thumbnail generation is only supported for NBS scope in this version")
        return False
    return enabled


def main():
    start_dt = datetime.now().astimezone()
    start_wall = time.perf_counter()
    start_cpu = time.process_time()

    try:
        args = parse_arguments()
        try:
            cfg = parse_cfg(args.cfgfile)
        except (FileNotFoundError, ValueError) as e:
            logger.error("%s", str(e))
            sys.exit(1)

        if args.nbs:
            cfg["scope"] = "NBS"
        else:
            cfg["scope"] = cfg.get("scope")

        # Apply CLI overrides for vocabulary settings
        if args.vocabulary_ttl_path:
            cfg["vocabulary-ttl-path"] = args.vocabulary_ttl_path
        if args.vocabulary_backend:
            cfg["vocabulary-backend"] = args.vocabulary_backend

        solr_url = cfg["solrserver"] + cfg["solrcore"]
        authentication = _resolve_authentication(cfg)

        # BulkIndexer uses the shared tools.solr_add() client; initialize it here.
        initSolr(
            solr_url,
            pysolr.Solr(solr_url, always_commit=False, timeout=1020, auth=authentication),
            authentication,
        )
        logger.info("Solr connection establised: %s", solr_url)
        solr_ping()

        # Keep parent marking as a focused operation.
        if args.mark_parent:
            indexer = IndexMMD(solr_url, args.always_commit, authentication, cfg)
            status, msg = indexer.update_parent(to_solr_id(args.mark_parent.strip()))
            logger.info("Parent update status=%s message=%s", status, msg)
            sys.exit(int(status))

        files = _resolve_input_files(args)
        if not files:
            raise ValueError("No input files found")

        configured_threads = args.threads if args.threads is not None else int(cfg.get("threads", DEFAULT_THREADS))
        # Use single worker for single file input, multiple workers for batch inputs
        if args.input_file:
            workers = 1
            logger.debug("Single file input: using 1 worker (sequential processing)")
        else:
            workers = configured_threads
            logger.debug(f"Multiple file input: using {workers} workers (concurrent processing)")
        chunksize = args.chunksize if args.chunksize is not None else int(cfg.get("batch-size", DEFAULT_CHUNKSIZE))

        thumbnails_enabled = _resolve_thumbnail_flags(args, cfg)
        # Thumbnail generation is handled only for NBS scope.
        thumb_class = None

        logger.info(
            "Starting indexing with files=%d workers=%d chunksize=%d thumbnails=%s",
            len(files),
            workers,
            chunksize,
            thumbnails_enabled,
        )

        bulk = BulkIndexer(
            files,
            solr_url,
            threads=workers,
            chunksize=chunksize,
            auth=authentication,
            tflg=thumbnails_enabled,
            thumbClass=thumb_class,
            config=cfg,
        )
        result = bulk.bulkindex(files)
        logger.info("Processed %s files.", len(files))
        # Extract failure tracker from result tuple (8th element)
        if result and len(result) >= 8:
            failure_tracker = result[7]
            failure_tracker.log_summary()

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
    except Exception as exc:
        # args may not always be defined if exception occurs during arg parsing
        # but it should be defined for most cases since parse_arguments is first
        error_msg = _format_error_message(exc, args if 'args' in locals() else None)
        logger.error("%s", error_msg)
        sys.exit(1)


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
