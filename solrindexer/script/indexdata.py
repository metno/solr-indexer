#!/usr/bin/env python3
"""Unified indexdata CLI for single and bulk MMD ingestion."""

import argparse
import logging
import os
import sys

import pysolr
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from solrindexer.bulkindexer import BulkIndexer
from solrindexer.indexdata import IndexMMD
from solrindexer.script.searchindex import parse_cfg
from solrindexer.tools import initSolr, solr_ping, to_solr_id

logger = logging.getLogger(__name__)

DEFAULT_BULK_THRESHOLD = 500
DEFAULT_THREADS = 20
DEFAULT_CHUNKSIZE = 2500


def parse_arguments():
    parser = argparse.ArgumentParser(description="Index MMD XML files into Solr")
    parser.add_argument("-c", "--cfg", dest="cfgfile", required=True, help="Configuration file")
    parser.add_argument("-a", "--always_commit", action="store_true", help="Always commit to Solr")
    parser.add_argument("-i", "--input_file", help="Individual file to ingest")
    parser.add_argument("-l", "--list_file", help="File containing xml file paths to ingest")
    parser.add_argument("-d", "--directory", help="Directory with xml files to ingest")
    parser.add_argument(
        "-parent",
        "--mark_parent",
        required=False,
        help="Metadata identifier of existing Solr document to mark as parent",
    )

    parser.add_argument(
        "--bulk-threshold",
        type=int,
        default=None,
        help="Use single-worker mode at or below this file count",
    )
    parser.add_argument("--threads", type=int, default=None, help="Number of worker threads")
    parser.add_argument("--chunksize", type=int, default=None, help="Batch size for bulk indexing")

    parser.add_argument("-t", "--thumbnail", action="store_true", help="Enable thumbnail indexing")
    parser.add_argument("-n", "--no_thumbnail", action="store_true", help="Disable thumbnail indexing")
    parser.add_argument("-nbs", "--nbs", action="store_true", help="Enable NBS thumbnail mode")

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
        for name in os.listdir(args.directory):
            if name.lower().endswith(".xml"):
                files.append(os.path.join(args.directory, name))
        return sorted(files)

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
    try:
        args = parse_arguments()
        cfg = parse_cfg(args.cfgfile)

        if args.nbs:
            cfg["scope"] = "NBS"
        else:
            cfg["scope"] = cfg.get("scope")

        solr_url = cfg["solrserver"] + cfg["solrcore"]
        authentication = _resolve_authentication(cfg)

        # BulkIndexer uses the shared tools.solr_add() client; initialize it here.
        initSolr(
            solr_url,
            pysolr.Solr(solr_url, always_commit=False, timeout=1020, auth=authentication),
            authentication,
        )
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

        threshold = (
            args.bulk_threshold
            if args.bulk_threshold is not None
            else int(cfg.get("bulk-file-threshold", DEFAULT_BULK_THRESHOLD))
        )
        configured_threads = args.threads if args.threads is not None else int(cfg.get("threads", DEFAULT_THREADS))
        workers = 1 if len(files) <= threshold else configured_threads
        chunksize = args.chunksize if args.chunksize is not None else int(cfg.get("batch-size", DEFAULT_CHUNKSIZE))

        thumbnails_enabled = _resolve_thumbnail_flags(args, cfg)
        # Thumbnail generation is handled only for NBS scope.
        thumb_class = None

        logger.info(
            "Starting indexing with files=%d threshold=%d workers=%d chunksize=%d thumbnails=%s",
            len(files),
            threshold,
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

        # Extract failure tracker from result tuple (8th element)
        if result and len(result) >= 8:
            failure_tracker = result[7]
            failure_tracker.log_summary()

        if cfg.get("end-solr-commit", False):
            indexer = IndexMMD(solr_url, args.always_commit, authentication, cfg)
            indexer.commit()
            logger.info("Final Solr commit sent")

        logger.info("Done")
    except Exception as exc:
        logger.error("Indexing failed: %s", exc)
        sys.exit(1)


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
