"""
Vocabulary Management for MMD Metadata Validation
==================================================

This module provides vocabulary loading and validation for MMD metadata.
It supports two backends:
1. Native TTL parser (rdflib) - efficient, single-load approach
2. Legacy metvocab (optional fallback) - dynamically imported when configured

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

import hashlib
import logging
import os
import pickle
import tempfile
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import requests

try:
    import rdflib
    from rdflib.namespace import SKOS

    RDFLIB_AVAILABLE = True
except ImportError:
    RDFLIB_AVAILABLE = False

logger = logging.getLogger(__name__)


class VocabularyBackend(ABC):
    """Abstract base class for vocabulary backends."""

    @abstractmethod
    def search(self, vocab_uri: str, value: str) -> bool:
        """
        Search for a value in a controlled vocabulary.

        Args:
            vocab_uri: URI of the vocabulary (e.g., "https://vocab.met.no/mmd/ISO_Topic_Category")
            value: The value to search for

        Returns:
            True if value is found in the vocabulary, False otherwise
        """
        pass

    @abstractmethod
    def get_concepts(self, vocab_uri: str) -> set[str]:
        """
        Get all valid concept labels for a vocabulary.

        Args:
            vocab_uri: URI of the vocabulary

        Returns:
            Set of valid concept labels
        """
        pass


class VocabularyLoader(VocabularyBackend):
    """TTL/RDF vocabulary loader using rdflib.

    Loads a Turtle file once at initialization and provides efficient
    vocabulary validation for MMD metadata.
    """

    def __init__(self, ttl_path: str):
        """
        Initialize vocabulary loader from TTL file.

        Args:
            ttl_path: Path to the TTL (Turtle RDF) vocabulary file

        Raises:
            ValueError: If rdflib is not available or file not found
            Exception: If TTL parsing fails
        """
        if not RDFLIB_AVAILABLE:
            raise ValueError(
                "rdflib is required for native TTL vocabulary loading. "
                "Install it with: pip install rdflib"
            )

        ttl_path_obj = Path(ttl_path)
        if not ttl_path_obj.exists():
            raise FileNotFoundError(f"TTL vocabulary file not found: {ttl_path}")

        logger.debug("Loading TTL vocabulary from %s", ttl_path)
        self.graph = rdflib.Graph()
        try:
            self.graph.parse(str(ttl_path), format="turtle")
        except Exception as e:
            logger.error("Failed to parse TTL file %s: %s", ttl_path, e)
            raise

        # Build cache: vocab_uri -> set of preferred labels
        self._cache: dict[str, set[str]] = {}
        self._build_cache()
        logger.info("Loaded %d vocabulary collections", len(self._cache))

    def _build_cache(self) -> None:
        """Build vocabulary cache from RDF graph.

        Extracts all SKOS collections and their member concepts,
        building a map of vocabulary URI -> set of valid labels.
        """
        # Query for all SKOS Collections
        query = """
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT ?collection ?concept ?label
            WHERE {
                ?collection a skos:Collection ;
                            skos:member ?concept .
                ?concept skos:prefLabel ?label .
            }
        """
        try:
            results = self.graph.query(query)
            for row in results:
                collection_uri = str(row.collection)
                label = str(row.label)

                if collection_uri not in self._cache:
                    self._cache[collection_uri] = set()

                # Add both the exact label and handle language tags
                label_plain = label.split("@")[0] if "@" in label else label
                self._cache[collection_uri].add(label_plain)

                # logger.debug(f"Added label '{label_plain}' to {collection_uri}")

        except Exception as e:
            logger.warning("Failed to query RDF graph: %s", e)

    def search(self, vocab_uri: str, value: str) -> bool:
        """
        Search for a value in a controlled vocabulary.

        Args:
            vocab_uri: URI of the vocabulary collection
            value: The value to search for

        Returns:
            True if value is found in the vocabulary, False otherwise
        """
        if vocab_uri not in self._cache:
            logger.warning("Vocabulary URI not found: %s", vocab_uri)
            return False

        return value in self._cache[vocab_uri]

    def get_concepts(self, vocab_uri: str) -> set[str]:
        """
        Get all valid concept labels for a vocabulary.

        Args:
            vocab_uri: URI of the vocabulary collection

        Returns:
            Set of valid concept labels, or empty set if not found
        """
        return self._cache.get(vocab_uri, set())


class VocabularyLegacy(VocabularyBackend):
    """Legacy metvocab wrapper for backwards compatibility.

    Uses metvocab.MMDGroup dynamically imported to avoid hard dependency.
    """

    @staticmethod
    def _import_mmdgroup() -> object:
        """Helper method to import MMDGroup. Can be mocked in tests."""
        from metvocab.mmdgroup import MMDGroup

        return MMDGroup

    def __init__(self) -> None:
        """Initialize legacy metvocab backend.

        Raises:
            ImportError: If metvocab is not installed
        """
        try:
            self.MMDGroup: Any = self._import_mmdgroup()
            logger.info("Using legacy metvocab backend for vocabulary validation")
        except ImportError:
            logger.error(
                "metvocab is not installed. "
                "Install it with: pip install 'metvocab @ git+https://github.com/metno/met-vocab-tools@v1.2.0'"
            )
            raise

        self._groups_cache: dict[str, Any] = {}

    def search(self, vocab_uri: str, value: str) -> bool:
        """
        Search for a value using metvocab.

        Args:
            vocab_uri: URI of the vocabulary (e.g., "https://vocab.met.no/mmd/ISO_Topic_Category")
            value: The value to search for

        Returns:
            True if value is found via metvocab, False otherwise
        """
        try:
            # Instantiate MMDGroup for this vocabulary (caching might help here)
            if vocab_uri not in self._groups_cache:
                self._groups_cache[vocab_uri] = self.MMDGroup("mmd", vocab_uri)

            group = self._groups_cache[vocab_uri]
            return bool(group.search(value))
        except Exception as e:
            logger.warning(
                "metvocab search failed for %s with value '%s': %s",
                vocab_uri,
                value,
                e,
            )
            return False

    def get_concepts(self, vocab_uri: str) -> set[str]:
        """
        Get concepts from metvocab (minimal implementation).

        Note: metvocab doesn't provide a direct way to get all concepts,
        so this returns an empty set. Legacy mode is best effort.

        Args:
            vocab_uri: URI of the vocabulary

        Returns:
            Empty set (not implemented for legacy mode)
        """
        logger.debug("get_concepts() not fully implemented for legacy metvocab backend")
        return set()


class VocabularyRestSkosmos(VocabularyBackend):
    """Skosmos REST-backed vocabulary loader.

    Fetches Turtle for each vocabulary URI on first use, persists it as a
    pickle file in the system temp directory, and refreshes only when the
    cache entry is older than ``cache_ttl`` seconds (default: 86400 = 24 h).
    """

    _CACHE_DIR_NAME = "solrindexer_vocab"
    _CACHE_VERSION = 1  # bump to invalidate all on-disk entries after schema changes

    def __init__(
        self,
        endpoint_base_url: str,
        endpoint_timeout: float = 20.0,
        cache_ttl: float = 86400.0,
        cache_dir: Optional[str] = None,
    ):
        if not RDFLIB_AVAILABLE:
            raise ValueError(
                "rdflib is required for REST Skosmos vocabulary loading. "
                "Install it with: pip install rdflib"
            )

        self.endpoint_base_url = endpoint_base_url.rstrip("/")
        self.endpoint_timeout = endpoint_timeout
        self.cache_ttl = cache_ttl
        self._mem_cache: dict[str, set[str]] = {}

        # Resolve persistent cache directory.
        base = cache_dir or os.path.join(tempfile.gettempdir(), self._CACHE_DIR_NAME)
        self._cache_dir = Path(base)
        try:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.warning(
                "Could not create vocab cache dir %s: %s — disk cache disabled", base, exc
            )
            self._cache_dir = None  # type: ignore[assignment]

        parsed = urlparse(self.endpoint_base_url)
        root = (
            f"{parsed.scheme}://{parsed.netloc}"
            if parsed.scheme and parsed.netloc
            else self.endpoint_base_url
        )
        logger.debug(
            "REST Skosmos backend active; endpoint=%s/rest/v1/<vocab>/data timeout=%ss cache_ttl=%ss cache_dir=%s",
            root,
            self.endpoint_timeout,
            self.cache_ttl,
            self._cache_dir,
        )

    @staticmethod
    def _vocab_name_from_uri(vocab_uri: str) -> str:
        parsed = urlparse(vocab_uri)
        path_parts = [part for part in parsed.path.split("/") if part]
        if not path_parts:
            raise ValueError(f"Could not extract vocabulary name from URI: {vocab_uri}")
        # For URIs like https://vocab.met.no/mmd/ISO_Topic_Category, use "mmd".
        return path_parts[0]

    def _api_base_for_vocab(self, vocab_name: str) -> str:
        """Normalize configured endpoint to a Skosmos REST API base.

        Accepts values like:
        - https://vocab.met.no
        - https://vocab.met.no/mmd
        - https://vocab.met.no/rest/v1
        - https://vocab.met.no/rest/v1/mmd
        """
        base = self.endpoint_base_url.rstrip("/")

        if "/rest/v1/" in base:
            if base.endswith(f"/rest/v1/{vocab_name}"):
                return base
            if base.endswith("/rest/v1"):
                return f"{base}/{vocab_name}"
            return base

        parsed = urlparse(base)
        root = f"{parsed.scheme}://{parsed.netloc}"
        return f"{root}/rest/v1/{vocab_name}"

    def _cache_path(self, vocab_uri: str) -> Optional[Path]:
        """Return the pickle file path for *vocab_uri*, or None if caching is disabled."""
        if self._cache_dir is None:
            return None
        digest = hashlib.sha256(vocab_uri.encode()).hexdigest()[:24]
        return self._cache_dir / f"v{self._CACHE_VERSION}_{digest}.pkl"

    def _read_disk_cache(self, vocab_uri: str) -> Optional[set[str]]:
        """Return cached labels from disk if still fresh, else None."""
        path = self._cache_path(vocab_uri)
        if path is None or not path.exists():
            return None
        try:
            with path.open("rb") as fh:
                entry = pickle.load(fh)
            if entry.get("version") != self._CACHE_VERSION:
                return None
            age = time.time() - entry["fetched_at"]
            if age >= self.cache_ttl:
                logger.debug(
                    "Disk cache stale (%.0fs old, ttl=%.0fs): %s", age, self.cache_ttl, vocab_uri
                )
                return None
            labels: set[str] = entry["labels"]
            logger.debug(
                "Disk cache hit (%.0fs old): %d labels for %s", age, len(labels), vocab_uri
            )
            return labels
        except Exception as exc:
            logger.warning("Corrupt vocab cache file %s (%s) — will refetch", path, exc)
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            return None

    def _write_disk_cache(self, vocab_uri: str, labels: set[str]) -> None:
        """Persist *labels* to disk as a versioned pickle entry."""
        path = self._cache_path(vocab_uri)
        if path is None:
            return
        entry = {"version": self._CACHE_VERSION, "fetched_at": time.time(), "labels": labels}
        try:
            tmp = path.with_suffix(".tmp")
            with tmp.open("wb") as fh:
                pickle.dump(entry, fh, protocol=pickle.HIGHEST_PROTOCOL)
            tmp.replace(path)
            logger.debug("Vocab cache written: %s (%d labels)", path.name, len(labels))
        except Exception as exc:
            logger.warning("Could not write vocab cache %s: %s", path, exc)

    def _fetch_from_rest(self, vocab_uri: str) -> set[str]:
        """Fetch vocabulary labels from the Skosmos REST endpoint."""
        vocab_name = self._vocab_name_from_uri(vocab_uri)
        api_base = self._api_base_for_vocab(vocab_name)
        endpoint = f"{api_base}/data"
        logger.debug("Fetching vocabulary %s from %s", vocab_uri, endpoint)

        response = requests.get(
            endpoint,
            params={"uri": vocab_uri, "format": "text/turtle"},
            timeout=self.endpoint_timeout,
        )
        response.raise_for_status()

        graph = rdflib.Graph()
        graph.parse(data=response.text, format="turtle")

        labels = {
            str(label) for _, _, label in graph.triples((None, SKOS.prefLabel, None)) if str(label)
        }
        logger.debug("Fetched %d labels for vocabulary %s", len(labels), vocab_uri)
        return labels

    def search(self, vocab_uri: str, value: str) -> bool:
        concepts = self.get_concepts(vocab_uri)
        return value in concepts

    def get_concepts(self, vocab_uri: str) -> set[str]:
        # 1. In-memory cache (fastest).
        if vocab_uri in self._mem_cache:
            return self._mem_cache[vocab_uri]

        # 2. Disk cache (warm start across processes/restarts).
        cached = self._read_disk_cache(vocab_uri)
        if cached is not None:
            self._mem_cache[vocab_uri] = cached
            return cached

        # 3. Fetch from REST endpoint.
        try:
            labels = self._fetch_from_rest(vocab_uri)
        except Exception as e:
            logger.warning("REST vocabulary lookup failed for %s: %s", vocab_uri, e)
            labels = set()

        self._mem_cache[vocab_uri] = labels
        if labels:  # don't cache failed/empty results to disk
            self._write_disk_cache(vocab_uri, labels)
        return labels


def create_vocabulary_loader(
    ttl_path: Optional[str] = None,
    backend: str = "native",
    endpoint_base_url: str = "https://vocab.met.no/mmd",
    endpoint_timeout: float = 20.0,
    cache_ttl: float = 86400.0,
    cache_dir: Optional[str] = None,
) -> Optional[VocabularyBackend]:
    """
    Factory function to create appropriate vocabulary backend.

    Args:
        ttl_path: Path to TTL vocabulary file (required for native backend)
        backend: Backend to use ("native", "legacy-metvocab", or "rest-skosmos")
        endpoint_base_url: Base URL for Skosmos endpoint (used by rest-skosmos)
        endpoint_timeout: HTTP timeout (seconds) for Skosmos requests
        cache_ttl: Seconds before on-disk cache entry expires (default 86400 = 24 h)
        cache_dir: Override the temp directory used for disk caching (default: system temp)

    Returns:
        VocabularyBackend instance, or None if TTL not configured and backend is native

    Raises:
        ValueError: If backend is invalid or required parameters are missing
    """
    if backend == "legacy-metvocab":
        logger.info("Creating legacy metvocab vocabulary backend")
        return VocabularyLegacy()

    if backend == "native":
        if ttl_path is None:
            logger.info(
                "No TTL path configured; falling back to REST Skosmos backend at %s",
                endpoint_base_url,
            )
            return VocabularyRestSkosmos(
                endpoint_base_url=endpoint_base_url,
                endpoint_timeout=endpoint_timeout,
                cache_ttl=cache_ttl,
                cache_dir=cache_dir,
            )

        logger.info("Creating native TTL vocabulary backend from %s", ttl_path)
        return VocabularyLoader(ttl_path)

    if backend == "rest-skosmos":
        logger.info("Creating REST Skosmos vocabulary backend from %s", endpoint_base_url)
        return VocabularyRestSkosmos(
            endpoint_base_url=endpoint_base_url,
            endpoint_timeout=endpoint_timeout,
            cache_ttl=cache_ttl,
            cache_dir=cache_dir,
        )

    raise ValueError(
        f"Unknown vocabulary backend: {backend}. "
        "Valid options: 'native', 'legacy-metvocab', 'rest-skosmos'"
    )
