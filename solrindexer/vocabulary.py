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

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse

import requests

try:
    import rdflib
    from rdflib.namespace import RDF, RDFS, SKOS
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

        logger.debug(f"Loading TTL vocabulary from {ttl_path}")
        self.graph = rdflib.Graph()
        try:
            self.graph.parse(str(ttl_path), format="turtle")
        except Exception as e:
            logger.error(f"Failed to parse TTL file {ttl_path}: {e}")
            raise

        # Build cache: vocab_uri -> set of preferred labels
        self._cache: dict[str, set[str]] = {}
        self._build_cache()
        logger.info(f"Loaded {len(self._cache)} vocabulary collections")

    def _build_cache(self):
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
            logger.warning(f"Failed to query RDF graph: {e}")

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
            logger.warning(f"Vocabulary URI not found: {vocab_uri}")
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
    def _import_mmdgroup():
        """Helper method to import MMDGroup. Can be mocked in tests."""
        from metvocab.mmdgroup import MMDGroup
        return MMDGroup

    def __init__(self):
        """Initialize legacy metvocab backend.

        Raises:
            ImportError: If metvocab is not installed
        """
        try:
            self.MMDGroup = self._import_mmdgroup()
            logger.info("Using legacy metvocab backend for vocabulary validation")
        except ImportError:
            logger.error(
                "metvocab is not installed. "
                "Install it with: pip install 'metvocab @ git+https://github.com/metno/met-vocab-tools@v1.2.0'"
            )
            raise

        self._groups_cache: dict[str, MMDGroup] = {}  # noqa: F821

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
            logger.warning(f"metvocab search failed for {vocab_uri} with value '{value}': {e}")
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

    Fetches Turtle for each vocabulary URI on first use and caches labels in memory.
    """

    def __init__(self, endpoint_base_url: str, endpoint_timeout: float = 20.0):
        if not RDFLIB_AVAILABLE:
            raise ValueError(
                "rdflib is required for REST Skosmos vocabulary loading. "
                "Install it with: pip install rdflib"
            )

        self.endpoint_base_url = endpoint_base_url.rstrip("/")
        self.endpoint_timeout = endpoint_timeout
        self._cache: dict[str, set[str]] = {}

        parsed = urlparse(self.endpoint_base_url)
        root = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else self.endpoint_base_url
        logger.debug(
            "REST Skosmos backend active; normalized request pattern: %s/rest/v1/<vocab>/data?uri=<vocab_uri>&format=text/turtle (timeout=%ss)",
            root,
            self.endpoint_timeout,
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

    def _load_vocab(self, vocab_uri: str) -> set[str]:
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
            str(label)
            for _, _, label in graph.triples((None, SKOS.prefLabel, None))
            if str(label)
        }

        self._cache[vocab_uri] = labels
        logger.debug("Loaded %d labels for vocabulary %s", len(labels), vocab_uri)
        return labels

    def search(self, vocab_uri: str, value: str) -> bool:
        concepts = self.get_concepts(vocab_uri)
        return value in concepts

    def get_concepts(self, vocab_uri: str) -> set[str]:
        if vocab_uri in self._cache:
            return self._cache[vocab_uri]

        try:
            return self._load_vocab(vocab_uri)
        except Exception as e:
            logger.warning("REST vocabulary lookup failed for %s: %s", vocab_uri, e)
            self._cache[vocab_uri] = set()
            return self._cache[vocab_uri]


def create_vocabulary_loader(
    ttl_path: str | None = None,
    backend: str = "native",
    endpoint_base_url: str = "https://vocab.met.no/mmd",
    endpoint_timeout: float = 20.0,
) -> VocabularyBackend | None:
    """
    Factory function to create appropriate vocabulary backend.

    Args:
        ttl_path: Path to TTL vocabulary file (required for native backend)
        backend: Backend to use ("native", "legacy-metvocab", or "rest-skosmos")
        endpoint_base_url: Base URL for Skosmos endpoint (used by rest-skosmos)
        endpoint_timeout: HTTP timeout (seconds) for Skosmos requests

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
            )

        logger.info(f"Creating native TTL vocabulary backend from {ttl_path}")
        return VocabularyLoader(ttl_path)

    if backend == "rest-skosmos":
        logger.info("Creating REST Skosmos vocabulary backend from %s", endpoint_base_url)
        return VocabularyRestSkosmos(
            endpoint_base_url=endpoint_base_url,
            endpoint_timeout=endpoint_timeout,
        )

    raise ValueError(
        f"Unknown vocabulary backend: {backend}. "
        "Valid options: 'native', 'legacy-metvocab', 'rest-skosmos'"
    )
