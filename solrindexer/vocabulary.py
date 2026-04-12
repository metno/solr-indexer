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


def create_vocabulary_loader(
    ttl_path: str | None = None,
    backend: str = "native",
) -> VocabularyBackend | None:
    """
    Factory function to create appropriate vocabulary backend.

    Args:
        ttl_path: Path to TTL vocabulary file (required for native backend)
        backend: Backend to use ("native" or "legacy-metvocab")

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
            logger.warning("No TTL path configured; vocabulary validation will be skipped")
            return None

        logger.info(f"Creating native TTL vocabulary backend from {ttl_path}")
        return VocabularyLoader(ttl_path)

    raise ValueError(
        f"Unknown vocabulary backend: {backend}. "
        "Valid options: 'native', 'legacy-metvocab'"
    )
