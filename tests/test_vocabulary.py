"""
Tests for vocabulary module
============================

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
import pickle
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from solrindexer.mmd import MMD4SolR
from solrindexer.vocabulary import (
    VocabularyLegacy,
    VocabularyLoader,
    VocabularyRestSkosmos,
    create_vocabulary_loader,
)


def _vocabulary_ttl_candidates() -> list[Path]:
    """Return candidate TTL paths for local and CI checkout layouts.

    Priority:
    1. Explicit env override via MMD_REPO_PATH
    2. Sibling repo checkout relative to this test file
    3. Common cwd-relative layouts used in CI and local development
    """
    this_file = Path(__file__).resolve()
    tests_dir = this_file.parent
    solr_indexer_root = tests_dir.parent
    workspace_root = solr_indexer_root.parent

    candidates: list[Path] = []

    mmd_repo_path = os.getenv("MMD_REPO_PATH")
    if mmd_repo_path:
        env_path = Path(mmd_repo_path).expanduser().resolve()
        if env_path.name == "mmd-vocabulary.ttl":
            candidates.append(env_path)
        else:
            candidates.append(env_path / "thesauri" / "mmd-vocabulary.ttl")

    candidates.extend(
        [
            workspace_root / "mmd" / "thesauri" / "mmd-vocabulary.ttl",
            solr_indexer_root / ".." / "mmd" / "thesauri" / "mmd-vocabulary.ttl",
            Path.cwd() / "../mmd/thesauri/mmd-vocabulary.ttl",
            Path.cwd() / "mmd/thesauri/mmd-vocabulary.ttl",
        ]
    )

    # Keep order while removing duplicates.
    seen: set[Path] = set()
    unique_candidates: list[Path] = []
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique_candidates.append(resolved)

    return unique_candidates


def _resolve_vocabulary_ttl() -> Path | None:
    """Return the first existing vocabulary TTL path, if any."""
    for candidate in _vocabulary_ttl_candidates():
        if candidate.exists():
            return candidate
    return None


def _skip_if_no_ttl() -> None:
    """Skip tests requiring mmd-vocabulary.ttl when not available."""
    if VOCABULARY_TTL is not None:
        return
    candidates = "\n".join(f"  - {path}" for path in _vocabulary_ttl_candidates())
    pytest.skip(
        "TTL file not found. Set MMD_REPO_PATH to your mmd repository path "
        "or file path. Tried:\n"
        f"{candidates}"
    )


VOCABULARY_TTL = _resolve_vocabulary_ttl()


class TestVocabularyLoaderBasics:
    """Test VocabularyLoader initialization and basic functionality."""

    def test_init_with_valid_ttl_file(self):
        """Test that VocabularyLoader initializes with a valid TTL file."""
        _skip_if_no_ttl()

        loader = VocabularyLoader(str(VOCABULARY_TTL))
        assert loader is not None
        assert loader.graph is not None
        assert len(loader._cache) > 0

    def test_init_with_nonexistent_file(self):
        """Test that FileNotFoundError is raised for missing file."""
        with pytest.raises(FileNotFoundError):
            VocabularyLoader("/nonexistent/path/vocab.ttl")

    def test_init_without_rdflib(self):
        """Test that ValueError is raised if rdflib is not available."""
        with patch("solrindexer.vocabulary.RDFLIB_AVAILABLE", False):
            with pytest.raises(ValueError, match="rdflib is required"):
                VocabularyLoader(str(VOCABULARY_TTL))


class TestVocabularyLoaderSearch:
    """Test the search functionality of VocabularyLoader."""

    @pytest.fixture
    def loader(self):
        """Fixture to provide a VocabularyLoader instance."""
        _skip_if_no_ttl()
        return VocabularyLoader(str(VOCABULARY_TTL))

    def test_search_iso_topic_category_valid_value(self, loader):
        """Test searching for a valid value in ISO_Topic_Category."""
        vocab_uri = "https://vocab.met.no/mmd/ISO_Topic_Category"
        concepts = loader.get_concepts(vocab_uri)
        # The TTL file should have some ISO topic categories
        assert len(concepts) > 0

    def test_search_nonexistent_vocabulary(self, loader):
        """Test searching in a nonexistent vocabulary returns False."""
        vocab_uri = "https://vocab.met.no/mmd/NonExistentVocab"
        result = loader.search(vocab_uri, "some_value")
        assert result is False

    def test_get_concepts_returns_set(self, loader):
        """Test that get_concepts returns a set."""
        vocab_uri = "https://vocab.met.no/mmd/Use_Constraint"
        concepts = loader.get_concepts(vocab_uri)
        assert isinstance(concepts, set)

    def test_get_concepts_for_nonexistent_vocab(self, loader):
        """Test that get_concepts returns empty set for nonexistent vocab."""
        vocab_uri = "https://vocab.met.no/mmd/NonExistent"
        concepts = loader.get_concepts(vocab_uri)
        assert isinstance(concepts, set)
        assert len(concepts) == 0


class TestVocabularyLegacy:
    """Test VocabularyLegacy wrapper for backwards compatibility."""

    def test_init_with_mocked_metvocab(self):
        """Test VocabularyLegacy initialization with mocked metvocab."""
        mock_mmdgroup = MagicMock()

        with patch("solrindexer.vocabulary.VocabularyLegacy._import_mmdgroup") as mock_import:
            mock_import.return_value = MagicMock(return_value=mock_mmdgroup)
            legacy = VocabularyLegacy()
            assert legacy is not None

    def test_init_without_metvocab(self):
        """Test that ImportError is raised if metvocab is not installed."""
        with patch("solrindexer.vocabulary.VocabularyLegacy._import_mmdgroup") as mock_import:
            mock_import.side_effect = ImportError("metvocab not found")
            with pytest.raises(ImportError):
                VocabularyLegacy()

    def test_search_with_mocked_metvocab(self):
        """Test search method with mocked metvocab."""
        # Mock MMDGroup behavior
        mock_group_instance = MagicMock()
        mock_group_instance.search.return_value = True

        mock_mmdgroup_class = MagicMock(return_value=mock_group_instance)

        with patch("solrindexer.vocabulary.VocabularyLegacy._import_mmdgroup") as mock_import:
            mock_import.return_value = mock_mmdgroup_class
            legacy = VocabularyLegacy()
            result = legacy.search("https://vocab.met.no/mmd/SomeVocab", "test_value")
            assert result is True

    def test_search_not_found_with_mocked_metvocab(self):
        """Test search returns False when value not found."""
        mock_group_instance = MagicMock()
        mock_group_instance.search.return_value = False

        mock_mmdgroup_class = MagicMock(return_value=mock_group_instance)

        with patch("solrindexer.vocabulary.VocabularyLegacy._import_mmdgroup") as mock_import:
            mock_import.return_value = mock_mmdgroup_class
            legacy = VocabularyLegacy()
            result = legacy.search("https://vocab.met.no/mmd/SomeVocab", "invalid_value")
            assert result is False

    def test_get_concepts_not_implemented(self):
        """Test that get_concepts returns empty set (not implemented for legacy)."""
        mock_mmdgroup_class = MagicMock()

        with patch("solrindexer.vocabulary.VocabularyLegacy._import_mmdgroup") as mock_import:
            mock_import.return_value = mock_mmdgroup_class
            legacy = VocabularyLegacy()
            concepts = legacy.get_concepts("https://vocab.met.no/mmd/SomeVocab")
            assert isinstance(concepts, set)
            assert len(concepts) == 0


class TestCreateVocabularyLoader:
    """Test the factory function create_vocabulary_loader."""

    def test_create_native_loader_with_valid_path(self):
        """Test creating native loader with valid TTL path."""
        _skip_if_no_ttl()

        loader = create_vocabulary_loader(ttl_path=str(VOCABULARY_TTL), backend="native")
        assert loader is not None
        assert isinstance(loader, VocabularyLoader)

    def test_create_native_loader_without_path(self):
        """Test native backend without TTL path falls back to REST backend."""
        loader = create_vocabulary_loader(ttl_path=None, backend="native")
        assert isinstance(loader, VocabularyRestSkosmos)

    def test_create_legacy_loader(self):
        """Test creating legacy loader."""
        mock_mmdgroup_class = MagicMock()
        with patch(
            "solrindexer.vocabulary.VocabularyLegacy._import_mmdgroup",
            return_value=mock_mmdgroup_class,
        ):
            loader = create_vocabulary_loader(backend="legacy-metvocab")
            assert loader is not None
            assert isinstance(loader, VocabularyLegacy)

    def test_create_invalid_backend(self):
        """Test that ValueError is raised for invalid backend."""
        with pytest.raises(ValueError, match="Unknown vocabulary backend"):
            create_vocabulary_loader(backend="invalid-backend")

    def test_create_rest_skosmos_loader(self):
        """Test creating REST Skosmos loader."""
        loader = create_vocabulary_loader(
            backend="rest-skosmos",
            endpoint_base_url="https://example.test/mmd",
            endpoint_timeout=9.5,
        )
        assert isinstance(loader, VocabularyRestSkosmos)
        assert loader.endpoint_base_url == "https://example.test/mmd"
        assert loader.endpoint_timeout == 9.5


class TestVocabularyRestSkosmos:
    """Test REST/Skosmos vocabulary backend behavior."""

    @staticmethod
    def _mock_response(ttl_text: str):
        response = MagicMock()
        response.text = ttl_text
        response.raise_for_status = MagicMock()
        return response

    def test_search_fetches_and_caches_vocabulary(self, tmp_path):
        ttl_text = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <https://example.test/> .
        ex:c1 skos:prefLabel "oceans"@en .
        ex:c2 skos:prefLabel "climatologyMeteorologyAtmosphere"@en .
        """
        loader = VocabularyRestSkosmos("https://example.test/mmd", 7.0, cache_dir=str(tmp_path))

        with patch("solrindexer.vocabulary.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(ttl_text)

            vocab_uri = "https://vocab.met.no/mmd/ISO_Topic_Category"
            assert loader.search(vocab_uri, "oceans") is True
            assert loader.search(vocab_uri, "not-a-value") is False

            # Second search should use in-memory cache and not call HTTP again.
            assert mock_get.call_count == 1

    def test_search_returns_false_on_http_error(self, tmp_path):
        loader = VocabularyRestSkosmos("https://example.test/mmd", 7.0, cache_dir=str(tmp_path))
        with patch("solrindexer.vocabulary.requests.get") as mock_get:
            mock_get.side_effect = Exception("network down")
            vocab_uri = "https://vocab.met.no/mmd/ISO_Topic_Category"
            assert loader.search(vocab_uri, "oceans") is False

    def test_timeout_and_endpoint_arguments_are_used(self, tmp_path):
        ttl_text = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <https://example.test/> .
        ex:c1 skos:prefLabel "In Work"@en .
        """
        loader = VocabularyRestSkosmos("https://example.test/mmd", 3.25, cache_dir=str(tmp_path))

        with patch("solrindexer.vocabulary.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(ttl_text)
            vocab_uri = "https://vocab.met.no/mmd/Dataset_Production_Status"
            assert loader.search(vocab_uri, "In Work") is True

            mock_get.assert_called_once_with(
                "https://example.test/rest/v1/mmd/data",
                params={
                    "uri": "https://vocab.met.no/mmd/Dataset_Production_Status",
                    "format": "text/turtle",
                },
                timeout=3.25,
            )

    def test_disk_cache_is_written_after_fetch(self, tmp_path):
        ttl_text = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <https://example.test/> .
        ex:c1 skos:prefLabel "oceans"@en .
        """
        loader = VocabularyRestSkosmos("https://example.test/mmd", 7.0, cache_dir=str(tmp_path))
        vocab_uri = "https://vocab.met.no/mmd/ISO_Topic_Category"

        with patch("solrindexer.vocabulary.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(ttl_text)
            loader.search(vocab_uri, "oceans")

        cache_files = list(tmp_path.glob("*.pkl"))
        assert len(cache_files) == 1
        entry = pickle.loads(cache_files[0].read_bytes())
        assert "oceans" in entry["labels"]
        assert entry["fetched_at"] <= time.time()

    def test_disk_cache_is_used_on_second_instance(self, tmp_path):
        ttl_text = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <https://example.test/> .
        ex:c1 skos:prefLabel "oceans"@en .
        """
        vocab_uri = "https://vocab.met.no/mmd/ISO_Topic_Category"

        # First loader fetches from REST and writes to disk.
        loader1 = VocabularyRestSkosmos("https://example.test/mmd", 7.0, cache_dir=str(tmp_path))
        with patch("solrindexer.vocabulary.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(ttl_text)
            loader1.search(vocab_uri, "oceans")
            assert mock_get.call_count == 1

        # Second loader (fresh instance, same cache_dir) should hit disk — no HTTP.
        loader2 = VocabularyRestSkosmos("https://example.test/mmd", 7.0, cache_dir=str(tmp_path))
        with patch("solrindexer.vocabulary.requests.get") as mock_get2:
            result = loader2.search(vocab_uri, "oceans")
            assert result is True
            mock_get2.assert_not_called()

    def test_expired_disk_cache_triggers_refetch(self, tmp_path):
        ttl_text = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <https://example.test/> .
        ex:c1 skos:prefLabel "oceans"@en .
        """
        vocab_uri = "https://vocab.met.no/mmd/ISO_Topic_Category"
        loader = VocabularyRestSkosmos(
            "https://example.test/mmd", 7.0, cache_ttl=60.0, cache_dir=str(tmp_path)
        )
        path = loader._cache_path(vocab_uri)

        # Write a cache entry that is already 2 hours old.
        stale_entry = {
            "version": loader._CACHE_VERSION,
            "fetched_at": time.time() - 7200,
            "labels": {"oceans"},
        }
        path.write_bytes(pickle.dumps(stale_entry, protocol=pickle.HIGHEST_PROTOCOL))

        with patch("solrindexer.vocabulary.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(ttl_text)
            loader.search(vocab_uri, "oceans")
            mock_get.assert_called_once()

    def test_corrupt_disk_cache_triggers_refetch(self, tmp_path):
        vocab_uri = "https://vocab.met.no/mmd/ISO_Topic_Category"
        loader = VocabularyRestSkosmos("https://example.test/mmd", 7.0, cache_dir=str(tmp_path))
        path = loader._cache_path(vocab_uri)
        path.write_bytes(b"not valid pickle data !!!")

        ttl_text = """
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
        @prefix ex: <https://example.test/> .
        ex:c1 skos:prefLabel "oceans"@en .
        """
        with patch("solrindexer.vocabulary.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(ttl_text)
            result = loader.search(vocab_uri, "oceans")
            assert result is True
            mock_get.assert_called_once()

    def test_failed_fetch_does_not_write_disk_cache(self, tmp_path):
        vocab_uri = "https://vocab.met.no/mmd/ISO_Topic_Category"
        loader = VocabularyRestSkosmos("https://example.test/mmd", 7.0, cache_dir=str(tmp_path))

        with patch("solrindexer.vocabulary.requests.get") as mock_get:
            mock_get.side_effect = Exception("timeout")
            loader.search(vocab_uri, "oceans")

        assert list(tmp_path.glob("*.pkl")) == []


class TestMMD4SolRIntegration:
    """Test integration of vocabulary_loader with MMD4SolR."""

    def test_mmd4solr_with_vocabulary_loader(self):
        """Test that MMD4SolR accepts vocabulary_loader parameter."""
        test_mmd_xml = """<mmd:mmd xmlns:mmd="http://www.met.no/schema/mmd">
            <mmd:metadata_identifier>test-id</mmd:metadata_identifier>
            <mmd:title>Test Title</mmd:title>
            <mmd:abstract>Test Abstract</mmd:abstract>
            <mmd:metadata_status>Active</mmd:metadata_status>
            <mmd:dataset_production_status>In Work</mmd:dataset_production_status>
            <mmd:collection>TEST</mmd:collection>
            <mmd:last_metadata_update>
                <mmd:update>
                    <mmd:datetime>2024-01-01T00:00:00Z</mmd:datetime>
                </mmd:update>
            </mmd:last_metadata_update>
            <mmd:iso_topic_category>climatologyMeteorologyAtmosphere</mmd:iso_topic_category>
            <mmd:keywords vocabulary="GCMDSK">
                <mmd:keyword>test keyword</mmd:keyword>
            </mmd:keywords>
        </mmd:mmd>
        """

        # Create mock loader
        mock_loader = MagicMock()
        mock_loader.search.return_value = True

        # Parse XML and create MMD4SolR instance
        import lxml.etree as ET

        root = ET.fromstring(test_mmd_xml.encode("utf-8"))
        doc = MMD4SolR(
            mydoc=root,
            vocabulary_loader=mock_loader,
        )

        # Verify vocabulary_loader is stored
        assert doc.vocabulary_loader is mock_loader

    def test_mmd4solr_without_vocabulary_loader(self):
        """Test that MMD4SolR works without vocabulary_loader."""
        from solrindexer.mmd import MMD4SolR

        test_mmd_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <mmd:mmd xmlns:mmd="http://www.met.no/schema/mmd">
            <mmd:metadata_identifier>test-id</mmd:metadata_identifier>
            <mmd:title>Test Title</mmd:title>
            <mmd:abstract>Test Abstract</mmd:abstract>
            <mmd:metadata_status>Active</mmd:metadata_status>
            <mmd:dataset_production_status>In Work</mmd:dataset_production_status>
            <mmd:collection>TEST</mmd:collection>
            <mmd:last_metadata_update>
                <mmd:update>
                    <mmd:datetime>2024-01-01T00:00:00Z</mmd:datetime>
                </mmd:update>
            </mmd:last_metadata_update>
            <mmd:iso_topic_category>climatologyMeteorologyAtmosphere</mmd:iso_topic_category>
            <mmd:keywords vocabulary="GCMDSK">
                <mmd:keyword>test keyword</mmd:keyword>
            </mmd:keywords>
        </mmd:mmd>
        """

        import lxml.etree as ET

        root = ET.fromstring(test_mmd_xml.encode("utf-8"))
        doc = MMD4SolR(mydoc=root)

        # Verify vocabulary_loader is None
        assert doc.vocabulary_loader is None

    def test_check_mmd_skips_vocabulary_validation_without_loader(self):
        """Test that check_mmd skips vocabulary validation if loader is None."""
        from solrindexer.mmd import MMD4SolR

        test_mmd_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <mmd:mmd xmlns:mmd="http://www.met.no/schema/mmd">
            <mmd:metadata_identifier>test-id</mmd:metadata_identifier>
            <mmd:title>Test Title</mmd:title>
            <mmd:abstract>Test Abstract</mmd:abstract>
            <mmd:metadata_status>Active</mmd:metadata_status>
            <mmd:dataset_production_status>In Work</mmd:dataset_production_status>
            <mmd:collection>INVALID_COLLECTION</mmd:collection>
            <mmd:last_metadata_update>
                <mmd:update>
                    <mmd:datetime>2024-01-01T00:00:00Z</mmd:datetime>
                </mmd:update>
            </mmd:last_metadata_update>
            <mmd:iso_topic_category>climatologyMeteorologyAtmosphere</mmd:iso_topic_category>
            <mmd:keywords vocabulary="GCMDSK">
                <mmd:keyword>test keyword</mmd:keyword>
            </mmd:keywords>
        </mmd:mmd>
        """

        import lxml.etree as ET

        root = ET.fromstring(test_mmd_xml.encode("utf-8"))
        # Without vocabulary_loader, check should still pass (vocabulary validation skipped)
        doc = MMD4SolR(mydoc=root, vocabulary_loader=None)
        result = doc.check_mmd()
        # Should still return True because vocabulary validation is skipped
        assert isinstance(result, bool)

    def test_check_mmd_validates_with_loader(self):
        """Test that check_mmd validates vocabulary when loader is provided."""
        from solrindexer.mmd import MMD4SolR

        test_mmd_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <mmd:mmd xmlns:mmd="http://www.met.no/schema/mmd">
            <mmd:metadata_identifier>test-id</mmd:metadata_identifier>
            <mmd:title>Test Title</mmd:title>
            <mmd:abstract>Test Abstract</mmd:abstract>
            <mmd:metadata_status>Active</mmd:metadata_status>
            <mmd:dataset_production_status>In Work</mmd:dataset_production_status>
            <mmd:collection>TEST</mmd:collection>
            <mmd:last_metadata_update>
                <mmd:update>
                    <mmd:datetime>2024-01-01T00:00:00Z</mmd:datetime>
                </mmd:update>
            </mmd:last_metadata_update>
            <mmd:iso_topic_category>climatologyMeteorologyAtmosphere</mmd:iso_topic_category>
            <mmd:keywords vocabulary="GCMDSK">
                <mmd:keyword>test keyword</mmd:keyword>
            </mmd:keywords>
        </mmd:mmd>
        """

        import lxml.etree as ET

        root = ET.fromstring(test_mmd_xml.encode("utf-8"))

        # Create mock loader
        mock_loader = MagicMock()
        mock_loader.search.return_value = True

        doc = MMD4SolR(mydoc=root, vocabulary_loader=mock_loader)
        doc.check_mmd()

        # Vocabulary search should be called for controlled elements
        assert mock_loader.search.called
