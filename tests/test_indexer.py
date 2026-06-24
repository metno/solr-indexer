import pytest

from solrindexer.indexer import BulkIndexer


@pytest.mark.indexdata
def test_should_use_process_pool_false_with_single_thread():
    bulk = BulkIndexer([], "http://example/solr/core", threads=1, config={})

    assert bulk._should_use_process_pool(50) is False


@pytest.mark.indexdata
def test_should_use_process_pool_false_for_small_batch_by_default():
    bulk = BulkIndexer([], "http://example/solr/core", threads=8, config={})

    assert bulk._should_use_process_pool(1) is False
    assert bulk._should_use_process_pool(4) is False


@pytest.mark.indexdata
def test_should_use_process_pool_respects_config_threshold():
    bulk = BulkIndexer(
        [],
        "http://example/solr/core",
        threads=8,
        config={"process-pool-min-docs": 3},
    )

    assert bulk._should_use_process_pool(2) is False
    assert bulk._should_use_process_pool(3) is True


@pytest.mark.indexdata
def test_bulkindex_collects_unique_parent_ids(monkeypatch):
    files = ["child-a.xml", "child-b.xml", "parent.xml"]
    docs_by_file = {
        "child-a.xml": ({"id": "child-a", "related_dataset_id": "parent-1"}, "parent-1"),
        "child-b.xml": ({"id": "child-b", "related_dataset_id": "parent-1"}, "parent-1"),
        "parent.xml": ({"id": "parent-1", "isParent": False}, None),
    }

    bulk = BulkIndexer(files, "http://example/solr/core", threads=1, chunksize=10, config={})

    monkeypatch.setattr("solrindexer.indexer.load_file", lambda file_path: file_path)
    monkeypatch.setattr(bulk, "mmd2solr", lambda mmd, status, file_path: docs_by_file[file_path])
    monkeypatch.setattr(bulk, "add2solr", lambda docs, file_ids=None: None)

    result = bulk.bulkindex(files)

    assert result[0] == {"parent-1"}
    assert result[1] == 0
    assert result[2] == 3
    assert result[3] == 3
    assert result[4] is bulk.failure_tracker


@pytest.mark.indexdata
def test_mmd2solr_missing_required_field_is_reported_as_validation_failure(monkeypatch):
    bulk = BulkIndexer([], "http://example/solr/core", threads=1, config={})

    class FakeMMD4SolR:
        def __init__(
            self,
            filename=None,
            mydoc=None,
            bulkFile=None,
            xsd_path=None,
            warning_callback=None,
            vocabulary_loader=None,
        ):
            self.warning_callback = warning_callback

        def check_mmd(self):
            if callable(self.warning_callback):
                self.warning_callback(
                    "❌ check_mmd missing required mmd:dataset_production_status",
                    "validation",
                )
            return False

        def get_metadata_identifier(self):
            return "urn:uuid:test-missing-required"

    monkeypatch.setattr("solrindexer.indexer.MMD4SolR", FakeMMD4SolR)

    doc, status = bulk.mmd2solr(mmd=object(), status=None, file="bad.xml")

    assert doc is None
    assert status is None
    assert len(bulk.failure_tracker.failures) == 1
    failure = bulk.failure_tracker.failures[0]
    assert failure.error_stage == "validation"
    assert "dataset_production_status" in failure.error_message
