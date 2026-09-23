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


# ---------------------------------------------------------------------------
# override-feature-type / feature-type gating
# ---------------------------------------------------------------------------


@pytest.mark.indexdata
def test_resolve_override_feature_type_returns_none_when_unset():
    bulk = BulkIndexer([], "http://example/solr/core", threads=1, config={})

    assert bulk._resolve_override_feature_type() is None


@pytest.mark.indexdata
def test_resolve_override_feature_type_returns_canonical_value():
    bulk = BulkIndexer(
        [],
        "http://example/solr/core",
        threads=1,
        config={"override-feature-type": "timeseries"},
    )

    assert bulk._resolve_override_feature_type() == "timeSeries"


@pytest.mark.indexdata
def test_resolve_override_feature_type_warns_and_ignores_invalid_value(monkeypatch):
    bulk = BulkIndexer(
        [],
        "http://example/solr/core",
        threads=1,
        config={"override-feature-type": "not-a-real-type"},
    )

    warnings = []
    monkeypatch.setattr(
        "solrindexer.indexer.logger.warning", lambda msg, *args: warnings.append(msg % args)
    )

    result = bulk._resolve_override_feature_type()

    assert result is None
    assert warnings
    assert "not a valid featureType" in warnings[0]


def _run_bulkindex_for_single_doc(monkeypatch, doc, config, feature_type_side_effect=None):
    """Helper: run bulkindex for a single pre-built doc, tracking process_feature_type calls."""
    file_path = "file.xml"
    bulk = BulkIndexer(
        [file_path], "http://example/solr/core", threads=1, chunksize=10, config=config
    )

    monkeypatch.setattr("solrindexer.indexer.load_file", lambda file_path: file_path)
    monkeypatch.setattr(bulk, "mmd2solr", lambda mmd, status, file_path: (dict(doc), None))
    monkeypatch.setattr(bulk, "add2solr", lambda docs, file_ids=None: None)

    calls = []

    def fake_process_feature_type(tmpdoc):
        calls.append(tmpdoc)
        if feature_type_side_effect is not None:
            return feature_type_side_effect(tmpdoc)
        return (tmpdoc, None)

    monkeypatch.setattr("solrindexer.indexer.process_feature_type", fake_process_feature_type)

    bulk.bulkindex([file_path])
    return calls


@pytest.mark.indexdata
def test_bulkindex_applies_override_feature_type_without_lookup(monkeypatch):
    doc = {"id": "doc-1", "data_access_url_opendap": "http://fake.dap/ds"}
    config = {"override-feature-type": "point"}

    calls = _run_bulkindex_for_single_doc(monkeypatch, doc, config)

    assert calls == []  # process_feature_type is never invoked when override is set


@pytest.mark.indexdata
def test_bulkindex_skips_lookup_for_wms_dataset(monkeypatch):
    doc = {
        "id": "doc-1",
        "data_access_url_opendap": "http://fake.dap/ds",
        "data_access_url_ogc_wms": "http://fake.wms/ds",
    }
    config = {}

    calls = _run_bulkindex_for_single_doc(monkeypatch, doc, config)

    assert calls == []


@pytest.mark.indexdata
def test_bulkindex_skips_lookup_for_grid_spatial_representation(monkeypatch):
    doc = {
        "id": "doc-1",
        "data_access_url_opendap": "http://fake.dap/ds",
        "spatial_representation": "Grid",
    }
    config = {}

    calls = _run_bulkindex_for_single_doc(monkeypatch, doc, config)

    assert calls == []


@pytest.mark.indexdata
def test_bulkindex_still_performs_lookup_for_plain_opendap_dataset(monkeypatch):
    doc = {"id": "doc-1", "data_access_url_opendap": "http://fake.dap/ds"}
    config = {}

    calls = _run_bulkindex_for_single_doc(monkeypatch, doc, config)

    assert len(calls) == 1
