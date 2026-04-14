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
