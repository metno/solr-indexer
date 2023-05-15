import pytest

from solrindexer.searchindex import parse_cfg


@pytest.mark.searchindex
def testParseCfg():
    assert parse_cfg
