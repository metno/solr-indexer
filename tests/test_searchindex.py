import pytest
from solrindexer.script.searchindex import parse_cfg


@pytest.mark.searchindex
def testParseCfg():
    assert parse_cfg
