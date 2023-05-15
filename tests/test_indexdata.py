import pytest

from solrindexer.indexdata import getZones


@pytest.mark.indexdata
def testGetZones():
    assert getZones(3, 73) == 31
