import pytest

from solrindexer.indexdata import getZones

def testGetZones():
    assert getZones(3, 73) == 31