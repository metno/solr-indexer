import pytest

from solrindexer.tools import getZones
from solrindexer.tools import to_solr_id


@pytest.mark.indexdata
def testGetZones():
    assert getZones(3, 73) == 31


@pytest.mark.indexdata
def testToSolrId():
    metadata_identifier = 'no.met:b7cb7934-77ca-4439-812e-f560df3fe7eb'
    solr_id = 'no-met-b7cb7934-77ca-4439-812e-f560df3fe7eb'
    assert to_solr_id(metadata_identifier) == solr_id
