import pytest

from solrindexer.tools import getZones
from solrindexer.tools import to_solr_id
from solrindexer.tools import parse_date
from solrindexer.tools import checkDateFormat


@pytest.mark.indexdata
def testGetZones():
    assert getZones(3, 73) == 31


@pytest.mark.indexdata
def testToSolrId():
    metadata_identifier = 'no.met:b7cb7934-77ca-4439-812e-f560df3fe7eb'
    solr_id = 'no-met-b7cb7934-77ca-4439-812e-f560df3fe7eb'
    assert to_solr_id(metadata_identifier) == solr_id


@pytest.mark.indexdata
def testParseValidDate():
    valid_date = "2023-05-25T10:19:13Z"
    assert parse_date(valid_date) == valid_date


@pytest.mark.indexdata
def testParseGenericDate():
    date = "2022-02-28T14:26:33.905269+00:0"
    parsed_date = "2022-02-28T14:26:33Z"
    assert parse_date(date) == parsed_date


@pytest.mark.indexdata
def testDateFormatValid():
    valid_date = "2022-02-28T14:26:33Z"
    assert checkDateFormat(valid_date) is True


@pytest.mark.indexdata
def testDateFormatInValid():
    not_valid_date = "2022-02-28T14:26:33.905269+00:0"
    assert checkDateFormat(not_valid_date) is False
