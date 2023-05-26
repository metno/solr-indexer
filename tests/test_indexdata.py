import pytest

from solrindexer.indexdata import MMD4SolR

""" Global test variables"""
infile = "./tests/data/reference_nc.xml"


@pytest.mark.indexdata
def testMMD4SolrInit():
    assert MMD4SolR(infile)


@pytest.mark.indexdata
def testMMD4SolrInitFileNotFOund():
    nofile = "nofile.xml"
    with pytest.raises(FileNotFoundError):
        MMD4SolR(nofile)


@pytest.mark.indexdata
def testCheckMMd():
    mydoc = MMD4SolR(infile)
    assert mydoc.check_mmd


@pytest.mark.indexdata
def testToSolR():
    mydoc = MMD4SolR(infile)
    assert mydoc.tosolr
