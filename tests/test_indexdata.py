import json

import lxml.etree as ET
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


# ---------------------------------------------------------------------------
# Platform extraction tests
# ---------------------------------------------------------------------------

NS = "http://www.met.no/schema/mmd"


def _make_mmd(*platform_xml_fragments):
    """Build a minimal MMD root element with the given mmd:platform snippets."""
    platforms = "\n".join(platform_xml_fragments)
    xml = f"""<mmd xmlns="http://www.met.no/schema/mmd">
  <metadata_identifier>test-id</metadata_identifier>
  {platforms}
</mmd>"""
    return ET.fromstring(xml.encode())


@pytest.mark.indexdata
def test_extract_platform_required_fields():
    root = _make_mmd(
        """<platform>
          <short_name>Sentinel-1A</short_name>
          <long_name>Sentinel-1A (C-band SAR)</long_name>
        </platform>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_platform(solr)

    assert solr["platform_short_name"] == ["Sentinel-1A"]
    assert solr["platform_long_name"] == ["Sentinel-1A (C-band SAR)"]
    assert solr["platform_name"] == ["Sentinel-1A: Sentinel-1A (C-band SAR)"]
    assert "platform_resource" not in solr
    assert "platform_instrument_short_name" not in solr
    assert "platform_ancillary_cloud_coverage" not in solr


@pytest.mark.indexdata
def test_extract_platform_full_with_instrument_and_ancillary():
    root = _make_mmd(
        """<platform>
          <short_name>Sentinel-1A</short_name>
          <long_name>Sentinel-1A (C-band SAR)</long_name>
          <resource>https://example.com/platform</resource>
          <orbit_relative>12</orbit_relative>
          <orbit_absolute>34567</orbit_absolute>
          <orbit_direction>ascending</orbit_direction>
          <instrument>
            <short_name>SAR-C</short_name>
            <long_name>Synthetic Aperture Radar C-band</long_name>
            <resource>https://example.com/instrument</resource>
            <mode>IW</mode>
            <polarisation>VV+VH</polarisation>
            <product_type>GRD</product_type>
          </instrument>
          <ancillary>
            <cloud_coverage>12.5</cloud_coverage>
            <scene_coverage>95.0</scene_coverage>
            <timeliness>NRT</timeliness>
          </ancillary>
        </platform>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_platform(solr)

    assert solr["platform_short_name"] == ["Sentinel-1A"]
    assert solr["platform_resource"] == ["https://example.com/platform"]
    assert solr["platform_orbit_relative"] == [12]
    assert solr["platform_orbit_absolute"] == [34567]
    assert solr["platform_orbit_direction"] == ["ascending"]
    assert solr["platform_instrument_short_name"] == ["SAR-C"]
    assert solr["platform_instrument_long_name"] == ["Synthetic Aperture Radar C-band"]
    assert solr["platform_instrument_name"] == ["SAR-C: Synthetic Aperture Radar C-band"]
    assert solr["platform_instrument_resource"] == ["https://example.com/instrument"]
    assert solr["platform_instrument_mode"] == ["IW"]
    assert solr["platform_instrument_polarisation"] == ["VV+VH"]
    assert solr["platform_instrument_product_type"] == ["GRD"]
    assert solr["platform_ancillary_cloud_coverage"] == [12.5]
    assert solr["platform_ancillary_scene_coverage"] == [95.0]
    assert solr["platform_ancillary_timeliness"] == ["NRT"]

    parsed = json.loads(solr["platform_json"])
    assert len(parsed) == 1
    assert parsed[0]["instrument"]["short_name"] == "SAR-C"
    assert parsed[0]["ancillary"]["timeliness"] == "NRT"


@pytest.mark.indexdata
def test_extract_platform_multiple_platforms():
    root = _make_mmd(
        """<platform>
          <short_name>Sentinel-1A</short_name>
          <long_name>Sentinel-1A SAR</long_name>
        </platform>""",
        """<platform>
          <short_name>Sentinel-2A</short_name>
          <long_name>Sentinel-2A MSI</long_name>
        </platform>""",
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_platform(solr)

    assert solr["platform_short_name"] == ["Sentinel-1A", "Sentinel-2A"]
    assert solr["platform_long_name"] == ["Sentinel-1A SAR", "Sentinel-2A MSI"]
    assert solr["platform_name"] == ["Sentinel-1A: Sentinel-1A SAR", "Sentinel-2A: Sentinel-2A MSI"]
    parsed = json.loads(solr["platform_json"])
    assert len(parsed) == 2


@pytest.mark.indexdata
def test_extract_platform_no_instrument_no_ancillary():
    root = _make_mmd(
        """<platform>
          <short_name>NOAA-20</short_name>
          <long_name>NOAA-20 Weather Satellite</long_name>
        </platform>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_platform(solr)

    assert "platform_instrument_short_name" not in solr
    assert "platform_ancillary_cloud_coverage" not in solr
    parsed = json.loads(solr["platform_json"])
    assert "instrument" not in parsed[0]
    assert "ancillary" not in parsed[0]


@pytest.mark.indexdata
def test_extract_platform_invalid_orbit_numbers_are_skipped():
    root = _make_mmd(
        """<platform>
          <short_name>TestSat</short_name>
          <long_name>Test Satellite</long_name>
          <orbit_relative>not-a-number</orbit_relative>
          <orbit_absolute>also-bad</orbit_absolute>
        </platform>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_platform(solr)

    assert "platform_orbit_relative" not in solr
    assert "platform_orbit_absolute" not in solr


@pytest.mark.indexdata
def test_extract_platform_no_platforms():
    root = _make_mmd()
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_platform(solr)

    assert "platform_short_name" not in solr
    assert "platform_json" not in solr
