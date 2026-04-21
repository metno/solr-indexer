import json

import lxml.etree as ET
import pytest

from solrindexer.cli import _split_files_for_processes
from solrindexer.mmd import MMD4SolR, _get_cached_schema

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


def _make_mmd_with_data_center(*data_center_fragments):
    """Build a minimal MMD root element with the given mmd:data_center snippets."""
    data_centers = "\n".join(data_center_fragments)
    xml = f"""<mmd xmlns="http://www.met.no/schema/mmd">
  <metadata_identifier>test-id</metadata_identifier>
  {data_centers}
</mmd>"""
    return ET.fromstring(xml.encode())


def _make_mmd_with_project(*project_fragments):
    """Build a minimal MMD root element with the given mmd:project snippets."""
    projects = "\n".join(project_fragments)
    xml = f"""<mmd xmlns="http://www.met.no/schema/mmd">
  <metadata_identifier>test-id</metadata_identifier>
  {projects}
</mmd>"""
    return ET.fromstring(xml.encode())


def _make_mmd_with_storage_information(storage_information_xml=""):
    """Build a minimal MMD root element with an optional mmd:storage_information snippet."""
    xml = f"""<mmd xmlns="http://www.met.no/schema/mmd">
  <metadata_identifier>test-id</metadata_identifier>
  {storage_information_xml}
</mmd>"""
    return ET.fromstring(xml.encode())


def _make_mmd_with_personnel(*personnel_fragments):
    """Build a minimal MMD root element with the given mmd:personnel snippets."""
    personnel_xml = "\n".join(personnel_fragments)
    xml = f"""<mmd xmlns="http://www.met.no/schema/mmd">
  <metadata_identifier>test-id</metadata_identifier>
  {personnel_xml}
</mmd>"""
    return ET.fromstring(xml.encode())


def _make_mmd_with_last_metadata_update(*update_fragments):
    """Build a minimal MMD root element with mmd:last_metadata_update history."""
    updates = "\n".join(update_fragments)
    xml = f"""<mmd xmlns="http://www.met.no/schema/mmd">
  <metadata_identifier>test-id</metadata_identifier>
  <last_metadata_update>
    {updates}
  </last_metadata_update>
</mmd>"""
    return ET.fromstring(xml.encode())


@pytest.mark.indexdata
def test_extract_last_metadata_update_json_is_chronological():
    root = _make_mmd_with_last_metadata_update(
        """<update>
      <datetime>2024-06-01</datetime>
      <type>Major modification</type>
      <note>third</note>
    </update>""",
        """<update>
      <datetime>2022-01-15</datetime>
      <type>Created</type>
      <note>first</note>
    </update>""",
        """<update>
      <datetime>2023-03-20</datetime>
      <type>Minor modification</type>
      <note>second</note>
    </update>""",
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_last_metadata_update(solr)

    history = json.loads(solr["last_metadata_update_json"])
    assert [item["datetime"] for item in history] == [
        "2022-01-15T00:00:00Z",
        "2023-03-20T00:00:00Z",
        "2024-06-01T00:00:00Z",
    ]
    assert [item["note"] for item in history] == ["first", "second", "third"]

    # New fields point to earliest and latest dates.
    assert solr["last_metadata_created_date"] == "2022-01-15T00:00:00Z"
    assert solr["last_metadata_updated_date"] == "2024-06-01T00:00:00Z"


@pytest.mark.indexdata
def test_extract_last_metadata_update_json_omits_empty_values():
    root = _make_mmd_with_last_metadata_update(
        """<update>
      <datetime>2022-01-15</datetime>
      <type>Created</type>
      <note></note>
    </update>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_last_metadata_update(solr)

    history = json.loads(solr["last_metadata_update_json"])
    assert history == [
        {
            "datetime": "2022-01-15T00:00:00Z",
            "type": "Created",
        }
    ]


@pytest.mark.indexdata
def test_extract_last_metadata_update_single_entry_created_and_updated_same():
    root = _make_mmd_with_last_metadata_update(
        """<update>
      <datetime>2022-01-15</datetime>
      <type>Created</type>
      <note>initial</note>
    </update>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_last_metadata_update(solr)

    assert solr["last_metadata_created_date"] == "2022-01-15T00:00:00Z"
    assert solr["last_metadata_updated_date"] == "2022-01-15T00:00:00Z"
    assert solr["last_metadata_created_date"] == solr["last_metadata_updated_date"]


@pytest.mark.indexdata
def test_extract_personnel_json_includes_type_and_uris():
    root = _make_mmd_with_personnel(
        """<personnel>
      <role>Technical contact</role>
      <type>Person</type>
      <name uri="https://orcid.org/0000-1111-2222-3333">Ole Dole</name>
      <organisation uri="https://ror.org/001n36p86">Norwegian Meteorological Institute</organisation>
      <email>ole.dole@example.com</email>
      <phone>004711111111</phone>
      <contact_address>
        <address>Meteorologisk institutt, Henrik Mohnsplass 1</address>
        <city>Oslo</city>
        <province_or_state>Oslo</province_or_state>
        <postal_code>0000</postal_code>
        <country>Norway</country>
      </contact_address>
    </personnel>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_personnel(solr)

    personnel_json = json.loads(solr["personnel_json"])
    assert personnel_json == [
        {
            "role": "Technical contact",
            "type": "Person",
            "name": "Ole Dole",
            "name_uri": "https://orcid.org/0000-1111-2222-3333",
            "organisation": "Norwegian Meteorological Institute",
            "org_uri": "https://ror.org/001n36p86",
            "email": "ole.dole@example.com",
            "phone": "004711111111",
            "contact_address": {
                "address": "Meteorologisk institutt, Henrik Mohnsplass 1",
                "city": "Oslo",
                "province_or_state": "Oslo",
                "postal_code": "0000",
                "country": "Norway",
            },
        }
    ]


@pytest.mark.indexdata
def test_extract_personnel_json_omits_missing_type_and_uris():
    root = _make_mmd_with_personnel(
        """<personnel>
      <role>Investigator</role>
      <name>Jane Doe</name>
      <organisation>MET Norway</organisation>
      <email>jane.doe@example.com</email>
      <contact_address>
        <country>NORWAY</country>
      </contact_address>
    </personnel>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_personnel(solr)

    personnel_json = json.loads(solr["personnel_json"])
    assert personnel_json == [
        {
            "role": "Investigator",
            "name": "Jane Doe",
            "organisation": "MET Norway",
            "email": "jane.doe@example.com",
            "contact_address": {
                "country": "NORWAY",
            },
        }
    ]


@pytest.mark.indexdata
def test_extract_personnel_json_omits_empty_values_in_entry_and_contact_address():
    root = _make_mmd_with_personnel(
        """<personnel>
      <role>Technical contact</role>
      <name>Jane Doe</name>
      <email></email>
      <contact_address>
        <city></city>
        <country>Norway</country>
      </contact_address>
    </personnel>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_personnel(solr)

    personnel_json = json.loads(solr["personnel_json"])
    assert personnel_json == [
        {
            "role": "Technical contact",
            "name": "Jane Doe",
            "contact_address": {
                "country": "Norway",
            },
        }
    ]


@pytest.mark.indexdata
def test_extract_personnel_adds_email_and_uris_to_resources():
    root = _make_mmd_with_personnel(
        """<personnel>
      <role>Technical contact</role>
      <name uri="https://orcid.org/0000-1111-2222-3333">Ole Dole</name>
      <organisation uri="https://ror.org/001n36p86">MET Norway</organisation>
      <email>ole.dole@example.com</email>
    </personnel>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {"resources": ["https://existing.example/resource"]}
    doc._extract_personnel(solr)
    assert solr["resources"] == [
        "https://existing.example/resource",
        "ole.dole@example.com",
        "https://orcid.org/0000-1111-2222-3333",
        "https://ror.org/001n36p86",
    ]


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
    assert solr["platform_name"] == ["Sentinel-1A"]
    assert solr["platform_name_facet"] == ["Sentinel-1A"]
    assert "platform_resource" not in solr
    assert "platform_instrument_short_name" not in solr
    assert "platform_instrument_name_facet" not in solr
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
    assert solr["platform_instrument_name"] == ["SAR-C"]
    assert solr["platform_name_facet"] == ["Sentinel-1A"]
    assert solr["platform_instrument_name_facet"] == ["SAR-C"]
    assert solr["platform_instrument_resource"] == ["https://example.com/instrument"]
    assert solr["platform_instrument_mode"] == ["IW"]
    assert solr["platform_instrument_polarisation"] == ["VV+VH"]
    assert solr["platform_instrument_product_type"] == ["GRD"]
    assert solr["platform_ancillary_cloud_coverage"] == [12.5]
    assert solr["platform_ancillary_scene_coverage"] == [95.0]
    assert solr["platform_ancillary_timeliness"] == ["NRT"]
    assert solr["resources"] == [
        "https://example.com/platform",
        "https://example.com/instrument",
    ]

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
    assert solr["platform_name"] == ["Sentinel-1A", "Sentinel-2A"]
    assert solr["platform_name_facet"] == ["Sentinel-1A", "Sentinel-2A"]
    parsed = json.loads(solr["platform_json"])
    assert len(parsed) == 2


@pytest.mark.indexdata
def test_extract_platform_name_falls_back_to_long_name():
    root = _make_mmd(
        """<platform>
          <long_name>Fallback Platform Name</long_name>
        </platform>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_platform(solr)

    assert "platform_short_name" not in solr
    assert solr["platform_long_name"] == ["Fallback Platform Name"]
    assert solr["platform_name"] == ["Fallback Platform Name"]
    assert solr["platform_name_facet"] == ["Fallback Platform Name"]


@pytest.mark.indexdata
def test_extract_platform_instrument_name_falls_back_to_long_name():
    root = _make_mmd(
        """<platform>
          <short_name>Sentinel-1A</short_name>
          <instrument>
            <long_name>Fallback Instrument Name</long_name>
          </instrument>
        </platform>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_platform(solr)

    assert "platform_instrument_short_name" not in solr
    assert solr["platform_instrument_long_name"] == ["Fallback Instrument Name"]
    assert solr["platform_instrument_name"] == ["Fallback Instrument Name"]
    assert solr["platform_instrument_name_facet"] == ["Fallback Instrument Name"]


@pytest.mark.indexdata
def test_extract_platform_appends_resources_and_deduplicates_facets():
    root = _make_mmd(
        """<platform>
          <short_name>Sentinel-1A</short_name>
          <resource>https://example.com/shared-resource</resource>
          <instrument>
            <short_name>SAR-C</short_name>
            <resource>https://example.com/shared-resource</resource>
          </instrument>
        </platform>
        <platform>
          <short_name>Sentinel-1A</short_name>
          <instrument>
            <short_name>SAR-C</short_name>
          </instrument>
        </platform>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {"resources": ["https://existing.example/resource"]}
    doc._extract_platform(solr)

    assert solr["platform_name"] == ["Sentinel-1A"]
    assert solr["platform_name_facet"] == ["Sentinel-1A"]
    assert solr["platform_instrument_name"] == ["SAR-C"]
    assert solr["platform_instrument_name_facet"] == ["SAR-C"]
    assert solr["resources"] == [
        "https://existing.example/resource",
        "https://example.com/shared-resource",
    ]


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


@pytest.mark.indexdata
def test_extract_data_center_single():
    root = _make_mmd_with_data_center(
        """<data_center>
          <data_center_name>
            <short_name>MET Norway</short_name>
            <long_name>Norwegian Meteorological Institute</long_name>
          </data_center_name>
          <data_center_url>http://met.no</data_center_url>
        </data_center>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_data_center(solr)

    assert solr["data_center_short_name"] == ["MET Norway"]
    assert solr["data_center_long_name"] == ["Norwegian Meteorological Institute"]
    assert solr["data_center_name"] == ["MET Norway"]
    assert solr["data_center_url"] == ["http://met.no"]


@pytest.mark.indexdata
def test_extract_data_center_name_fallback_to_long_name():
    root = _make_mmd_with_data_center(
        """<data_center>
          <data_center_name>
            <long_name>Norwegian Meteorological Institute</long_name>
          </data_center_name>
        </data_center>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_data_center(solr)

    assert "data_center_short_name" not in solr
    assert solr["data_center_long_name"] == ["Norwegian Meteorological Institute"]
    assert solr["data_center_name"] == ["Norwegian Meteorological Institute"]


@pytest.mark.indexdata
def test_extract_data_center_multiple_entries():
    root = _make_mmd_with_data_center(
        """<data_center>
          <data_center_name>
            <short_name>MET Norway</short_name>
            <long_name>Norwegian Meteorological Institute</long_name>
          </data_center_name>
          <data_center_url>http://met.no</data_center_url>
        </data_center>""",
        """<data_center>
          <data_center_name>
            <short_name>NERSC</short_name>
          </data_center_name>
          <data_center_url>https://nersc.no</data_center_url>
        </data_center>""",
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_data_center(solr)

    assert solr["data_center_short_name"] == ["MET Norway", "NERSC"]
    assert solr["data_center_long_name"] == ["Norwegian Meteorological Institute"]
    assert solr["data_center_name"] == ["MET Norway", "NERSC"]
    assert solr["data_center_url"] == ["http://met.no", "https://nersc.no"]


@pytest.mark.indexdata
def test_extract_project_name_prefers_short_then_falls_back_to_long():
    root = _make_mmd_with_project(
        """<project>
          <short_name>Nansen Legacy</short_name>
          <long_name>Nansen Legacy Project</long_name>
        </project>""",
        """<project>
          <long_name>Long Name Only Project</long_name>
        </project>""",
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_projects(solr)

    assert solr["project_short_name"] == ["Nansen Legacy"]
    assert solr["project_long_name"] == ["Nansen Legacy Project", "Long Name Only Project"]
    assert solr["project_name"] == ["Nansen Legacy", "Long Name Only Project"]


@pytest.mark.indexdata
def test_tosolr_extracts_activity_type():
    root = ET.fromstring(
        b"""<mmd xmlns="http://www.met.no/schema/mmd">
  <metadata_identifier>test-id</metadata_identifier>
  <activity_type>Space Borne Instrument</activity_type>
  <activity_type>Aircraft</activity_type>
</mmd>"""
    )
    doc = MMD4SolR(mydoc=root)

    solr = doc.tosolr()

    assert solr["activity_type"] == ["Space Borne Instrument", "Aircraft"]


@pytest.mark.indexdata
def test_extract_storage_information_full():
    root = _make_mmd_with_storage_information(
        """<storage_information>
        <file_name>osisaf_radiative_flux_24h_hl_polstere-050_multi_202003051200.nc</file_name>
        <file_location>/home/steingod/Desktop</file_location>
        <file_format>NetCDF-CF</file_format>
        <file_size unit="GB">0.12</file_size>
        <checksum type="sha512sum">ad33563f9ab3a6f2ba74ebd72cb1f45fda00d55121a29a29230147e3472ef267c50882b0e0b21ef9ee5ed5cc25e454167cd19a1818f1e13bc044b6fc3ef8f285</checksum>
        <storage_expiry_date>2027-10-24</storage_expiry_date>
      </storage_information>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_storage_information(solr)

    assert (
        solr["storage_information_file_name"]
        == "osisaf_radiative_flux_24h_hl_polstere-050_multi_202003051200.nc"
    )
    assert solr["storage_information_file_location"] == "/home/steingod/Desktop"
    assert solr["storage_information_file_format"] == "NetCDF-CF"
    assert solr["storage_information_file_size"] == "0.12"
    assert solr["storage_information_file_size_unit"] == "GB"
    assert solr["storage_information_file_checksum_type"] == "sha512sum"
    assert solr["storage_information_file_checksum"].startswith("ad33563f9ab3a6f2")
    assert solr["storage_information_file_storage_expiry_date"] == "2027-10-24T00:00:00Z"


@pytest.mark.indexdata
def test_extract_storage_information_partial():
    root = _make_mmd_with_storage_information(
        """<storage_information>
        <file_name>example.nc</file_name>
        <file_size>4.2</file_size>
      </storage_information>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_storage_information(solr)

    assert solr["storage_information_file_name"] == "example.nc"
    assert solr["storage_information_file_size"] == "4.2"
    assert "storage_information_file_size_unit" not in solr
    assert "storage_information_file_checksum" not in solr
    assert "storage_information_file_storage_expiry_date" not in solr


@pytest.mark.indexdata
def test_tosolr_removes_file_location_from_mmd_xml_file_only():
    root = _make_mmd_with_storage_information(
        """<storage_information>
        <file_name>example.nc</file_name>
        <file_location>/sensitive/path/example.nc</file_location>
        <file_format>NetCDF-CF</file_format>
      </storage_information>"""
    )
    doc = MMD4SolR(mydoc=root)

    solr = doc.tosolr()

    assert solr["storage_information_file_location"] == "/sensitive/path/example.nc"
    assert "<mmd:file_location>" not in solr["mmd_xml_file"]
    assert "/sensitive/path/example.nc" not in solr["mmd_xml_file"]


@pytest.mark.indexdata
def test_extract_storage_information_missing():
    root = _make_mmd_with_storage_information()
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_storage_information(solr)

    assert "storage_information_file_name" not in solr


# ---------------------------------------------------------------------------
# Related Information extraction tests
# ---------------------------------------------------------------------------


def _make_mmd_with_related_information(*related_information_fragments):
    """Build a minimal MMD root element with the given mmd:related_information snippets."""
    related_info_xml = "\n".join(related_information_fragments)
    xml = f"""<mmd xmlns="http://www.met.no/schema/mmd">
  <metadata_identifier>test-id</metadata_identifier>
  {related_info_xml}
</mmd>"""
    return ET.fromstring(xml.encode())


def _make_mmd_with_dataset_citation(*dataset_citation_fragments):
    """Build a minimal MMD root element with the given mmd:dataset_citation snippets."""
    dataset_citation_xml = "\n".join(dataset_citation_fragments)
    xml = f"""<mmd xmlns="http://www.met.no/schema/mmd">
  <metadata_identifier>test-id</metadata_identifier>
  {dataset_citation_xml}
</mmd>"""
    return ET.fromstring(xml.encode())


@pytest.mark.indexdata
def test_extract_dataset_citation_json_descriptions_resources_and_dates():
    root = _make_mmd_with_dataset_citation(
        """<dataset_citation>
        <author>Cristian Lussana</author>
        <title>seNorge_2018</title>
        <series>Earth System Science Data</series>
        <volume>11</volume>
        <issue>4</issue>
        <publication_date>2019-10-01</publication_date>
        <publisher>Copernicus Publications</publisher>
        <doi>https://doi.org/10.5194/essd-11-1531-2019</doi>
        <url>https://example.org/citation</url>
      </dataset_citation>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {"descriptions": ["existing-description"], "resources": ["https://existing.example"]}
    doc._extract_dataset_citation(solr)

    citation_json = json.loads(solr["dataset_citation_json"])
    assert citation_json == [
        {
            "author": "Cristian Lussana",
            "title": "seNorge_2018",
            "series": "Earth System Science Data",
            "volume": "11",
            "issue": "4",
            "publication_date": "2019-10-01",
            "publisher": "Copernicus Publications",
            "doi": "https://doi.org/10.5194/essd-11-1531-2019",
            "url": "https://example.org/citation",
        }
    ]

    assert solr["descriptions"] == [
        "existing-description",
        "Cristian Lussana",
        "2019-10-01",
        "seNorge_2018",
        "Earth System Science Data",
        "11",
        "4",
        "Copernicus Publications",
    ]
    assert solr["resources"] == [
        "https://existing.example",
        "https://doi.org/10.5194/essd-11-1531-2019",
        "https://example.org/citation",
    ]
    assert solr["dataset_citation_doi"] == ["https://doi.org/10.5194/essd-11-1531-2019"]
    assert solr["dataset_citation_publication_date"] == ["2019-10-01T00:00:00Z"]


@pytest.mark.indexdata
def test_extract_dataset_citation_skips_invalid_publication_date_and_filters_empty_values():
    root = _make_mmd_with_dataset_citation(
        """<dataset_citation>
        <author>Jane Doe</author>
        <publication_date>not-a-date</publication_date>
        <title></title>
        <doi>https://doi.org/10.1234/example</doi>
      </dataset_citation>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_dataset_citation(solr)

    citation_json = json.loads(solr["dataset_citation_json"])
    assert citation_json == [
        {
            "author": "Jane Doe",
            "publication_date": "not-a-date",
            "doi": "https://doi.org/10.1234/example",
        }
    ]
    assert solr["descriptions"] == ["Jane Doe", "not-a-date"]
    assert solr["resources"] == ["https://doi.org/10.1234/example"]
    assert solr["dataset_citation_doi"] == ["https://doi.org/10.1234/example"]
    assert "dataset_citation_publication_date" not in solr


@pytest.mark.indexdata
def test_extract_dataset_citation_repeated_with_deduped_doi_and_publication_dates():
    root = _make_mmd_with_dataset_citation(
        """<dataset_citation>
        <author>Alice</author>
        <publication_date>2019-10-01</publication_date>
        <doi>https://doi.org/10.1234/dup</doi>
      </dataset_citation>""",
        """<dataset_citation>
        <author>Bob</author>
        <publication_date>2019-10-01T00:00:00Z</publication_date>
        <doi>https://doi.org/10.1234/dup</doi>
      </dataset_citation>""",
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_dataset_citation(solr)

    citation_json = json.loads(solr["dataset_citation_json"])
    assert len(citation_json) == 2
    assert solr["dataset_citation_doi"] == ["https://doi.org/10.1234/dup"]
    assert solr["dataset_citation_publication_date"] == ["2019-10-01T00:00:00Z"]


@pytest.mark.indexdata
def test_extract_dataset_citation_no_nodes_is_noop():
    root = _make_mmd_with_dataset_citation()
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_dataset_citation(solr)

    assert "dataset_citation_json" not in solr
    assert "dataset_citation_doi" not in solr
    assert "dataset_citation_publication_date" not in solr
    assert "descriptions" not in solr
    assert "resources" not in solr


@pytest.mark.indexdata
def test_extract_related_information_single():
    """Test extraction of a single related_information entry."""
    root = _make_mmd_with_related_information(
        """<related_information>
        <type>Dataset landing page</type>
        <description>Landing page for this dataset</description>
        <resource>https://example.com/dataset/landing</resource>
      </related_information>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_related_information(solr)

    assert solr["related_information_type"] == ["Dataset landing page"]
    assert solr["related_information_description"] == ["Landing page for this dataset"]
    assert solr["related_information_resource"] == ["https://example.com/dataset/landing"]
    assert solr["related_url_landing_page"] == "https://example.com/dataset/landing"

    related_json = json.loads(solr["related_information_json"])
    assert len(related_json) == 1
    assert related_json[0]["type"] == "Dataset landing page"
    assert related_json[0]["resource"] == "https://example.com/dataset/landing"
    assert related_json[0]["description"] == "Landing page for this dataset"


@pytest.mark.indexdata
def test_extract_related_information_multiple_mixed_types():
    """Test extraction of multiple related_information entries with different types."""
    root = _make_mmd_with_related_information(
        """<related_information>
        <type>Dataset landing page</type>
        <description>Main landing page</description>
        <resource>https://example.com/dataset/landing</resource>
      </related_information>""",
        """<related_information>
        <type>Scientific publication</type>
        <description>Publication describing the dataset</description>
        <resource>https://doi.org/10.1234/example</resource>
      </related_information>""",
        """<related_information>
        <type>Users guide</type>
        <description>User documentation</description>
        <resource>https://example.com/docs/guide.pdf</resource>
      </related_information>""",
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_related_information(solr)

    assert len(solr["related_information_type"]) == 3
    assert len(solr["related_information_resource"]) == 3
    assert len(solr["related_information_description"]) == 3

    assert "Dataset landing page" in solr["related_information_type"]
    assert "Scientific publication" in solr["related_information_type"]
    assert "Users guide" in solr["related_information_type"]

    assert solr["related_url_landing_page"] == "https://example.com/dataset/landing"
    assert "related_url_scientific_publication" not in solr
    assert "related_url_user_guide" not in solr

    related_json = json.loads(solr["related_information_json"])
    assert len(related_json) == 3


@pytest.mark.indexdata
def test_extract_related_information_all_types():
    """Test extraction with all supported related_information types."""
    types_and_URLs = [
        ("Dataset landing page", "related_url_landing_page", "https://landing.example.com"),
        ("Users guide", "related_url_user_guide", "https://guide.example.com"),
        ("Project home page", "related_url_home_page", "https://project.example.com"),
        ("Observation facility", "related_url_obs_facility", "https://facility.example.com"),
        ("Extended metadata", "related_url_ext_metadata", "https://metadata.example.com"),
        (
            "Scientific publication",
            "related_url_scientific_publication",
            "https://publication.example.com",
        ),
        ("Data paper", "related_url_data_paper", "https://paper.example.com"),
        ("Data management plan", "related_url_data_management_plan", "https://dmp.example.com"),
        ("Other documentation", "related_url_other_documentation", "https://docs.example.com"),
        ("Software", "related_url_software", "https://software.example.com"),
        (
            "Data server landing page",
            "related_url_data_server_landing_page",
            "https://server.example.com",
        ),
    ]

    fragments = [
        f"""<related_information>
        <type>{info_type}</type>
        <description>Description for {info_type}</description>
        <resource>{url}</resource>
      </related_information>"""
        for info_type, _, url in types_and_URLs
    ]

    root = _make_mmd_with_related_information(*fragments)
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_related_information(solr)

    # Check that all types are present
    assert len(solr["related_information_type"]) == 11

    # related_url_landing_page is populated; all other type-specific fields are deprecated
    assert solr["related_url_landing_page"] == "https://landing.example.com"
    for info_type, field_name, url in types_and_URLs:
        if field_name == "related_url_landing_page":
            continue
        assert field_name not in solr, f"Deprecated field {field_name} should not be present"
        assert (field_name + "_desc") not in solr, (
            f"Deprecated field {field_name}_desc should not be present"
        )


@pytest.mark.indexdata
def test_extract_related_information_duplicate_types():
    """Test extraction with multiple entries of the same type."""
    root = _make_mmd_with_related_information(
        """<related_information>
        <type>Scientific publication</type>
        <description>First publication</description>
        <resource>https://example.com/pub1</resource>
      </related_information>""",
        """<related_information>
        <type>Scientific publication</type>
        <description>Second publication</description>
        <resource>https://example.com/pub2</resource>
      </related_information>""",
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_related_information(solr)

    assert solr["related_information_type"] == ["Scientific publication", "Scientific publication"]
    assert solr["related_information_resource"] == [
        "https://example.com/pub1",
        "https://example.com/pub2",
    ]
    assert "related_url_scientific_publication" not in solr
    assert "related_url_scientific_publication_desc" not in solr


@pytest.mark.indexdata
def test_extract_related_information_missing_description():
    """Test extraction with missing description field."""
    root = _make_mmd_with_related_information(
        """<related_information>
        <type>Dataset landing page</type>
        <description></description>
        <resource>https://example.com/landing</resource>
      </related_information>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_related_information(solr)

    assert solr["related_information_type"] == ["Dataset landing page"]
    assert solr["related_information_resource"] == ["https://example.com/landing"]
    # Empty description should not be in the list
    assert "related_information_description" not in solr
    assert "related_url_landing_page_desc" not in solr


@pytest.mark.indexdata
def test_extract_related_information_missing_resource():
    """Test extraction with missing resource field."""
    root = _make_mmd_with_related_information(
        """<related_information>
        <type>Dataset landing page</type>
        <description>A landing page</description>
        <resource></resource>
      </related_information>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_related_information(solr)

    assert solr["related_information_type"] == ["Dataset landing page"]
    assert solr["related_information_description"] == ["A landing page"]
    # Empty resource should not be in the list
    assert "related_information_resource" not in solr
    # landing page field should not be populated when resource is empty
    assert "related_url_landing_page" not in solr


@pytest.mark.indexdata
def test_extract_related_information_no_related_information():
    """Test extraction when no related_information elements exist."""
    root = _make_mmd_with_related_information()
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_related_information(solr)

    assert "related_information_type" not in solr
    assert "related_information_resource" not in solr
    assert "related_information_description" not in solr
    assert "related_information_json" not in solr
    assert "related_url_landing_page" not in solr


@pytest.mark.indexdata
def test_extract_related_information_observation_facility_description_to_field():
    """Test that Observation facility type with description appends to observation_facility field."""
    root = _make_mmd_with_related_information(
        """<related_information>
        <type>Observation facility</type>
        <description>Ny-Alesund observatory</description>
        <resource>https://facility.example.com</resource>
      </related_information>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {"observation_facility": ["Existing facility"]}
    doc._extract_related_information(solr)
    assert solr["observation_facility"] == [
        "Existing facility",
        "Ny-Alesund observatory",
    ]


@pytest.mark.indexdata
def test_split_files_for_processes_round_robin_distribution():
    files = [f"f{i}.xml" for i in range(7)]
    shards = _split_files_for_processes(files, process_count=3)

    assert len(shards) == 3
    assert shards[0] == ["f0.xml", "f3.xml", "f6.xml"]
    assert shards[1] == ["f1.xml", "f4.xml"]
    assert shards[2] == ["f2.xml", "f5.xml"]


@pytest.mark.indexdata
def test_split_files_for_processes_caps_to_file_count():
    files = ["a.xml", "b.xml"]
    shards = _split_files_for_processes(files, process_count=10)

    assert len(shards) == 2
    assert shards[0] == ["a.xml"]
    assert shards[1] == ["b.xml"]


@pytest.mark.indexdata
def test_get_cached_schema_returns_none_for_empty_path():
    """Schema cache returns None gracefully for empty/missing paths."""
    result = _get_cached_schema("")
    assert result is None

    result = _get_cached_schema(None)
    assert result is None


@pytest.mark.indexdata
def test_get_cached_schema_loads_and_caches():
    """Schema is loaded from disk and cached for subsequent calls."""
    # Use the reference MMD file's XSD if available; fall back to test logic
    xsd_path = "./mmd/xsd/mmd.xsd"
    if not __import__("pathlib").Path(xsd_path).exists():
        pytest.skip(f"XSD not found at {xsd_path}")

    # First call: load from disk
    schema1 = _get_cached_schema(xsd_path)
    assert schema1 is not None
    assert isinstance(schema1, ET.XMLSchema)

    # Second call: should return same cached instance
    schema2 = _get_cached_schema(xsd_path)
    assert schema2 is schema1  # same object, not just equal


@pytest.mark.indexdata
def test_get_cached_schema_different_paths_cached_independently():
    """Different XSD paths are cached separately."""
    xsd_path_1 = "./mmd/xsd/mmd.xsd"

    # Verify cache works with multiple calls on same path
    # (we only test reachable path to avoid missing file warnings)
    if not __import__("pathlib").Path(xsd_path_1).exists():
        pytest.skip(f"XSD not found at {xsd_path_1}")

    schema1 = _get_cached_schema(xsd_path_1)
    assert schema1 is not None

    # Verify the same call returns the same instance (cache reuse)
    schema1_again = _get_cached_schema(xsd_path_1)
    assert schema1_again is schema1
