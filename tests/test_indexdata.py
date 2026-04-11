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


def _make_mmd_with_data_center(*data_center_fragments):
    """Build a minimal MMD root element with the given mmd:data_center snippets."""
    data_centers = "\n".join(data_center_fragments)
    xml = f"""<mmd xmlns="http://www.met.no/schema/mmd">
  <metadata_identifier>test-id</metadata_identifier>
  {data_centers}
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

    # Legacy fields should still point to the latest update entry.
    assert solr["last_metadata_update_datetime"] == "2024-06-01T00:00:00Z"
    assert solr["last_metadata_update_type"] == "Major modification"
    assert solr["last_metadata_update_note"] == "third"


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
def test_extract_personnel_json_includes_type_and_uris():
  root = _make_mmd_with_personnel(
    """<personnel>
      <role>Technical contact</role>
      <type>Person</type>
      <name uri="https://orcid.org/0000-1111-2222-3333">Ole Dole</name>
      <organisation uri="https://ror.org/001n36p86">Norwegian Meteorological Institute</organisation>
      <email>ole.dole@example.com</email>
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
      "orchid_uri": "https://orcid.org/0000-1111-2222-3333",
      "organisation": "Norwegian Meteorological Institute",
      "ror_uri": "https://ror.org/001n36p86",
      "email": "ole.dole@example.com",
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
    }
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

    assert solr["storage_information_file_name"] == "osisaf_radiative_flux_24h_hl_polstere-050_multi_202003051200.nc"
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
    assert solr["related_url_landing_page"] == ["https://example.com/dataset/landing"]
    assert solr["related_url_landing_page_desc"] == ["Landing page for this dataset"]

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
      </related_information>"""
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

    assert solr["related_url_landing_page"] == ["https://example.com/dataset/landing"]
    assert solr["related_url_scientific_publication"] == ["https://doi.org/10.1234/example"]
    assert solr["related_url_user_guide"] == ["https://example.com/docs/guide.pdf"]

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
        ("Scientific publication", "related_url_scientific_publication", "https://publication.example.com"),
        ("Data paper", "related_url_data_paper", "https://paper.example.com"),
        ("Data management plan", "related_url_data_management_plan", "https://dmp.example.com"),
        ("Other documentation", "related_url_other_documentation", "https://docs.example.com"),
        ("Software", "related_url_software", "https://software.example.com"),
        ("Data server landing page", "related_url_data_server_landing_page", "https://server.example.com"),
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

    # Check that type-specific fields are populated
    for info_type, field_name, url in types_and_URLs:
        assert solr[field_name] == [url], f"Field {field_name} not correctly populated"
        assert solr[field_name + "_desc"] == [f"Description for {info_type}"]


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
      </related_information>"""
    )
    doc = MMD4SolR(mydoc=root)
    solr = {}
    doc._extract_related_information(solr)

    assert solr["related_information_type"] == ["Scientific publication", "Scientific publication"]
    assert solr["related_url_scientific_publication"] == ["https://example.com/pub1", "https://example.com/pub2"]
    assert solr["related_url_scientific_publication_desc"] == ["First publication", "Second publication"]


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
    # Type-specific URL field should not be populated without resource
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
