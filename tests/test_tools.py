from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from solrindexer.tools import (
    _extract_feature_type,
    add_adc_thumbnails,
    checkDateFormat,
    load_adc_thumbnail_path_contract_cases,
    parse_date,
    process_feature_type,
    resolve_parent_ids,
    to_solr_id,
)


@pytest.mark.indexdata
def testToSolrId():
    metadata_identifier = "no.met:b7cb7934-77ca-4439-812e-f560df3fe7eb"
    solr_id = "no-met-b7cb7934-77ca-4439-812e-f560df3fe7eb"
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


# ---------------------------------------------------------------------------
# _extract_feature_type
# ---------------------------------------------------------------------------


class TestExtractFeatureType:
    """Tests for _extract_feature_type (xarray only, thread-safe)."""

    @pytest.mark.indexdata
    def test_xarray_success(self):
        """xarray opens dataset and returns featureType attribute."""
        mock_ds = MagicMock()
        mock_ds.attrs = {"featureType": "timeSeries"}
        mock_ds.__enter__ = lambda s: s
        mock_ds.__exit__ = MagicMock(return_value=False)

        with patch("xarray.open_dataset", return_value=mock_ds):
            ft, err = _extract_feature_type("http://fake.dap/ds")

        assert ft == "timeSeries"
        assert err is None

    @pytest.mark.indexdata
    def test_xarray_missing_attribute_returns_none(self):
        """When featureType is absent xarray returns (None, None) — not an error."""
        mock_ds = MagicMock()
        mock_ds.attrs = {}

        with patch("xarray.open_dataset", return_value=mock_ds):
            ft, err = _extract_feature_type("http://fake.dap/ds")

        assert ft is None
        assert err is None

    @pytest.mark.indexdata
    def test_xarray_fails_returns_error_message(self):
        """When xarray raises, returns (None, error_msg) — no fallback available."""
        with patch("xarray.open_dataset", side_effect=RuntimeError("xr boom")):
            ft, err = _extract_feature_type("http://fake.dap/ds")

        assert ft is None
        assert err is not None
        assert "xr boom" in err

    @pytest.mark.indexdata
    def test_xarray_attribute_error_returns_none_no_error(self):
        """AttributeError on xarray (no featureType) is not treated as an error."""
        with patch("xarray.open_dataset", side_effect=AttributeError):
            ft, err = _extract_feature_type("http://fake.dap/ds")

        assert ft is None
        assert err is None


# ---------------------------------------------------------------------------
# process_feature_type
# ---------------------------------------------------------------------------


class TestProcessFeatureType:
    """Tests for process_feature_type return-tuple contract."""

    def _make_doc(self, opendap_url="http://fake.dap/ds", status="Active"):
        return {
            "id": "no.met:test-id",
            "metadata_status": status,
            "data_access_url_opendap": opendap_url,
        }

    @pytest.mark.indexdata
    def test_no_opendap_url_returns_unchanged_no_error(self):
        doc = {"id": "no.met:x", "metadata_status": "Active"}
        result_doc, err = process_feature_type(doc)
        assert result_doc is doc
        assert err is None

    @pytest.mark.indexdata
    def test_inactive_metadata_returns_unchanged_no_error(self):
        doc = self._make_doc(status="Inactive")
        result_doc, err = process_feature_type(doc)
        assert result_doc is doc
        assert err is None

    @pytest.mark.indexdata
    def test_extraction_error_propagated_as_error_msg(self):
        """Extraction failure surfaces as non-None error_msg, doc is returned unchanged."""
        doc = self._make_doc()
        with patch("xarray.open_dataset", side_effect=RuntimeError("xr boom")):
            result_doc, err = process_feature_type(doc)

        assert result_doc is doc
        assert err is not None
        assert "feature_type" not in result_doc

    @pytest.mark.indexdata
    def test_successful_extraction_updates_doc(self):
        """Happy path: canonical feature_type is written into the document."""
        doc = self._make_doc()
        mock_ds = MagicMock()
        mock_ds.attrs = {"featureType": "timeSeries"}

        with patch("xarray.open_dataset", return_value=mock_ds):
            result_doc, err = process_feature_type(doc)

        assert err is None
        assert result_doc.get("feature_type") == "timeSeries"

    @pytest.mark.indexdata
    def test_invalid_canonical_type_no_error(self):
        """Unknown featureType value is silently ignored — not an error."""
        doc = self._make_doc()
        mock_ds = MagicMock()
        mock_ds.attrs = {"featureType": "notAValidType"}

        with patch("xarray.open_dataset", return_value=mock_ds):
            result_doc, err = process_feature_type(doc)

        assert err is None
        assert "feature_type" not in result_doc


class TestResolveParentIds:
    """Tests for the final parent resolution helper."""

    @pytest.mark.indexdata
    def test_empty_parent_ids_returns_none(self):
        assert resolve_parent_ids(set(), solr_client=MagicMock()) is None

    @pytest.mark.indexdata
    def test_existing_parent_already_marked_resolves_without_update(self):
        solr_client = MagicMock()

        with patch(
            "solrindexer.tools.get_dataset",
            return_value={"doc": {"id": "parent-1", "isParent": True}},
        ), patch("solrindexer.tools.set_parent_flag") as set_parent_flag_mock:
            result = resolve_parent_ids({"parent-1"}, solr_client=solr_client)

        assert result is None
        set_parent_flag_mock.assert_not_called()

    @pytest.mark.indexdata
    def test_existing_parent_updates_and_resolves(self):
        solr_client = MagicMock()

        with patch(
            "solrindexer.tools.get_dataset",
            return_value={"doc": {"id": "parent-1", "isParent": False}},
        ), patch("solrindexer.tools.set_parent_flag") as set_parent_flag_mock:
            result = resolve_parent_ids({"parent-1"}, solr_client=solr_client)

        assert result is None
        set_parent_flag_mock.assert_called_once_with("parent-1", solr_client=solr_client)

    @pytest.mark.indexdata
    def test_missing_parent_is_returned_as_unresolved(self):
        solr_client = MagicMock()

        with patch("solrindexer.tools.get_dataset", return_value={"doc": None}):
            result = resolve_parent_ids({"parent-1"}, solr_client=solr_client)

        assert result == {"parent-1"}


class TestAddAdcThumbnails:
    """Tests for ADC pre-generated thumbnail lookup."""

    @pytest.mark.indexdata
    def test_sets_thumbnail_url_when_file_exists(self, tmp_path):
        relative_path = tmp_path / "no.met" / "no.met.thredds" / "2024" / "11"
        relative_path.mkdir(parents=True, exist_ok=True)
        file_path = relative_path / "no-met-dataset-id.png"
        file_path.write_bytes(b"png")

        doc = {
            "metadata_identifier": "no.met:dataset-id",
            "data_access_url_ogc_wms": ["https://thredds.met.no/data/wms"],
            "temporal_extent_start_date": "2024-11-10",
        }
        cfg = {
            "adc-thumbnails-base-path": str(tmp_path),
            "adc-thumbnails-base-url": "https://adc.example/thumbnails",
        }

        def fake_builder(metadata_identifier, start_date, wms_url):
            assert metadata_identifier == "no.met:dataset-id"
            assert start_date == "2024-11-10"
            assert wms_url == "https://thredds.met.no/data/wms"
            return Path("no.met") / "no.met.thredds" / "2024" / "11" / "no-met-dataset-id.png"

        with patch("solrindexer.tools._load_adc_path_builder", return_value=fake_builder):
            updated_doc = add_adc_thumbnails(doc, cfg)

        assert updated_doc["thumbnail_url"] == (
            "https://adc.example/thumbnails/no.met/no.met.thredds/2024/11/no-met-dataset-id.png"
        )

    @pytest.mark.indexdata
    def test_leaves_doc_unchanged_when_file_missing(self, tmp_path):
        doc = {
            "metadata_identifier": "no.met:dataset-id",
            "data_access_url_ogc_wms": ["https://thredds.met.no/data/wms"],
            "temporal_extent_start_date": "2024-11-10",
        }
        cfg = {
            "adc-thumbnails-base-path": str(tmp_path),
            "adc-thumbnails-base-url": "https://adc.example/thumbnails",
        }

        with patch(
            "solrindexer.tools._load_adc_path_builder",
            return_value=lambda **_: Path("missing.png"),
        ):
            updated_doc = add_adc_thumbnails(doc, cfg)

        assert "thumbnail_url" not in updated_doc


@pytest.mark.indexdata
def test_adc_deterministic_path_contract_vectors():
    from solrindexer.tools import _load_adc_path_builder

    builder = _load_adc_path_builder()
    contract_cases = load_adc_thumbnail_path_contract_cases()

    if builder is None or not contract_cases:
        pytest.skip("metsis-thumbnail-generator not available; skipping ADC contract parity test")

    for case in contract_cases:
        resolved = builder(
            metadata_identifier=case["metadata_identifier"],
            start_date=case["start_date"],
            wms_url=case["wms_url"],
        )
        assert resolved.as_posix() == case["expected_relative_path"], case["name"]
