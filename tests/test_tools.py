from unittest.mock import MagicMock, patch

import pytest
from solrindexer.tools import checkDateFormat, getZones, parse_date, to_solr_id
from solrindexer.tools.tools import (
    _extract_feature_type,
    process_feature_type,
)


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


# ---------------------------------------------------------------------------
# _extract_feature_type
# ---------------------------------------------------------------------------

class TestExtractFeatureType:
    """Tests for _extract_feature_type (xarray-first, netCDF4 fallback)."""

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
    def test_xarray_fails_netcdf4_fallback_success(self):
        """When xarray raises, netCDF4 fallback kicks in and succeeds."""
        mock_nc = MagicMock()
        mock_nc.getncattr.return_value = "trajectory"

        with patch("xarray.open_dataset", side_effect=RuntimeError("xr boom")):
            with patch("netCDF4.Dataset", return_value=mock_nc):
                ft, err = _extract_feature_type("http://fake.dap/ds")

        assert ft == "trajectory"
        assert err is None
        mock_nc.close.assert_called_once()

    @pytest.mark.indexdata
    def test_both_fail_returns_error_message(self):
        """When both xarray and netCDF4 fail, returns (None, error_msg)."""
        with patch("xarray.open_dataset", side_effect=RuntimeError("xr boom")):
            with patch("netCDF4.Dataset", side_effect=OSError("nc boom")):
                ft, err = _extract_feature_type("http://fake.dap/ds")

        assert ft is None
        assert err is not None
        assert "xr boom" in err
        assert "nc boom" in err

    @pytest.mark.indexdata
    def test_netcdf4_attribute_error_returns_none_no_error(self):
        """AttributeError on netCDF4 (no featureType) is not treated as an error."""
        with patch("xarray.open_dataset", side_effect=RuntimeError("xr boom")):
            mock_nc = MagicMock()
            mock_nc.getncattr.side_effect = AttributeError
            with patch("netCDF4.Dataset", return_value=mock_nc):
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
            with patch("netCDF4.Dataset", side_effect=OSError("nc boom")):
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
