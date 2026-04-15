from unittest.mock import MagicMock, patch

import pytest

from solrindexer.cli import _report_parent_integrity, _resolve_referenced_parents
from solrindexer.failure_tracker import FailureTracker


@pytest.mark.indexdata
def test_resolve_referenced_parents_returns_none_when_empty():
    assert _resolve_referenced_parents("http://example/solr/core", None, set()) is None


@pytest.mark.indexdata
def test_resolve_referenced_parents_returns_all_when_client_init_fails():
    with patch("solrindexer.cli.pysolr.Solr", side_effect=RuntimeError("boom")):
        result = _resolve_referenced_parents(
            "http://example/solr/core",
            None,
            {"parent-1", "parent-2"},
        )

    assert result == {"parent-1", "parent-2"}


@pytest.mark.indexdata
def test_report_parent_integrity_adds_warning_for_each_unresolved_parent():
    failure_tracker = FailureTracker()

    _report_parent_integrity(
        parent_ids_referenced={"parent-1", "parent-2"},
        unresolved_parent_ids={"parent-2"},
        failure_tracker=failure_tracker,
    )

    assert len(failure_tracker.warnings) == 1
    assert failure_tracker.warnings[0].warning_stage == "parent_integrity"
    assert failure_tracker.warnings[0].metadata_identifier == "parent-2"


@pytest.mark.indexdata
def test_resolve_referenced_parents_uses_tools_helper():
    solr_client = MagicMock()

    with patch("solrindexer.cli.pysolr.Solr", return_value=solr_client), patch(
        "solrindexer.cli.resolve_parent_ids",
        return_value={"parent-3"},
    ) as resolve_parent_ids_mock:
        result = _resolve_referenced_parents(
            "http://example/solr/core",
            None,
            {"parent-1", "parent-3"},
        )

    assert result == {"parent-3"}
    resolve_parent_ids_mock.assert_called_once_with(
        {"parent-1", "parent-3"},
        solr_client=solr_client,
    )
