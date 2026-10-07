import logging
from types import SimpleNamespace
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from solrindexer.cli import (
    EXIT_FAILURE,
    EXIT_SUCCESS,
    EXIT_WARNINGS,
    _determine_exit_code,
    _report_parent_integrity,
    _resolve_input_files,
    _resolve_referenced_parents,
    parse_arguments,
)
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


@pytest.mark.indexdata
def test_determine_exit_code_returns_success_when_no_failures_or_warnings():
    assert _determine_exit_code(FailureTracker()) == EXIT_SUCCESS


@pytest.mark.indexdata
def test_determine_exit_code_returns_warning_code_when_only_warnings_exist():
    failure_tracker = FailureTracker()
    failure_tracker.add_warning("file.xml", "warning", "validation", "id-1")

    assert _determine_exit_code(failure_tracker) == EXIT_WARNINGS


@pytest.mark.indexdata
def test_determine_exit_code_returns_failure_code_when_failures_exist():
    failure_tracker = FailureTracker()
    failure_tracker.add_warning("file.xml", "warning", "validation", "id-1")
    failure_tracker.add_failure("file.xml", "failure", "indexing", "id-1")

    assert _determine_exit_code(failure_tracker) == EXIT_FAILURE


@pytest.mark.indexdata
def test_parse_arguments_feature_type_flags_default_to_none():
    """Unset flags default to None so config-file values aren't overridden."""
    with patch("sys.argv", ["indexdata", "-c", "cfg.yml", "-i", "file.xml"]):
        args = parse_arguments()

    assert args.skip_feature_type is None
    assert args.override_feature_type is None


@pytest.mark.indexdata
def test_parse_arguments_skip_feature_type_flag_sets_true():
    with patch(
        "sys.argv",
        ["indexdata", "-c", "cfg.yml", "-i", "file.xml", "--skip-feature-type"],
    ):
        args = parse_arguments()

    assert args.skip_feature_type is True


@pytest.mark.indexdata
def test_parse_arguments_override_feature_type_flag_sets_value():
    with patch(
        "sys.argv",
        [
            "indexdata",
            "-c",
            "cfg.yml",
            "-i",
            "file.xml",
            "--override-feature-type",
            "timeSeries",
        ],
    ):
        args = parse_arguments()

    assert args.override_feature_type == "timeSeries"


def _input_args(**kwargs):
    defaults = {"input_file": None, "list_file": None, "directory": None, "recursive": False}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


@pytest.fixture
def cli_caplog(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    # Package logs deliberately do not propagate to pytest's root capture handler.
    package_logger = logging.getLogger("solrindexer")
    package_logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.INFO, logger="solrindexer.cli"):
            yield caplog
    finally:
        package_logger.removeHandler(caplog.handler)


def test_resolve_input_files_logs_single_file(cli_caplog):
    assert _resolve_input_files(_input_args(input_file="a.xml")) == ["a.xml"]
    assert "Input: single file a.xml" in cli_caplog.text


def test_resolve_input_files_logs_list_file(tmp_path, cli_caplog):
    list_file = tmp_path / "files.txt"
    list_file.write_text("a.xml\n\nb.xml\n", encoding="utf-8")
    assert _resolve_input_files(_input_args(list_file=str(list_file))) == ["a.xml", "b.xml"]
    assert f"Input: file list {list_file}" in cli_caplog.text


@pytest.mark.parametrize("recursive, label", [(True, "recursive"), (False, "non-recursive")])
def test_resolve_input_files_logs_directory(tmp_path, cli_caplog, recursive, label):
    (tmp_path / "a.xml").write_text("<x/>", encoding="utf-8")
    files = _resolve_input_files(_input_args(directory=str(tmp_path), recursive=recursive))
    assert files == [str(tmp_path / "a.xml")]
    assert f"Input: directory {tmp_path} ({label})" in cli_caplog.text
