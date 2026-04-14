"""Tests for the failure tracking system."""

import pytest

from solrindexer.failure_tracker import FailureRecord, FailureTracker


def test_failure_tracker_creation():
    """Test that FailureTracker can be created."""
    tracker = FailureTracker()
    assert tracker.failures == []
    assert len(tracker.failures) == 0


def test_add_single_failure():
    """Test adding a single failure."""
    tracker = FailureTracker()
    tracker.add_failure(
        filename="test_file.xml",
        error_message="Test error message",
        error_stage="parsing",
        metadata_identifier="test_id_001",
    )

    assert len(tracker.failures) == 1
    assert tracker.failures[0].filename == "test_file.xml"
    assert tracker.failures[0].error_message == "Test error message"
    assert tracker.failures[0].error_stage == "parsing"
    assert tracker.failures[0].metadata_identifier == "test_id_001"


def test_add_multiple_failures():
    """Test adding multiple failures."""
    tracker = FailureTracker()

    failures = [
        ("file1.xml", "Error 1", "parsing", "id_001"),
        ("file2.xml", "Error 2", "validation", "id_002"),
        ("file3.xml", "Error 3", "conversion", None),  # No identifier
        ("file4.xml", "Error 4", "indexing", "id_004"),
    ]

    for filename, msg, stage, identifier in failures:
        tracker.add_failure(filename, msg, stage, identifier)

    assert len(tracker.failures) == 4
    assert tracker.failures[2].metadata_identifier is None


def test_add_single_warning():
    """Test adding a single warning."""
    tracker = FailureTracker()
    tracker.add_warning(
        filename="warn_file.xml",
        warning_message="Non-controlled value in collection",
        warning_stage="validation",
        metadata_identifier="warn_id_001",
    )

    assert len(tracker.warnings) == 1
    assert tracker.warnings[0].filename == "warn_file.xml"
    assert tracker.warnings[0].warning_message == "Non-controlled value in collection"
    assert tracker.warnings[0].warning_stage == "validation"
    assert tracker.warnings[0].metadata_identifier == "warn_id_001"


def test_get_failures_by_stage():
    """Test filtering failures by stage."""
    tracker = FailureTracker()

    stages = ["parsing", "validation", "parsing", "conversion", "parsing"]
    for i, stage in enumerate(stages):
        tracker.add_failure(
            filename=f"file{i}.xml",
            error_message=f"Error {i}",
            error_stage=stage,
            metadata_identifier=f"id_{i:03d}",
        )

    parsing_failures = tracker.get_failures_by_stage("parsing")
    assert len(parsing_failures) == 3

    validation_failures = tracker.get_failures_by_stage("validation")
    assert len(validation_failures) == 1

    conversion_failures = tracker.get_failures_by_stage("conversion")
    assert len(conversion_failures) == 1


def test_get_warnings_by_stage():
    """Test filtering warnings by stage."""
    tracker = FailureTracker()
    tracker.add_warning("file1.xml", "Warning 1", "validation", "id_001")
    tracker.add_warning("file2.xml", "Warning 2", "validation", "id_002")
    tracker.add_warning("file3.xml", "Warning 3", "conversion", "id_003")

    validation_warnings = tracker.get_warnings_by_stage("validation")
    assert len(validation_warnings) == 2

    conversion_warnings = tracker.get_warnings_by_stage("conversion")
    assert len(conversion_warnings) == 1


def test_get_summary_empty():
    """Test summary generation when no failures."""
    tracker = FailureTracker()
    summary = tracker.get_summary()
    assert "No failures" in summary


def test_get_summary_with_failures():
    """Test summary generation with failures."""
    tracker = FailureTracker()

    tracker.add_failure(
        filename="file1.xml",
        error_message="XSD validation failed",
        error_stage="validation",
        metadata_identifier="noa_met_004_ppi_0-25km",
    )
    tracker.add_failure(
        filename="file2.xml",
        error_message="Missing temporal_extent_start_date",
        error_stage="conversion",
        metadata_identifier=None,
    )
    tracker.add_failure(
        filename="file3.xml",
        error_message="Solr indexing failed: connection timeout",
        error_stage="indexing",
        metadata_identifier="noa_met_004_ppi_0-50km",
    )

    summary = tracker.get_summary()
    assert "3 FAILURE(S)" in summary
    assert "file1.xml" in summary
    assert "file2.xml" in summary
    assert "file3.xml" in summary
    assert "[VALIDATION]" in summary
    assert "[CONVERSION]" in summary
    assert "[INDEXING]" in summary
    assert "noa_met_004_ppi_0-25km" in summary
    assert "noa_met_004_ppi_0-50km" in summary


def test_summary_grouping_by_filename():
    """Test that summary groups failures correctly by filename."""
    tracker = FailureTracker()

    # Add single file with multiple stage failures
    tracker.add_failure(
        filename="multi_stage_file.xml", error_message="Parse error", error_stage="parsing"
    )
    tracker.add_failure(
        filename="multi_stage_file.xml", error_message="Validation error", error_stage="validation"
    )
    tracker.add_failure(
        filename="multi_stage_file.xml", error_message="Conversion error", error_stage="conversion"
    )

    summary = tracker.get_summary()
    # Should show file with all its failures together
    assert "FILE: multi_stage_file.xml" in summary
    assert "[PARSING]" in summary
    assert "[VALIDATION]" in summary
    assert "[CONVERSION]" in summary
    # Should show 3 failures total
    assert "3 FAILURE(S)" in summary


def test_summary_failure_lines_include_fail_icon():
    """Failure lines should include the fail icon in default (unicode) mode."""
    tracker = FailureTracker()
    tracker.add_failure(
        filename="bad.xml",
        error_message="File was not parsed (XML parsing failed)",
        error_stage="parsing",
    )

    summary = tracker.get_summary()
    assert "❌ [PARSING] File was not parsed (XML parsing failed)" in summary


def test_summary_failure_lines_use_ascii_icon_when_enabled(monkeypatch):
    """Failure lines should switch to ASCII icon when SOLRINDEXER_ASCII_ICONS=1."""
    monkeypatch.setenv("SOLRINDEXER_ASCII_ICONS", "1")

    tracker = FailureTracker()
    tracker.add_failure(
        filename="bad.xml",
        error_message="File was not parsed (XML parsing failed)",
        error_stage="parsing",
    )

    summary = tracker.get_summary()
    assert "[FAIL] [PARSING] File was not parsed (XML parsing failed)" in summary


def test_summary_contains_warnings_for_indexed_documents():
    """Warnings should appear in summary for documents without failures."""
    tracker = FailureTracker()
    tracker.add_warning(
        filename="indexed_file.xml",
        warning_message="mmd:collection has non-controlled value: ADC",
        warning_stage="validation",
        metadata_identifier="indexed_id_001",
    )

    summary = tracker.get_summary()
    assert "WARNING(S)" in summary
    assert "FILE: indexed_file.xml [indexed_id_001]" in summary
    assert "[VALIDATION] mmd:collection has non-controlled value: ADC" in summary


def test_summary_includes_warnings_for_failed_documents():
    """Warnings are included even when the same file also has failures."""
    tracker = FailureTracker()
    tracker.add_warning(
        filename="failed_file.xml",
        warning_message="mmd:collection has non-controlled value: ADC",
        warning_stage="validation",
        metadata_identifier="failed_id_001",
    )
    tracker.add_failure(
        filename="failed_file.xml",
        error_message="Solr indexing failed",
        error_stage="indexing",
        metadata_identifier="failed_id_001",
    )

    summary = tracker.get_summary()
    assert "FILE: failed_file.xml [failed_id_001]" in summary
    assert "[INDEXING] Solr indexing failed" in summary
    assert "[VALIDATION] mmd:collection has non-controlled value: ADC" in summary


def test_failure_record_with_partial_data():
    """Test FailureRecord with minimal data."""
    record = FailureRecord(filename="test.xml")
    assert record.filename == "test.xml"
    assert record.metadata_identifier is None
    assert record.error_message == ""
    assert record.error_stage == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
