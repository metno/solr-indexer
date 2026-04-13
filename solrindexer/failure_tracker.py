"""
SOLR-indexer : Failure Tracking
================================

Copyright MET Norway

Licensed under the GNU GENERAL PUBLIC LICENSE, Version 3; you may not
use this file except in compliance with the License. You may obtain a
copy of the License at

    https://www.gnu.org/licenses/gpl-3.0.en.html

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.
"""

import logging
import threading
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class FailureRecord:
    """Record of a single document processing failure."""
    filename: str
    metadata_identifier: str = None
    error_message: str = ""
    error_stage: str = ""  # e.g., "parsing", "validation", "conversion", "indexing"


@dataclass
class WarningRecord:
    """Record of a single document processing warning."""
    filename: str
    metadata_identifier: str = None
    warning_message: str = ""
    warning_stage: str = ""  # e.g., "validation", "conversion"


@dataclass
class FailureTracker:
    """Centralized tracker for all document processing failures."""
    failures: list[FailureRecord] = field(default_factory=list)
    warnings: list[WarningRecord] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def add_failure(self, filename: str, error_message: str, error_stage: str = "",
                    metadata_identifier: str = None) -> None:
        """
        Record a processing failure.

        Parameters
        ----------
        filename : str
            Path to the file that failed
        error_message : str
            Description of what went wrong
        error_stage : str
            Stage where failure occurred (parsing, validation, conversion, thumbnail, indexing)
        metadata_identifier : str, optional
            The metadata_identifier if it was successfully extracted
        """
        record = FailureRecord(
            filename=filename,
            metadata_identifier=metadata_identifier,
            error_message=error_message,
            error_stage=error_stage
        )
        with self._lock:
            self.failures.append(record)

    def add_warning(self, filename: str, warning_message: str, warning_stage: str = "",
                    metadata_identifier: str = None) -> None:
        """Record a processing warning."""
        record = WarningRecord(
            filename=filename,
            metadata_identifier=metadata_identifier,
            warning_message=warning_message,
            warning_stage=warning_stage,
        )
        with self._lock:
            self.warnings.append(record)

    def get_summary(self) -> str:
        """
        Generate a human-readable summary of failures and warnings.

        Groups failures by filename, showing all stages that failed for each file.

        Returns
        -------
        str
            Formatted summary report
        """
        if not self.failures and not self.warnings:
            return "No failures or warnings recorded during processing."

        summary_lines = []
        summary_lines.append("")
        summary_lines.append("=" * 80)
        failed_files = len({f.filename for f in self.failures})
        warned_files = len({w.filename for w in self.warnings})
        summary_lines.append(
            f"PROCESSING SUMMARY - {len(self.failures)} FAILURE(S) in {failed_files} file(s), "
            f"{len(self.warnings)} WARNING(S) in {warned_files} file(s)"
        )
        summary_lines.append("=" * 80)
        summary_lines.append("")

        if self.failures:
            # Group failures by filename for better readability
            by_filename = {}
            for failure in self.failures:
                if failure.filename not in by_filename:
                    by_filename[failure.filename] = []
                by_filename[failure.filename].append(failure)

            # Sort filenames for consistent output
            for filename in sorted(by_filename.keys()):
                file_failures = by_filename[filename]

                # Get any metadata_identifier from the failures (should be same for all stages of same file)
                metadata_id = None
                for failure in file_failures:
                    if failure.metadata_identifier:
                        metadata_id = failure.metadata_identifier
                        break

                # Build header line
                id_str = f" [{metadata_id}]" if metadata_id else ""
                summary_lines.append(f"FILE: {filename}{id_str}")
                summary_lines.append("-" * 80)

                # List all stages and errors for this file
                for failure in sorted(file_failures, key=lambda f: f.error_stage or ""):
                    stage = failure.error_stage or "unknown"
                    summary_lines.append(f"  [{stage.upper()}] {failure.error_message}")

                summary_lines.append("")

        if self.warnings:
            summary_lines.append("WARNING(S)")
            summary_lines.append("=" * 80)

            by_warning_file = {}
            for warning in self.warnings:
                if warning.filename not in by_warning_file:
                    by_warning_file[warning.filename] = []
                by_warning_file[warning.filename].append(warning)

            for filename in sorted(by_warning_file.keys()):
                file_warnings = by_warning_file[filename]

                metadata_id = None
                for warning in file_warnings:
                    if warning.metadata_identifier:
                        metadata_id = warning.metadata_identifier
                        break

                id_str = f" [{metadata_id}]" if metadata_id else ""
                summary_lines.append(f"FILE: {filename}{id_str}")
                summary_lines.append("-" * 80)

                for warning in sorted(file_warnings, key=lambda w: w.warning_stage or ""):
                    stage = warning.warning_stage or "unknown"
                    summary_lines.append(f"  [{stage.upper()}] {warning.warning_message}")

                summary_lines.append("")

        summary_lines.append("=" * 80)
        return "\n".join(summary_lines)

    def log_summary(self) -> None:
        """Log the failure summary to the logger."""
        summary = self.get_summary()
        if len(self.failures) > 0 or len(self.warnings) > 0:
            logger.info(summary)
        else:
            logger.info("No failures or warnings recorded during processing.")

    def get_failures_by_stage(self, stage: str) -> list[FailureRecord]:
        """
        Get all failures for a specific stage.

        Parameters
        ----------
        stage : str
            The processing stage

        Returns
        -------
        List[FailureRecord]
            List of failures at that stage
        """
        return [f for f in self.failures if f.error_stage == stage]

    def get_warnings_by_stage(self, stage: str) -> list[WarningRecord]:
        """Get all warnings for a specific stage."""
        return [w for w in self.warnings if w.warning_stage == stage]
