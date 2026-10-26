"""Tests for environment-controlled console and file logging."""

from __future__ import annotations

import builtins
import logging
import sys
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from solrindexer import _init_logging


@pytest.fixture(name="log_obj")
def logging_fixture(monkeypatch: pytest.MonkeyPatch) -> Iterator[logging.Logger]:
    """Provide an isolated logger and deterministic logging environment."""
    monkeypatch.setenv("SOLRINDEXER_LOGLEVEL", "DEBUG")
    monkeypatch.delenv("SOLRINDEXER_LOGFILE", raising=False)
    monkeypatch.delenv("SOLRINDEXER_LOG_RICH", raising=False)
    logger = logging.Logger("solrindexer.test")
    yield logger
    for handler in logger.handlers:
        handler.close()
    logger.handlers.clear()


@pytest.mark.parametrize("value", ["0", "false", "no", "off", " FALSE "])
def test_disabled_rich_uses_plain_handlers_without_importing_rich(
    log_obj: logging.Logger, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """Disabling Rich bypasses its imports, even when it is installed."""
    monkeypatch.setenv("SOLRINDEXER_LOG_RICH", value)
    with patch("builtins.__import__", wraps=builtins.__import__) as import_mock:
        _init_logging(log_obj)
    assert not any(call.args[0].startswith("rich") for call in import_mock.call_args_list)
    assert [type(handler) for handler in log_obj.handlers] == [
        logging.StreamHandler,
        logging.StreamHandler,
    ]


@pytest.mark.parametrize("value", [None, "1", "true", "yes", "on", " TRUE "])
def test_enabled_rich_and_default_use_rich_handlers(
    log_obj: logging.Logger, monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    """Keep existing Rich behavior by default and for explicit true values."""
    rich_logging = pytest.importorskip("rich.logging")
    if value is not None:
        monkeypatch.setenv("SOLRINDEXER_LOG_RICH", value)
    _init_logging(log_obj)
    assert len(log_obj.handlers) == 2
    assert all(isinstance(handler, rich_logging.RichHandler) for handler in log_obj.handlers)


def test_missing_rich_falls_back_to_plain_handlers(log_obj: logging.Logger) -> None:
    """Requesting Rich still works when the optional dependency is absent."""
    with patch.dict(sys.modules, {"rich.console": None, "rich.logging": None}):
        _init_logging(log_obj)
    assert [type(handler) for handler in log_obj.handlers] == [
        logging.StreamHandler,
        logging.StreamHandler,
    ]


@pytest.mark.parametrize("value", ["", "invalid"])
def test_invalid_rich_value_warns_and_keeps_default(
    log_obj: logging.Logger,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    value: str,
) -> None:
    """Invalid values are reported rather than silently changing logging."""
    rich_logging = pytest.importorskip("rich.logging")
    monkeypatch.setenv("SOLRINDEXER_LOG_RICH", value)
    _init_logging(log_obj)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "SOLRINDEXER_LOG_RICH" in captured.err
    assert "using default '1'" in captured.err
    assert all(isinstance(handler, rich_logging.RichHandler) for handler in log_obj.handlers)


@pytest.mark.parametrize("rich_value", ["0", "1"])
def test_logging_preserves_stream_routing_and_plain_logfile(
    log_obj: logging.Logger,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    rich_value: str,
) -> None:
    """Console routing and plain file output stay unchanged in either mode."""
    monkeypatch.setenv("SOLRINDEXER_LOG_RICH", rich_value)
    log_file = tmp_path / "indexer.log"
    monkeypatch.setenv("SOLRINDEXER_LOGFILE", str(log_file))
    _init_logging(log_obj)
    messages = ["debug-entry", "info-entry", "warning-entry", "error-entry"]
    for level, message in zip(
        [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], messages
    ):
        log_obj.log(level, message)
    captured = capsys.readouterr()
    for message in messages[:2]:
        assert captured.out.count(message) == 1
        assert message not in captured.err
    for message in messages[2:]:
        assert captured.err.count(message) == 1
        assert message not in captured.out
    file_text = log_file.read_text(encoding="utf-8")
    for message in messages:
        assert file_text.count(message) == 1
    assert "\x1b[" not in file_text


def test_plain_logging_does_not_wrap_long_messages(
    log_obj: logging.Logger,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Batch logs retain the full message on one line regardless of width."""
    monkeypatch.setenv("SOLRINDEXER_LOG_RICH", "0")
    monkeypatch.setenv("COLUMNS", "40")
    _init_logging(log_obj)
    message = "long-message-" * 200
    log_obj.info(message)
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.count(message) == 1
    assert len(captured.out.splitlines()) == 1
    assert "\x1b[" not in captured.out
