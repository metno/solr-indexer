from types import SimpleNamespace

import pytest

from solrindexer.search import _format_xml_for_display, build_search_request, parse_cfg


@pytest.mark.searchindex
def testParseCfg():
    assert parse_cfg


@pytest.mark.searchindex
def test_build_search_request_default_mode():
    args = SimpleNamespace(string="id:test", mmd=False)

    params = build_search_request(args)

    assert params["q"] == "id:test"
    assert params["wt"] == "json"
    assert params["rows"] == 10
    assert "mmd_xml_file:[xml]" not in params["fl"]


@pytest.mark.searchindex
def test_build_search_request_mmd_mode():
    args = SimpleNamespace(string="id:test", mmd=True)

    params = build_search_request(args)

    assert params == {
        "q": "id:test",
        "wt": "xml",
        "rows": 10,
        "fl": "mmd_xml_file:[xml]",
    }


@pytest.mark.searchindex
def test_format_xml_for_display_pretty_prints_xml():
    xml_text = (
        "<response><result><doc><str name='mmd_xml_file'>value</str></doc></result></response>"
    )

    formatted = _format_xml_for_display(xml_text)

    assert formatted.startswith("<response>")
    assert "\n  <result>" in formatted
