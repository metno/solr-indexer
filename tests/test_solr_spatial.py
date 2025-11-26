# solrindexer/tools/test_mmd_solr_spatial.py

import pytest

from solrindexer.tools.mmd_solr_spatial import (
    create_polygon_wkt_from_bbox,
    generate_solr_envelope,
    parse_envelope_to_bbox,
    wkt_to_segmetized_geojson,
)


def test_generate_solr_envelope_valid():
    result = generate_solr_envelope(60, 10, 50, 0)
    assert result == "ENVELOPE(0, 10, 60, 50)"

    result = generate_solr_envelope(90, 180, -90, -180)
    assert result == "ENVELOPE(-180, 180, 90, -90)"

    result = generate_solr_envelope(90, -180, -90, 180)
    assert result == "ENVELOPE(180, -180, 90, -90)"

    result = generate_solr_envelope(90, 0, -90, 160)
    assert result == "ENVELOPE(160, 0, 90, -90)"


@pytest.mark.parametrize(
    "north,south,east,west",
    [
        (91, 50, 10, 0),
        (60, -91, 10, 0),
        (60, 50, 181, 0),
        (60, 50, 10, -181),
    ],
)
def test_generate_solr_envelope_out_of_bounds(north, south, east, west):
    with pytest.raises(ValueError):
        generate_solr_envelope(north, east, south, west)


def test_generate_solr_envelope_south_greater_than_north():
    with pytest.raises(ValueError):
        generate_solr_envelope(50, 10, 60, 0)


def test_create_polygon_wkt_from_bbox_valid():
    wkt, centroid = create_polygon_wkt_from_bbox(60, 10, 50, 0)
    assert wkt.startswith("POLYGON")
    assert centroid.startswith("POINT")

    from shapely.wkt import loads

    geom = loads(wkt)
    assert geom.exterior.is_ccw is True


def test_create_polygon_wkt_from_bbox_point():
    wkt, centroid = create_polygon_wkt_from_bbox(10, 20, 10, 20)
    assert wkt.startswith("POINT")
    assert centroid.startswith("POINT")


def test_create_polygon_wkt_from_bbox_idl_crossing():
    wkt, centroid = create_polygon_wkt_from_bbox(60, -170, 50, 170)
    assert wkt.startswith("POLYGON")
    assert centroid.startswith("POINT")
    # Should cross IDL, so east > 180 in the polygon coordinates
    assert any(
        float(coord) > 180 for coord in wkt.replace("POLYGON ((", "").replace("))", "").replace(",", " ").split()
    )

    from shapely.wkt import loads

    geom = loads(wkt)
    assert geom.exterior.is_ccw is False


@pytest.mark.parametrize(
    "north,south,east,west",
    [
        (91, 50, 10, 0),
        (60, -91, 10, 0),
        (60, 50, 181, 0),
        (60, 50, 10, -181),
    ],
)
def test_create_polygon_wkt_from_bbox_out_of_bounds(north, south, east, west):
    with pytest.raises(ValueError):
        create_polygon_wkt_from_bbox(north, east, south, west)


def test_create_polygon_wkt_from_bbox_south_greater_than_north():
    with pytest.raises(ValueError):
        create_polygon_wkt_from_bbox(50, 10, 60, 0)


def test_parse_envelope_to_bbox_valid():
    bbox = parse_envelope_to_bbox("ENVELOPE(0, 10, 60, 50)")
    assert bbox == (60.0, 10.0, 50.0, 0.0)


def test_parse_envelope_to_bbox_invalid_format():
    with pytest.raises(ValueError):
        parse_envelope_to_bbox("ENVELOPE(0, 10, 60)")


def test_parse_envelope_to_bbox_non_numeric():
    with pytest.raises(ValueError):
        parse_envelope_to_bbox("ENVELOPE(a, 10, 60, 50)")


def test_parse_envelope_to_bbox_out_of_bounds():
    with pytest.raises(ValueError):
        parse_envelope_to_bbox("ENVELOPE(0, 200, 60, 50)")
    with pytest.raises(ValueError):
        parse_envelope_to_bbox("ENVELOPE(0, 10, 100, 50)")


def test_wkt_to_segmetized_geojson_polygon():
    wkt, _ = create_polygon_wkt_from_bbox(60, 10, 50, 0)
    geojson = wkt_to_segmetized_geojson(wkt)
    assert "Polygon" in geojson


def test_wkt_to_segmetized_geojson_linestring():
    from shapely.geometry import LineString

    line = LineString([(0, 0), (1, 1)])
    wkt = line.wkt
    geojson = wkt_to_segmetized_geojson(wkt)
    assert "LineString" in geojson


def test_wkt_to_segmetized_geojson_point():
    from shapely.geometry import Point

    wkt = Point(1, 2).wkt
    geojson = wkt_to_segmetized_geojson(wkt)
    assert "Point" in geojson
