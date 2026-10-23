# solrindexer/tools/test_mmd_solr_spatial.py

import pytest

from solrindexer.spatial import (
    create_polygon_wkt_from_bbox,
    generate_solr_envelope,
    parse_envelope_to_bbox,
    wkt_rect_to_segmetized_geom,
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
    # IDL crossing is represented by orientation here; denormalization to >180
    # happens later in handle_solr_spatial for geometry output.
    assert all(
        -180 <= float(coord) <= 180
        for coord in wkt.replace("POLYGON ((", "").replace("))", "").replace(",", " ").split()
    )

    from shapely.wkt import loads

    geom = loads(wkt)
    assert geom.exterior.is_ccw is False


@pytest.mark.parametrize(
    "north,south,east,west",
    [
        (91, 50, 10, 0),
        (60, -91, 10, 0),
    ],
)
def test_create_polygon_wkt_from_bbox_out_of_bounds(north, south, east, west):
    with pytest.raises(ValueError):
        create_polygon_wkt_from_bbox(north, east, south, west)


@pytest.mark.parametrize(
    "north,south,east,west",
    [
        (60, 50, 181, 0),
        (60, 50, 10, -181),
    ],
)
def test_create_polygon_wkt_from_bbox_allows_extended_longitudes(north, south, east, west):
    wkt, centroid = create_polygon_wkt_from_bbox(north, east, south, west)

    assert wkt.startswith("POLYGON")
    assert centroid.startswith("POINT")


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
    geojson = wkt_rect_to_segmetized_geom(wkt)
    assert "POLYGON" in geojson


def test_wkt_to_segmetized_geojson_linestring():
    from shapely.geometry import LineString

    line = LineString([(0, 0), (1, 1)])
    wkt = line.wkt
    geojson = wkt_rect_to_segmetized_geom(wkt)
    assert "LINESTRING" in geojson


def test_wkt_to_segmetized_geojson_point():
    from shapely.geometry import Point

    wkt = Point(1, 2).wkt
    geojson = wkt_rect_to_segmetized_geom(wkt)
    assert "POINT" in geojson


# Polygon orientation for Solr (mmd:polygon). Solr takes the inside of a
# polygon to be on the left of its exterior ring: a clockwise ring indexes the
# rest of the Earth and matches every bounding-box search.

GML_NS = "http://www.opengis.net/gml"


def _gml(points_latlon, holes=()):
    """GML polygon as MMD writes it: EPSG:4326, each position as "lat lon"."""

    def ring(points):
        pos = "".join(f"<gml:pos>{lat} {lon}</gml:pos>" for lat, lon in points)
        return f"<gml:LinearRing>{pos}</gml:LinearRing>"

    interiors = "".join(f"<gml:interior>{ring(h)}</gml:interior>" for h in holes)
    return (
        f'<gml:Polygon xmlns:gml="{GML_NS}" srsName="EPSG:4326">'
        f"<gml:exterior>{ring(points_latlon)}</gml:exterior>{interiors}</gml:Polygon>"
    )


def _rings(wkt):
    from shapely.wkt import loads

    return loads(wkt)


def _unwrapped_is_ccw(geom):
    from shapely.geometry import Polygon

    from solrindexer.spatial import unwrap_ring

    ring, winding = unwrap_ring(geom.exterior.coords)
    assert winding == 0
    return Polygon(ring).exterior.is_ccw


def _index(gml, north, east, south, west):
    from solrindexer.spatial import handle_solr_spatial

    return handle_solr_spatial({}, north, east, south, west, gml=gml)


# A Sentinel-1 GRDM frame from the NBS catalogue, corners as written (clockwise).
NBS_CLOCKWISE = [
    (65.381798, 9.476668),
    (65.835999, 4.134038),
    (67.431908, 4.803319),
    (66.960785, 10.486644),
    (65.381798, 9.476668),
]


def test_unwrap_ring_crossing_antimeridian():
    from solrindexer.spatial import unwrap_ring

    ring, winding = unwrap_ring([(170, 50), (-170, 50), (-170, 60), (170, 60), (170, 50)])
    assert winding == 0
    assert [lon for lon, _ in ring] == [170, 190, 190, 170]


def test_unwrap_ring_round_a_pole():
    from solrindexer.spatial import unwrap_ring

    eastward = [(lon, 80) for lon in (-180, -90, 0, 90)]
    assert unwrap_ring(eastward)[1] == 360
    assert unwrap_ring(list(reversed(eastward)))[1] == -360


def test_clockwise_polygon_is_reversed():
    doc = _index(_gml(NBS_CLOCKWISE), 67.431908, 10.486644, 65.381798, 4.134038)
    indexed = _rings(doc["geospatial_bounds3d"])
    assert indexed.exterior.is_ccw
    # same footprint, corners in the opposite order
    given = _rings(
        "POLYGON ((9.476668 65.381798, 4.134038 65.835999, 4.803319 67.431908, "
        "10.486644 66.960785, 9.476668 65.381798))"
    )
    assert indexed.equals(given)
    assert doc["geometry_wkt"] == doc["geospatial_bounds3d"]
    assert '"type":"Polygon"' in doc["geometry_geojson"]


def test_anticlockwise_polygon_is_kept_as_parsed():
    from solrindexer.spatial import parse_gml_to_wkt

    gml = _gml(list(reversed(NBS_CLOCKWISE)))
    doc = _index(gml, 67.431908, 10.486644, 65.381798, 4.134038)
    assert doc["geospatial_bounds3d"] == parse_gml_to_wkt(gml)
    assert doc["geometry_wkt"] == doc["geospatial_bounds3d"]


@pytest.mark.parametrize("clockwise", [True, False])
def test_antimeridian_polygon(clockwise):
    # 170E to 170W across the antimeridian. Anticlockwise on the globe is
    # 170 -> -170 along the south edge, i.e. clockwise in plain longitudes.
    anticlockwise = [(50, 170), (50, -170), (60, -170), (60, 170), (50, 170)]
    points = list(reversed(anticlockwise)) if clockwise else anticlockwise
    doc = _index(_gml(points), 60, -170, 50, 170)

    indexed = _rings(doc["geospatial_bounds3d"])
    assert all(-180 <= x <= 180 for x, _ in indexed.exterior.coords)
    assert _unwrapped_is_ccw(indexed)
    # the 20 degree box, not the 340 degree one
    assert {round(x) for x, _ in indexed.exterior.coords} == {170, -170}

    displayed = _rings(doc["geometry_wkt"])
    assert displayed.exterior.is_ccw
    assert displayed.bounds == (170.0, 50.0, 190.0, 60.0)


def test_antimeridian_polygon_not_mangled_by_repair():
    # In plain longitudes this ring crosses itself, so the old planar repair
    # (buffer(0)) turned it into a different shape. Unwrapped it is a valid
    # quadrilateral round the antimeridian.
    points = [(50, 175), (52, -175), (60, -178), (58, 178), (50, 175)]
    doc = _index(_gml(points), 60, -175, 50, 175)
    indexed = _rings(doc["geospatial_bounds3d"])
    assert len(indexed.exterior.coords) == 5
    assert _unwrapped_is_ccw(indexed)
    assert _rings(doc["geometry_wkt"]).bounds == (175.0, 50.0, 185.0, 60.0)


@pytest.mark.parametrize(
    "lats,expected",
    [
        ((80, 80, 80, 80), "ENVELOPE(-180.0, 180.0, 90.0, 80.0)"),
        ((-75, -75, -75, -75), "ENVELOPE(-180.0, 180.0, -75.0, -90.0)"),
    ],
)
def test_ring_round_a_pole_indexes_the_polar_cap(lats, expected):
    lons = (-170, -80, 10, 100)
    points = list(zip(lats, lons)) + [(lats[0], lons[0])]
    # rectangle not quite full-width, so the whole-longitude shortcut does not apply
    north, south = (90, 80) if lats[0] > 0 else (-75, -90)
    doc = _index(_gml(points), north, 179.9, south, -179.9)
    assert doc["geospatial_bounds3d"] == expected


def test_polygon_with_hole():
    outer = [(60, 0), (70, 0), (70, 10), (60, 10), (60, 0)]  # clockwise
    hole = [(62, 2), (62, 8), (68, 8), (68, 2), (62, 2)]  # anticlockwise
    doc = _index(_gml(outer, holes=[hole]), 70, 10, 60, 0)
    indexed = _rings(doc["geospatial_bounds3d"])
    assert indexed.exterior.is_ccw
    assert not indexed.interiors[0].is_ccw
    assert indexed.area == pytest.approx(100 - 36)
