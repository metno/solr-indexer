import logging
import math

import numpy as np
import pygml
import shapely
from pygeoif import shape
from shapely import GEOSException, Point, to_geojson, to_wkt
from shapely.geometry import MultiPolygon, Polygon, box
from shapely.geometry.polygon import orient
from shapely.ops import unary_union
from shapely.validation import explain_validity
from shapely.wkt import loads

logger = logging.getLogger(__name__)


def handle_solr_spatial(solr_doc, north, east, south, west, gml=None, srs=None):
    """
    Handles processing of geospatial information from MMD,
    and mapping to solr fields. Handles mmd:rectangle and mmd:polygon

    The north, east, south, west coordinates are used to store and index a
    bbox using solr.BBoxField. using the OGC CQL/WKT ENVELOPE(minX,maxX,maxY,minY).
    if maxX < minX, dateline crossing is assumed.

    If mmd:polygon is provided as gml, this is parsed, validated, and indexed
    to the solr RecursivePrefixTree field type. Solr reads the inside of a
    polygon from the direction of its exterior ring, so rings are oriented
    first (see orient_polygon_for_solr): a clockwise ring would index the
    rest of the Earth instead of the dataset's footprint.

    Also we store WKT and GeoJSON string representation of the geometries.
    Those are used for visualization of the geometries in Openlayers/Leaflet etc.

    If we only have a rectangular polygon geometry, we segmetize this geometry for
    the string representation to make for better visualizaiton.
    """
    # Add bbox field
    logger.debug("Adding solr spatial fields and geometries")
    logger.debug(
        "Got geographic rectangle north: %s, east: %s, south: %s, west: %s",
        north,
        east,
        south,
        west,
    )
    solr_doc["bbox"] = generate_solr_envelope(north, east, south, west)
    if gml is not None:
        # Use the provided GML geometry for indexing.
        logger.debug("Got GML Geometry, parsing and processing")
        geom_wkt = parse_gml_to_wkt(gml)
        oriented = orient_polygon_for_solr(geom_wkt)
        if oriented is None:
            # Not a polygon (point, line): index it as given.
            geom_wkt = validate_fix_geometry(geom_wkt)
            index_wkt = display_wkt = geom_wkt
        elif isinstance(oriented, str):
            # A ring around a pole: index the polar cap it encloses.
            index_wkt, display_wkt = oriented, geom_wkt
        else:
            index_wkt, display_wkt = oriented
        solr_doc["geospatial_bounds3d"] = index_wkt
        if (west == -180.0 and east == 180.0) or (south == -90.0 and north == 90.0):
            logger.debug("Spanning whole longitude using ENVELOPE syntax to avoid coplanar error")
            solr_doc["geospatial_bounds3d"] = solr_doc["bbox"]
        solr_doc["geometry_wkt"] = display_wkt
        solr_doc["geometry_geojson"] = wkt_to_geojson(display_wkt)
        return solr_doc

    if gml is None:
        geom_solr_wkt, _center = create_polygon_wkt_from_bbox(north, east, south, west)
        if geom_solr_wkt.startswith("POINT"):
            solr_doc["geospatial_bounds3d"] = geom_solr_wkt
        else:
            solr_doc["geospatial_bounds3d"] = solr_doc["bbox"]

        if (west == -180.0 and east == 180.0) or (south == -90.0 and north == 90.0):
            logger.debug("Spanning whole longitude using ENVELOPE syntax to avoid coplanar error")
            solr_doc["geospatial_bounds3d"] = solr_doc["bbox"]

        # Handle denormalizing when crossing dateline for openlayers to understand
        if east < west:
            east += 360
        geom_wkt_string, _center = create_polygon_wkt_from_bbox(north, east, south, west)
        solr_doc["geometry_wkt"] = wkt_rect_to_segmetized_geom(geom_wkt_string, output="WKT")
        solr_doc["geometry_geojson"] = wkt_rect_to_segmetized_geom(
            geom_wkt_string, output="GeoJSON"
        )

    return solr_doc


def parse_gml_to_wkt(gml):
    """Parses a gml geometry xml string and return WKT"""
    geom = pygml.parse(gml)
    return shape(geom).wkt


def _lon_step(lon_from, lon_to):
    """Longitude change along the shortest path between two meridians, in [-180, 180)."""
    return (lon_to - lon_from + 180.0) % 360.0 - 180.0


def unwrap_ring(coords):
    """
    Unwrap the longitudes of a ring so that no edge is longer than 180 degrees.

    Consecutive vertices are joined along the shortest path, so the edge from
    170 to -170 crosses the antimeridian (20 degrees) rather than going round
    the globe (340 degrees). In the unwrapped coordinates that edge runs from
    170 to 190, and planar geometry (orientation, validity) applies again.

    :param coords: ring coordinates (lon, lat), closed or not
    :return: (unwrapped open ring as a list of (lon, lat), winding) where the
             winding is the total longitude change around the ring: 0 for an
             ordinary ring, +360 or -360 for a ring that goes once round a pole
    """
    points = [(float(c[0]), float(c[1])) for c in coords]
    if len(points) > 1 and points[0] == points[-1]:
        points = points[:-1]
    unwrapped = [points[0]]
    for lon, lat in points[1:]:
        previous = unwrapped[-1][0]
        # whole turns only, so coordinates that need no shift stay exact
        unwrapped.append((lon + 360.0 * round((previous - lon) / 360.0), lat))
    closing = _lon_step(unwrapped[-1][0], unwrapped[0][0])
    winding = round(unwrapped[-1][0] + closing - unwrapped[0][0])
    return unwrapped, winding


def _wrap_lons(xy):
    """Coordinate array with longitudes back in [-180, 180], keeping 180 as 180."""
    lon = xy[:, 0]
    turns = np.where(lon > 180.0, -360.0, np.where(lon < -180.0, 360.0, 0.0))
    return np.column_stack([lon + turns, xy[:, 1]])


def _polar_cap_envelope(ring, winding):
    """
    ENVELOPE of the polar cap enclosed by a ring that goes once round a pole.

    The pole is on the side the ring encloses: an anticlockwise ring goes
    eastward round the north pole and westward round the south pole.
    """
    lats = [lat for _lon, lat in ring]
    north_pole = sum(lats) / len(lats) >= 0
    if (winding > 0) != north_pole:
        logger.warning(
            "Ring round the %s pole is clockwise; indexing the polar cap it describes",
            "north" if north_pole else "south",
        )
    if north_pole:
        return generate_solr_envelope(90.0, 180.0, min(lats), -180.0)
    return generate_solr_envelope(max(lats), 180.0, -90.0, -180.0)


def _orient_polygon(polygon):
    """
    Orient one shapely Polygon in unwrapped coordinates.

    :return: the oriented Polygon or MultiPolygon in unwrapped longitudes
             (exterior anticlockwise, holes clockwise), or an ENVELOPE string
             when the exterior ring goes round a pole
    """
    exterior, winding = unwrap_ring(polygon.exterior.coords)
    if winding != 0:
        return _polar_cap_envelope(exterior, winding)
    holes = []
    for interior in polygon.interiors:
        ring, _winding = unwrap_ring(interior.coords)
        # Place the hole in the same 360 degree window as the exterior.
        shift = 360.0 * round((exterior[0][0] - ring[0][0]) / 360.0)
        holes.append([(lon + shift, lat) for lon, lat in ring])
    unwrapped = Polygon(exterior, holes)
    if not unwrapped.is_valid:
        unwrapped = loads(validate_fix_geometry(unwrapped.wkt))
    if unwrapped.geom_type == "MultiPolygon":
        return MultiPolygon([orient(part, sign=1.0) for part in unwrapped.geoms])
    return orient(unwrapped, sign=1.0)


def orient_polygon_for_solr(geom_wkt):
    """
    Orient a polygon the way Solr's spatial fields expect.

    Solr (Spatial4j / Geo3D) takes the inside of a polygon to be on the left of
    its exterior ring. Given clockwise, a footprint becomes "the whole Earth
    except the footprint" and matches every search, wherever its box is.

    Orientation is judged along the shortest path between vertices, so a
    polygon crossing the antimeridian (an edge from 170 to -170) is handled like
    any other. That is the same rule create_polygon_wkt_from_bbox follows when
    it orients a dateline-crossing box clockwise in plain longitudes.

    :param geom_wkt: WKT of the parsed mmd:polygon, longitudes in [-180, 180]
    :return: None if the geometry is not a (Multi)Polygon;
             an ENVELOPE string for a ring that goes round a pole;
             else (index_wkt, display_wkt): index_wkt with longitudes in
             [-180, 180] for geospatial_bounds3d, display_wkt with the
             antimeridian crossing denormalized (longitudes above 180) for
             geometry_wkt / geometry_geojson, as the bbox path does
    """
    geom = loads(geom_wkt)
    if geom.geom_type == "Polygon":
        parts = [geom]
    elif geom.geom_type == "MultiPolygon":
        parts = list(geom.geoms)
    else:
        return None

    oriented = []
    for part in parts:
        result = _orient_polygon(part)
        if isinstance(result, str):
            if len(parts) > 1:
                logger.warning("Polar ring inside a MultiPolygon; indexing its polar cap only")
            return result
        oriented.extend(result.geoms if result.geom_type == "MultiPolygon" else [result])

    display = oriented[0] if len(oriented) == 1 else MultiPolygon(oriented)
    if display.bounds[0] < -180.0:
        display = shapely.transform(display, lambda xy: xy + [360.0, 0.0])
    index = shapely.transform(display, _wrap_lons)

    if index.geom_type == geom.geom_type and index.equals_exact(geom, 1e-9):
        # Already oriented and valid: keep the WKT exactly as parsed.
        index_wkt = geom_wkt
    else:
        logger.info("Polygon reoriented (or repaired) for Solr: %s", geom_wkt[:120])
        index_wkt = index.wkt
    display_wkt = index_wkt if display.equals_exact(index, 1e-9) else display.wkt
    return index_wkt, display_wkt


def generate_solr_envelope(north, east, south, west):
    """
    Generate a CQL ENVELOPE(minX, maxX, maxY, minY) string from given
    north, south, east, and west coordinates.

    Expect coordinates int WGS84 (EPSG:4326). Latitude [-90, 90] and Longitude [-180, 180]

    :param north: Northern latitude (float)
    :param east: Eastern longitude (float)
    :param south: Southern latitude (float)
    :param west: Western longitude (float)
    :return: ENVELOPE string (str)
    :raises ValueError: If the coordinates are invalid
    """
    # Validate that the input coordinates are within EPSG:4326 bounds
    if not (-90 <= south <= 90):
        raise ValueError(f"South latitude {south} is out of bounds. Must be between -90 and 90.")
    if not (-90 <= north <= 90):
        raise ValueError(f"North latitude {north} is out of bounds. Must be between -90 and 90.")
    if not (-180 <= west <= 180):
        raise ValueError(f"West longitude {west} is out of bounds. Must be between -180 and 180.")
    if not (-180 <= east <= 180):
        raise ValueError(f"East longitude {east} is out of bounds. Must be between -180 and 180.")
    if south > north:
        raise ValueError(f"South latitude {south} cannot be greater than North latitude {north}.")

    return f"ENVELOPE({west}, {east}, {north}, {south})"


def create_polygon_wkt_from_bbox(north, east, south, west):
    """
    Create a WKT polygon from WGS84 bounding box (north, south, east, west),
    validate the coordinates are within EPSG:4326 bounds, and ensure CCW orientation.
    Latitude [-90, 90] and Longitude [-180, 180]

    :param north: Northern latitude (float)
    :param east: Eastern longitude (float)
    :param south: Southern latitude (float)
    :param west: Western longitude (float)
    :return: WKT representation of the polygon (str)
    :raises ValueError: If the coordinates are out of EPSG:4326 bounds
    """

    # Validate that the input coordinates are within EPSG:4326 bounds
    if not (-90 <= south <= 90):
        raise ValueError(f"South latitude {south} is out of bounds. Must be between -90 and 90.")
    if not (-90 <= north <= 90):
        raise ValueError(f"North latitude {north} is out of bounds. Must be between -90 and 90.")
    if south > north:
        raise ValueError(f"South latitude {south} cannot be greater than North latitude {north}.")

    # Handle point geometry.
    if north == south and east == west:
        point = Point(east, north)
        centroid = point.centroid
        return point.wkt, centroid.wkt

    # Crossing the International Dateline (IDL):
    # denormalize east coordinate so renderers draw the shape in the expected world.
    crossing = False  # IDL crossing information

    if east < west:
        crossing = True

    # Create a bounding box (minx, miny, maxx, maxy) which is (west, south, east, north)
    bbox = box(east, south, west, north) if crossing else box(west, south, east, north)

    # Solr expects CCW for non-crossing polygons and CW when crossing IDL.
    polygon = orient(bbox, sign=-1.0) if crossing else orient(bbox, sign=1.0)

    # Extract the centeroid
    centroid = polygon.centroid

    # Return the WKT representation of the polygon
    return polygon.wkt, centroid.wkt


def parse_envelope_to_bbox(envelope_wkt):
    """
    Parse a WKT/CQL ENVELOPE geometry, normalize the coordinates, and return a polygon
    spanning the shortest distance across the dateline if necessary.

    Expect coordinates int WGS84 (EPSG:4326). Latitude [-90, 90] and Longitude [-180, 180]

    :param envelope_wkt: A string in ENVELOPE(minX, maxX, maxY, minY) format
    :return: A tuple of (north, south, east, west) as floats rounded to 7 decimals
    """
    # Check if the input is in ENVELOPE format
    if not envelope_wkt.startswith("ENVELOPE(") or not envelope_wkt.endswith(")"):
        raise ValueError(
            "Invalid ENVELOPE syntax. Expected format: ENVELOPE(minX, maxX, maxY, minY)"
        )

    # Extract the coordinates
    coords = envelope_wkt.replace("ENVELOPE(", "").replace(")", "").split(",")
    if len(coords) != 4:
        raise ValueError("Invalid ENVELOPE syntax. Expected 4 coordinates: minX, maxX, maxY, minY")

    # Parse the coordinates
    try:
        minX, maxX, maxY, minY = map(float, coords)
    except ValueError:
        raise ValueError(
            "Invalid coordinates in ENVELOPE syntax. Ensure they are numeric."
        ) from ValueError

    # Validate coordinates
    if not (-90 <= minY <= 90 and -90 <= maxY <= 90):
        raise ValueError("Latitude values must be within the range [-90, 90].")
    if not (-180 <= minX <= 180 and -180 <= maxX <= 180):
        raise ValueError("Longitude values must be within the range [-180, 180].")

    # north, east, south, west
    return round(maxY, 9), round(maxX, 9), round(minY, 9), round(minX, 9)


def wkt_rect_to_segmetized_geom(wkt, segments=3, output="WKT"):
    """
    Reads a WKT string and output as WKT or GeoJSON depending on output. Deafult WKT

    If the geomtry is a polygon and it is rectangular, then we segmentize the points for
    more accurate visualization and reprojection.
    """
    geom = loads(wkt)
    segmitize = False
    if geom.geom_type == "Polygon" and math.isclose(
        geom.minimum_rotated_rectangle.area, geom.area
    ):
        segmitize = True
    if geom.geom_type in ("LineString", "MultiLineString"):
        segmitize = True

    if segmitize is True:
        segmetized_geom = geom.segmentize(segments)

    if segmitize is True:
        if output == "GeoJSON":
            return to_geojson(segmetized_geom)
        return to_wkt(segmetized_geom)
    if output == "GeoJSON":
        return to_geojson(geom)
    return to_wkt(geom)


def wkt_to_geojson(wkt):
    """Reads a WKT Geometry and return the GeoJSON equivalent"""
    geom = loads(wkt)
    return to_geojson(geom)


def get_center_from(wkt):
    """Get the center point WKT from geometry"""
    geom = loads(wkt)
    center = geom.centroid
    return to_wkt(center)


def validate_fix_geometry(geom_wkt):
    """
    Fix invalid geometry using the buffer(0) trick. and Simplify

    :param geom: A WKT Geometry
    :return: A valid WKT Geometry
    """
    geom = loads(geom_wkt)
    if not geom.is_valid:
        explain = explain_validity(geom)
        logger.warning("Invalid geometry..trying to repair and simplify: %s", explain)
        try:
            # Apply buffer(0) trick to fix the geometry
            fixed_geom = unary_union(geom.buffer(0).simplify(1))
            if fixed_geom.is_valid:
                logger.debug("Geometry fixed successfully.")
                return to_wkt(fixed_geom)
            logger.error("Failed to fix Geometry")
        except GEOSException as e:
            logger.error("Failed to fix geometry: %s", e)
            return geom_wkt
        except Exception as e:
            logger.error("Failed to fix geometry: %s", e)
            return geom_wkt
    return geom_wkt
