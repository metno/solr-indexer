import logging
import math

import pygml
from pygeoif import shape
from shapely import GEOSException, Point, to_geojson, to_wkt
from shapely.geometry import box
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
    to the solr RecursivePrefixTree field type.

    Also we store WKT and GeoJSON string representation of the geometries.
    Those are used for visualization of the geometries in Openlayers/Leaflet etc.

    If we only have a rectangular polygon geometry, we segmetize this geometry for
    the string representation to make for better visualizaiton.
    """
    # Add bbox field
    logger.info("Adding solr spatial fields and geometries")
    logger.debug(f"North: {north}, East: {east}, South: {south}, West: {west}")
    solr_doc['bbox'] = generate_solr_envelope(north, east, south, west)
    if gml is not None:
        """
        Use the provided GML Geometry for indexing.
        """
        logger.info("Got GML Geometry, parsing and processing")
        geom_wkt = parse_gml_to_wkt(gml)
        geom_wkt = validate_fix_geometry(geom_wkt)
        solr_doc["geospatial_bounds3d"] = geom_wkt
        solr_doc['geometry_wkt'] = geom_wkt
        solr_doc["geometry_geojson"] = wkt_to_geojson(geom_wkt)
        return solr_doc

    if gml is None:
        geom_solr_wkt, center = create_polygon_wkt_from_bbox(north, east, south, west)
        if geom_solr_wkt.startswith('POINT'):
            solr_doc["geospatial_bounds3d"] = geom_solr_wkt
        else:
            solr_doc["geospatial_bounds3d"] = solr_doc['bbox']

        if west == -180 and east == 180:
            logger.debug("Spanning whole longitude using ENVELOPE syntax to avoid coplanar error")
            solr_doc["geospatial_bounds3d"] = solr_doc["bbox"]

        # Handle denormalizing when crossing dateline for openlayers to understand
        if east < west:
            east += 360
        geom_wkt_string, center = create_polygon_wkt_from_bbox(north, east, south, west)
        solr_doc['geometry_wkt'] = wkt_rect_to_segmetized_geom(geom_wkt_string, output="WKT")
        solr_doc["geometry_geojson"] = wkt_rect_to_segmetized_geom(geom_wkt_string, output="GeoJSON")


    return solr_doc



def parse_gml_to_wkt(gml):
    """Parses a gml geometry xml string and return WKT"""
    geom = pygml.parse(gml)
    return shape(geom).wkt


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

    """
    Handle Point Geometry
    """
    if north == south and east == west:
        point = Point(east, north)
        centroid = point.centroid
        return point.wkt, centroid.wkt

    """
    Crossing the International Dateline (IDL):

    When creating a rectangular polyogn, we need to denormalize the
    coordinates to explicitt cross the IDL so that Openlayers and Leaflet will draw the geometry correctly.

    We know we cross the IDL if west > east from the MMD. Thus we add 360 degrees to the east coordinate.
    This will make Openlayers understand to cross the IDL geometry feature from east to west into the "next world
    spanning more than 180 degrees. This will make Openlayers render the rectangular geometry figure correctly.

    """
    crossing = False  # IDL crossing information

    if east < west:
        crossing = True

    # Create a bounding box (minx, miny, maxx, maxy) which is (west, south, east, north)
    bbox = box(east, south, west, north) if crossing else box(west, south, east, north)

    """
    Solr handles datelinecrossing the following way for the SpatialRecursivePrefrixTree field:

    No IDL crossing: Shapes are oriented in a CCW fashoin.
    When crossing the dateline: Shapes are oriented in a CW fashion.
    """
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
        raise ValueError("Invalid ENVELOPE syntax. Expected format: ENVELOPE(minX, maxX, maxY, minY)")

    # Extract the coordinates
    coords = envelope_wkt.replace("ENVELOPE(", "").replace(")", "").split(",")
    if len(coords) != 4:
        raise ValueError("Invalid ENVELOPE syntax. Expected 4 coordinates: minX, maxX, maxY, minY")

    # Parse the coordinates
    try:
        minX, maxX, maxY, minY = map(float, coords)
    except ValueError:
        raise ValueError("Invalid coordinates in ENVELOPE syntax. Ensure they are numeric.") from ValueError

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
    if geom.geom_type == "Polygon" and math.isclose(geom.minimum_rotated_rectangle.area, geom.area):
        segmitize = True
    if geom.geom_type == "LineString" or geom.geom_type == "MultiLineString":
        segmitize = True

    if segmitize is True:
        segmetized_geom = geom.segmentize(segments)

    if segmitize is True:
        if output == "GeoJSON":
            return to_geojson(segmetized_geom)
        else:
            return to_wkt(segmetized_geom)
    else:
        if output == "GeoJSON":
            return to_geojson(geom)

        return to_wkt(geom)
    return wkt

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
        logger.warning(f"Invalid geometry..trying to repair and simplify: {explain}")   # Explain the issue
        try:
            # Apply buffer(0) trick to fix the geometry
            fixed_geom = unary_union(geom.buffer(0).simplify(1))
            if fixed_geom.is_valid:
                logger.info("Geometry fixed successfully.")
                return to_wkt(fixed_geom)
            else:
                logger.error("Failed to fix Geometry")
        except GEOSException as e:
            logger.error(f"Failed to fix geometry: {e}")
            return geom_wkt
        except Exception as e:
            logger.error(f"Failed to fix geometry: {e}")
            return geom_wkt
    else:
        return geom_wkt
    return geom_wkt
