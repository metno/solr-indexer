#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import io
import sys
import logging
import base64

import cartopy.crs as ccrs
import matplotlib
import matplotlib.pyplot as plt


from owslib.wms import WebMapService

logger = logging.getLogger(__name__)
h_stdout = logging.StreamHandler()
h_stdout.setLevel(logging.DEBUG)
logger.addHandler(h_stdout)


matplotlib.use('agg')


def main(url):
    """ Create a base64 encoded thumbnail by means of cartopy.

        Args:
            url: wms GetCapabilities document

        Returns:
            thumbnail_b64: base64 string representation of image
    """

    # Make sure url does not provide request attributes
    url = url.split('?')[0]
    wms_timeout = 60
    logger.debug("Opening wms url %s with timeout %d", url, wms_timeout)
    wms = WebMapService(url, timeout=wms_timeout)

    """Some debugging"""
    logger.debug("Title: ", wms.identification.title)
    logger.debug("Type: ", wms.identification.type)
    logger.debug("Operations: ", [op.name for op in wms.operations])
    logger.debug("GetMap options: ", wms.getOperationByName("GetMap").formatOptions)
    available_layers = list(wms.contents.keys())

    wms_layer = available_layers[0]
    logger.debug(
        'Creating WMS thumbnail for layer: {}'.format(wms_layer))

    logger.debug("Abstract: ", wms_layer.abstract)
    logger.debug("BBox: ", wms_layer.boundingBoxWGS84)
    logger.debug("CRS: ", wms_layer.crsOptions)
    logger.debug("Styles: ", wms_layer.styles)
    logger.debug("Timestamps: ", wms_layer.timepositions)

    # Checking styles
    available_styles = list(wms.contents[wms_layer].styles.keys())

    if available_styles:
        wms_style = [available_styles[0]]
    else:
        wms_style = None

    wms_extent = wms.contents[available_layers[0]].boundingBoxWGS84
    # cartopy_extent = [wms_extent[0], wms_extent[2],
    #                  wms_extent[1], wms_extent[3]]
    wms_zoom_level = 0
    cartopy_extent_zoomed = [wms_extent[0] - wms_zoom_level,
                             wms_extent[2] + wms_zoom_level,
                             wms_extent[1] - wms_zoom_level,
                             wms_extent[3] + wms_zoom_level]

    max_extent = [-180.0, 180.0, -90.0, 90.0]

    for i, extent in enumerate(cartopy_extent_zoomed):
        if i % 2 == 0:
            if extent < max_extent[i]:
                cartopy_extent_zoomed[i] = max_extent[i]
        else:
            if extent > max_extent[i]:
                cartopy_extent_zoomed[i] = max_extent[i]

    map_projection = ccrs.Mercator()
    subplot_kw = dict(projection=map_projection)
    logger.debug(subplot_kw)

    # logger.debug("Aquire lock - creating subplot.")
    # lock.acquire()

    fig, ax = plt.subplots(subplot_kw=subplot_kw)

    # land_mask = cartopy.feature.NaturalEarthFeature(category='physical',
    #                                                scale='50m',
    #                                                facecolor='#cccccc',
    #                                                name='land')
    # ax.add_feature(land_mask, zorder=0, edgecolor='#aaaaaa',
    #        linewidth=0.5)

    # transparent background
    ax.spines['geo'].set_visible(False)
    # ax.outline_patch.set_visible(False)
    # ax.background_patch.set_visible(False)
    fig.patch.set_alpha(0)
    fig.set_alpha(0)
    fig.set_figwidth(4.5)
    fig.set_figheight(4.5)
    fig.set_dpi(100)
    # ax.background_patch.set_alpha(1)
    logger.debug("ax.add_wms().")
    ax.add_wms(wms=url, layers=[wms_layer],
               wms_kwargs={'transparent': False,
                           'styles': wms_style})

    add_coastlines = False
    if add_coastlines:
        ax.coastlines(resolution="50m", linewidth=0.5)
    if map_projection == ccrs.PlateCarree():
        ax.set_extent(cartopy_extent_zoomed)
    else:
        ax.set_extent(cartopy_extent_zoomed, ccrs.PlateCarree())

    # buf = io.BytesIO()
    buf = 'thumbnail.png'
    fig.savefig(buf, format='png', bbox_inches='tight')

    # buf.seek(0)
    # encode_string = base64.b64encode(buf.read())
    # buf.close()
    plt.close('all')
    # logger.debug("plot closed. releasing lock.")
    # lock.release()

    # thumbnail_b64 = str((b'data:image/png;base64,', encode_string)).encode().decode('utf-8')
    # thumbnail_b64 = (b'data:image/png;base64,' + encode_string).decode('utf-8')
    # logger.debug(thumbnail_b64)
    # del encode_string

    return


if __name__ == '__main__':
    main(sys.argv[1])
