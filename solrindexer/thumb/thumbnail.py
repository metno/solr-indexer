
"""
SOLR-indexer : WMS Thumbnail generator
======================================

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

PURPOSE:
    This is designed to generate wms thumbnails.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2017-11-09

UPDATES:
    Øystein Godøy, METNO/FOU, 2019-05-31
        Integrated modifications from Trygve Halsne and Massimo Di
        Stefano
    Øystein Godøy, METNO/FOU, 2018-04-19
        Added support for level 2
    Øystein Godøy, METNO/FOU, 2021-02-19
        Added argparse, fixing robustness issues.
    Johannes Langvatn, METNO/SUV, 2023-02-07
        Refactoring
"""
import os
import logging
import base64

import cartopy.crs as ccrs
import matplotlib.pyplot as plt

from owslib.wms import WebMapService

logger = logging.getLogger(__name__)


class WMSThumbNail:

    """
    wms_layer (str): WMS layer name
    wms_style (str): WMS style name
    wms_zoom_level (float): Negative zoom. Fixed value added in
                            all directions (E,W,N,S)
    add_coastlines (bool): If coastlines should be added
    projection (ccrs): Cartopy projection object or name (i.e. string)
    wms_timeout (int): timeout for WMS service
    thumbnail_extent (list): Spatial extent of the thumbnail in
                            lat/lon [x0, x1, y0, y1]
    """

    def __init__(self, wms_layer=None, wms_style=None, wms_zoom_level=0,
                 wms_timeout=None, add_coastlines=None, projection=None,
                 thumbnail_type=None, thumbnail_extent=None):
        self.wms_layer = wms_layer
        self.wms_style = wms_style
        self.wms_zoom_level = wms_zoom_level
        self.wms_timeout = wms_timeout
        self.add_coastlines = add_coastlines
        self.projection = projection
        self.thumbnail_type = thumbnail_type
        self.thumbnail_extent = thumbnail_extent

    def create_wms_thumbnail(self, url, id):
        """ Create a base64 encoded thumbnail by means of cartopy.

            Args:
                url: wms GetCapabilities document

            Returns:
                thumbnail_b64: base64 string representation of image
        """

        wms_layer = self.wms_layer
        wms_style = self.wms_style
        wms_zoom_level = self.wms_zoom_level
        wms_timeout = self.wms_timeout
        add_coastlines = self.add_coastlines
        map_projection = self.projection
        thumbnail_extent = self.thumbnail_extent

        # map projection string to ccrs projection
        if isinstance(map_projection, str):
            map_projection = getattr(ccrs, map_projection)()

        wms = WebMapService(url, timeout=wms_timeout)
        available_layers = list(wms.contents.keys())

        if wms_layer not in available_layers:
            wms_layer = available_layers[0]
            logger.info(
                'Creating WMS thumbnail for layer: {}'.format(wms_layer))

        # Checking styles
        available_styles = list(wms.contents[wms_layer].styles.keys())

        if available_styles:
            if wms_style not in available_styles:
                wms_style = [available_styles[0]]
            else:
                wms_style = None
        else:
            wms_style = None

        if not thumbnail_extent:
            wms_extent = wms.contents[available_layers[0]].boundingBoxWGS84
            # cartopy_extent = [wms_extent[0], wms_extent[2],
            #                  wms_extent[1], wms_extent[3]]

            cartopy_extent_zoomed = [wms_extent[0] - wms_zoom_level,
                                     wms_extent[2] + wms_zoom_level,
                                     wms_extent[1] - wms_zoom_level,
                                     wms_extent[3] + wms_zoom_level]
        else:
            cartopy_extent_zoomed = thumbnail_extent

        max_extent = [-180.0, 180.0, -90.0, 90.0]

        for i, extent in enumerate(cartopy_extent_zoomed):
            if i % 2 == 0:
                if extent < max_extent[i]:
                    cartopy_extent_zoomed[i] = max_extent[i]
            else:
                if extent > max_extent[i]:
                    cartopy_extent_zoomed[i] = max_extent[i]

        subplot_kw = dict(projection=map_projection)
        logger.info(subplot_kw)

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

        ax.add_wms(wms, wms_layer,
                   wms_kwargs={'transparent': False,
                               'styles': wms_style})

        if add_coastlines:
            ax.coastlines(resolution="50m", linewidth=0.5)
        if map_projection == ccrs.PlateCarree():
            ax.set_extent(cartopy_extent_zoomed)
        else:
            ax.set_extent(cartopy_extent_zoomed, ccrs.PlateCarree())

        thumbnail_fname = 'thumbnail_{}.png'.format(id)
        fig.savefig(thumbnail_fname, format='png', bbox_inches='tight')
        plt.close('all')

        with open(thumbnail_fname, 'rb') as infile:
            data = infile.read()
            encode_string = base64.b64encode(data)
            del data

        thumbnail_b64 = (b'data:image/png;base64,', encode_string).decode('utf-8')
        del encode_string

        # Remove thumbnail
        os.remove(thumbnail_fname)
        return thumbnail_b64

    def create_ts_thumbnail(self):
        """ Create a base64 encoded thumbnail """
        pass
