
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

import io
import logging
import base64

import cartopy.crs as ccrs

import matplotlib
import matplotlib.pyplot as plt

from threading import Lock

from owslib.wms import WebMapService

import multiprocessing as mp
logger = logging.getLogger(__name__)

matplotlib.use('agg')

lock = Lock()

blackListLayers = ('latitude', 'longitude', 'lat', 'lon', 'MS')


class WMSThumbNail(object):

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
                 wms_timeout=120, add_coastlines=None, projection=None,
                 thumbnail_type=None, thumbnail_extent=None,
                 thumbnail_api_host=None, thumbnail_api_endpoint=None,
                 thumbnail_impl='legacy'):
        self.wms_layer = wms_layer
        self.wms_style = wms_style
        self.wms_zoom_level = wms_zoom_level
        self.wms_timeout = wms_timeout
        self.add_coastlines = add_coastlines
        self.projection = projection
        self.thumbnail_type = thumbnail_type
        self.thumbnail_extent = thumbnail_extent
        self.thumbnail_impl = thumbnail_impl
        self.thumbnail_api_host = thumbnail_api_host
        self.thumbnail_api_endpoint = thumbnail_api_endpoint

        matplotlib.use('Agg')

    def create_wms_thumbnail(self, url, id, wms_layers_mmd):
        """ Create a base64 encoded thumbnail by means of cartopy.

            Args:
                url: wms GetCapabilities document

            Returns:
                thumbnail_b64: base64 string representation of images
        """
        logger.debug("%s. Starting wms url %s", mp.current_process().name, url)
        wms_layer = self.wms_layer
        wms_style = self.wms_style
        wms_zoom_level = self.wms_zoom_level
        wms_timeout = self.wms_timeout
        add_coastlines = self.add_coastlines
        map_projection = self.projection
        thumbnail_extent = self.thumbnail_extent

        # Pick the first layer from the mmd layers list
        if len(wms_layers_mmd) > 0:
            wms_layer_mmd = wms_layers_mmd[0]
        else:
            wms_layer_mmd = None

        """Some debugging"""
        logger.debug("wms_layer: %s", wms_layer)
        logger.debug("wms_layer from MMD: %s", wms_layer_mmd)
        logger.debug("wms_style: %s", wms_style)
        logger.debug("wmz_zoom_level: %d", wms_zoom_level)
        logger.debug("add_coastlines: %s", add_coastlines)
        logger.debug("map_projection:  %s", map_projection)
        logger.debug("thumbnail_extent: %s", thumbnail_extent)

        # Check if url is list or string, and process therafter
        if (isinstance(url, list)):
            url = url[0]  # Extract first url

        # Make sure url does not provide request attributes
        url = url.split('?')[0]

        if url.startswith("http://thredds.nersc"):
            url.replace("http:", "https:")

        if url.startswith("http://nbswms.met.no"):
            url.replace("http:", "https:")

        # Local test
        # url = url.replace('https://fastapi.s-enda-dev.k8s.met.no/', 'http://localhost:8000/')

        # map projection string to ccrs projection
        if isinstance(map_projection, str):
            map_projection = getattr(ccrs, map_projection)()
            logger.debug("map_projection:  %s", map_projection)
        logger.debug("Opening wms url %s with timeout %d", url, wms_timeout)
        wms = None
        try:
            wms = WebMapService(url, version="1.3.0", timeout=int(wms_timeout))
        except Exception as e:
            wms = None
            raise Exception("Could not read wms capability: ", e)

        """Some debugging"""
        logger.debug("Title: %s", wms.identification.title)
        logger.debug("Type: %s", wms.identification.type)
        logger.debug("Operations: %s", [op.name for op in wms.operations])
        logger.debug("GetMap options: %s", wms.getOperationByName("GetMap").formatOptions)

        """Get avilable layers and tiles"""
        available_layers = list(wms.contents.keys())
        logger.debug("Available layers :%s", available_layers)

        available_layers_titles = []
        for layer in available_layers:
            available_layers_titles.append(wms.contents[layer].title)
        logger.debug("Available layers titles :%s", available_layers_titles)

        if len(available_layers) == 0:
            raise Exception("No layers found. Cannot create thumbnail.")

        # Handle layer selection
        if wms_layer not in available_layers:
            # Layer from commandline/config not in available layers. Check MMD
            if wms_layer_mmd in available_layers:
                wms_layer = wms_layer_mmd
                logger.debug("Got layer from MMD: %s", wms_layer)
            # Check if MMD layers was given with title instead of name as for NBS
            elif wms_layer_mmd in available_layers_titles:
                idx = available_layers_titles.index(wms_layer_mmd)
                wms_layer = available_layers[idx]
                logger.debug("Matched MMD wms layer title %s, found layer name: %s",
                             wms_layer_mmd, wms_layer)
            # Fallback. Choose the first from capabilities after removing blacklisted layers
            else:
                for layer in blackListLayers:
                    try:
                        available_layers.remove(layer)
                    except ValueError:
                        pass
                wms_layer = available_layers[0]
            logger.debug(
                'Creating WMS thumbnail for layer: {}'.format(wms_layer))
        logger.debug("layer: %s", wms_layer)
        # logger.debug("Abstract: ", wms_layer.abstract)
        # logger.debug("BBox: ", wms_layer.boundingBoxWGS84)
        # logger.debug("CRS: ", wms_layer.crsOptions)
        # logger.debug("Styles: ", wms_layer.styles)
        # logger.debug("Timestamps: ", wms_layer.timepositions)

        # Checking styles
        available_styles = list(wms.contents[wms_layer].styles.keys())
        logger.debug("Input style: %s  . Available Styles: %s", wms_style, available_styles)
        if available_styles:
            if wms_style not in available_styles:
                wms_style = [available_styles[0]]
        else:
            wms_style = None

        if wms_style is not None and isinstance(wms_style, str):
            wms_style = [wms_style]
        logger.debug("Selected style: %s", wms_style)

        if not thumbnail_extent:
            wms_extent = wms.contents[available_layers[0]].boundingBoxWGS84
            logger.debug("Wms extent, %s", wms_extent)
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
        logger.debug(subplot_kw)

        logger.debug("creating subplot.")
        # lock.acquire()
        fig = None
        try:
            fig, ax = plt.subplots(subplot_kw=subplot_kw)
        except Exception as e:
            if fig is not None:
                plt.close(fig)
            plt.cla()
            fig = None
            ax = None
            wms = None
            plt.close('all')
            raise Exception("Could not plot wms: ", e)

        # logger.debug(type(ax))
        # logger.debug(ax)
        # logger.debug(ax.get_extent())
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
        logger.debug("ax.add_wms(layer=%s, style=%s).", wms_layer, wms_style)
        ax.add_wms(wms, layers=[wms_layer],
                   wms_kwargs={'transparent': False,
                               'styles': wms_style})

        if add_coastlines:
            ax.coastlines(resolution="50m", linewidth=0.5)
        if map_projection == ccrs.PlateCarree():
            ax.set_extent(cartopy_extent_zoomed)
        else:
            ax.set_extent(cartopy_extent_zoomed, ccrs.PlateCarree())

        # For DEBUGGING
        # thumbnail_fname = 'thumbnail_{}.png'.format(id)
        # fig.savefig(thumbnail_fname, format='png', bbox_inches='tight')
        # END DEBUG

        # Save figure to IO Buffer
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight')
        plt.cla()
        plt.close(fig)

        buf.seek(0)
        encode_string = base64.b64encode(buf.getbuffer())
        # logger.debug(encode_string)
        buf.close()
        plt.close('all')
        # logger.debug("plot closed. releasing lock.")
        # lock.release()

        # thumbnail_b64 = str((b'data:image/png;base64,', encode_string)).encode().decode('utf-8')
        thumbnail_b64 = (b'data:image/png;base64,' + encode_string).decode('utf-8')
        # logger.debug(thumbnail_b64)
        del encode_string
        del fig
        del ax
        del wms
        del buf
        logger.debug("%s. Finished", mp.current_process().name)
        return thumbnail_b64

    def create_ts_thumbnail(self):
        """ Create a base64 encoded thumbnail """
        pass
