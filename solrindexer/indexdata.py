"""
SOLR-indexer : Main indexer
===========================

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
    This is designed to simplify the process of indexing single or
    multiple datasets.

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
import math
import base64
import pysolr
import netCDF4
import logging
import warnings
import xmltodict
import dateutil.parser
import lxml.etree as ET
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import shapely.geometry as shpgeo

from metvocab.mmdgroup import MMDGroup
from owslib.wms import WebMapService
from shapely.geometry import box, mapping


logger = logging.getLogger(__name__)
IDREPLS = [':', '/', '.']


def getZones(lon, lat):
    "get UTM zone number from latitude and longitude"
    if lat >= 72.0 and lat < 84.0:
        if lon >= 0.0 and lon < 9.0:
            return 31
        if lon >= 9.0 and lon < 21.0:
            return 33
        if lon >= 21.0 and lon < 33.0:
            return 35
        if lon >= 33.0 and lon < 42.0:
            return 37
    if lat >= 56 and lat < 64.0 and lon >= 3 and lon <= 12:
        return 32
    return math.floor((lon + 180) / 6) + 1


class MMD4SolR:
    """ Read and check MMD files, convert to dictionary """

    def __init__(self, filename, file=None):
        logger.info('Creating an instance of IndexMMD')
        """ set variables in class """
        if file is None:
            self.filename = filename
            try:
                with open(self.filename, encoding='utf-8') as fd:
                    self.mydoc = xmltodict.parse(fd.read())
            except Exception as e:
                logger.error('Could not open file: %s; %s', self.filename, e)
                raise
        else:
            try:
                self.mydoc = xmltodict.parse(file)
            except Exception as e:
                logger.error('Could read incoming file: %s' , e)
                raise

    def check_mmd(self):
        """ Check and correct MMD if needed """
        """ Remember to check that multiple fields of abstract and title
        have set xml:lang= attributes... """

        """
        Check for presence of required elements
        Temporal and spatial extent are not required as of no as it will
        break functionality for some datasets and communities especially
        in the Arctic context.
        """
        # TODO add proper docstring
        mmd_requirements = {
            'mmd:metadata_version': False,
            'mmd:metadata_identifier': False,
            'mmd:title': False,
            'mmd:abstract': False,
            'mmd:metadata_status': False,
            'mmd:dataset_production_status': False,
            'mmd:collection': False,
            'mmd:last_metadata_update': False,
            'mmd:iso_topic_category': False,
            'mmd:keywords': False,
        }
        """
        Check for presence and non empty elements
        This must be further developed...
        """
        mmd = self.mydoc['mmd:mmd']
        for requirement in mmd_requirements.keys():
            if requirement in mmd:
                logger.info('Checking for: %s', requirement)
                if requirement in mmd:
                    if mmd[requirement] is not None:
                        logger.info('%s is present and non empty', requirement)
                        mmd_requirements[requirement] = True
                    else:
                        logger.warning('Required element %s is missing, setting it to unknown',
                                       requirement)
                        mmd[requirement] = 'Unknown'
                else:
                    logger.warning('Required element %s is missing, setting it to unknown.',
                                   requirement)
                    mmd[requirement] = 'Unknown'

        logger.info("Checking controlled vocabularies")
        # Should be collected from
        #    https://github.com/steingod/scivocab/tree/master/metno
        #  Is fetched from vocab.met.no via https://github.com/metno/met-vocab-tools

        mmd_iso_topic_category = MMDGroup('mmd', 'https://vocab.met.no/mmd/ISO_Topic_Category')
        mmd_collection = MMDGroup('mmd', 'https://vocab.met.no/mmd/Collection_Keywords')
        mmd_dataset_status = MMDGroup('mmd', 'https://vocab.met.no/mmd/Dataset_Production_Status')
        mmd_quality_control = MMDGroup('mmd', 'https://vocab.met.no/mmd/Quality_Control')
        mmd_controlled_elements = {
            'mmd:iso_topic_category': mmd_iso_topic_category,
            'mmd:collection': mmd_collection,
            'mmd:dataset_production_status': mmd_dataset_status,
            'mmd:quality_control': mmd_quality_control,
        }
        for element in mmd_controlled_elements.keys():
            logger.info('Checking %s for compliance with controlled vocabulary', element)
            if element in mmd:
                if isinstance(mmd[element], list):
                    for elem in mmd[element]:
                        if isinstance(elem, dict):
                            myvalue = elem['#text']
                        else:
                            myvalue = elem
                else:
                    if isinstance(mmd[element], dict):
                        myvalue = mmd[element]['#text']
                    else:
                        myvalue = mmd.get('element', None)

                if myvalue is not None:
                    if mmd_controlled_elements[element].search(myvalue) is False:
                        logger.warning('%s contains non valid content: %s', element, myvalue)

        """
        Check that keywords also contain GCMD keywords
        Need to check contents more specifically...
        """
        gcmd = False
        if isinstance(mmd['mmd:keywords'], list):
            for elem in mmd['mmd:keywords']:
                if str(elem['@vocabulary']).upper() == 'GCMDSK':
                    gcmd = True
                    break
            if not gcmd:
                logger.warning('Keywords in GCMD are not available (a)')
        else:
            if str(mmd['mmd:keywords']['@vocabulary']).upper() != 'GCMDSK':
                logger.warning('Keywords in GCMD are not available (b)')

        """
        Modify dates if necessary
        Adapted for the new MMD specification, but not all information is
        extracted as SolR is not adapted.
        FIXME and check
        """
        if 'mmd:last_metadata_update' in mmd:
            if isinstance(mmd['mmd:last_metadata_update'],
                          dict):
                for mydict in mmd['mmd:last_metadata_update'].items():
                    if 'mmd:update' in mydict:
                        for myupdate in mydict:
                            if 'mmd:update' not in myupdate:
                                mydateels = myupdate
                                # The comparison below is a hack, need to
                                # revisit later, but works for now.
                                myvalue = '0000-00-00:T00:00:00Z'
                                if isinstance(mydateels, list):
                                    for mydaterec in mydateels:
                                        if mydaterec['mmd:datetime'] > myvalue:
                                            myvalue = mydaterec['mmd:datetime']
                                else:
                                    if mydateels['mmd:datetime'].endswith('Z'):
                                        myvalue = mydateels['mmd:datetime']
                                    else:
                                        myvalue = mydateels['mmd:datetime']+'Z'

            else:
                # To be removed when all records are transformed into the
                # new format
                logger.warning('Removed D7 format in last_metadata_update')
                if mmd['mmd:last_metadata_update'].endswith('Z'):
                    myvalue = mmd['mmd:last_metadata_update']
                else:
                    myvalue = mmd['mmd:last_metadata_update']+'Z'
            mydate = dateutil.parser.parse(myvalue)
        if 'mmd:temporal_extent' in mmd:
            if isinstance(mmd['mmd:temporal_extent'], list):
                for item in mmd['mmd:temporal_extent']:
                    for mykey in item:
                        if (item[mykey] is None) or (item[mykey] == '--'):
                            mydate = ''
                            item[mykey] = mydate
                        else:
                            mydate = dateutil.parser.parse(str(item[mykey]))
                            item[mykey] = mydate.strftime('%Y-%m-%dT%H:%M:%SZ')

            else:
                for mykey, myitem in mmd['mmd:temporal_extent'].items():
                    if mykey == '@xmlns:gml':
                        continue
                    if (myitem is None) or (myitem == '--'):
                        mydate = ''
                        mmd['mmd:temporal_extent'][mykey].set(mydate)
                    else:
                        try:
                            mydate = dateutil.parser.parse(str(myitem))
                            mmd['mmd:temporal_extent'][mykey] = \
                                mydate.strftime('%Y-%m-%dT%H:%M:%SZ')
                        except Exception as e:
                            logger.error('Date format could not be parsed: %s', e)

    def tosolr(self):
        """
        Method for creating document with SolR representation of MMD according
        to the XSD.
        """

        # Defining Look Up Tables
        personnel_role_LUT = {'Investigator': 'investigator',
                              'Technical contact': 'technical',
                              'Metadata author': 'metadata_author',
                              'Data center contact': 'datacenter'
                              }
        related_information_LUT = {'Dataset landing page': 'landing_page',
                                   'Users guide': 'user_guide',
                                   'Project home page': 'home_page',
                                   'Observation facility': 'obs_facility',
                                   'Extended metadata': 'ext_metadata',
                                   'Scientific publication': 'scientific_publication',
                                   'Data paper': 'data_paper',
                                   'Data management plan': 'data_management_plan',
                                   'Other documentation': 'other_documentation',
                                   }

        # As of python 3.6 Dictionaries are ordered by insertion (as OrderedDict)
        mydict = {}

        # Cheeky shorthand
        mmd = self.mydoc['mmd:mmd']

        # SolR Can't use the mmd:metadata_identifier as identifier if it contains :, replace :
        # and other characters like / etc by _ in the id field, let metadata_identifier be
        # the correct one.

        logger.info("Identifier and metadata_identifier")
        if isinstance(mmd['mmd:metadata_identifier'], dict):
            myid = mmd['mmd:metadata_identifier']['#text']
            for e in IDREPLS:
                myid = myid.replace(e, '-')
            mydict['id'] = myid
            mydict['metadata_identifier'] = \
                mmd['mmd:metadata_identifier']['#text']
        else:
            myid = mmd['mmd:metadata_identifier']
            for e in IDREPLS:
                myid = myid.replace(e, '-')
            mydict['id'] = myid
            mydict['metadata_identifier'] = mmd['mmd:metadata_identifier']

        logger.info("Last metadata update")
        if 'mmd:last_metadata_update' in mmd:
            last_metadata_update = mmd['mmd:last_metadata_update']
            lmu_datetime = []
            lmu_type = []
            lmu_note = []
            # FIXME check if this works correctly
            # Only one last_metadata_update element
            if isinstance(last_metadata_update['mmd:update'], dict):
                lmu_datetime.append(str(last_metadata_update['mmd:update']['mmd:datetime']))
                lmu_type.append(last_metadata_update['mmd:update']['mmd:type'])
                lmu_note.append(last_metadata_update['mmd:update'].get('mmd:note', ''))
            # Multiple last_metadata_update elements
            else:
                for i, e in enumerate(last_metadata_update['mmd:update']):
                    lmu_datetime.append(str(e['mmd:datetime']))
                    lmu_type.append(e['mmd:type'])
                    if 'mmd:note' in e.keys():
                        lmu_note.append(e['mmd:note'])

            for i, myel in enumerate(lmu_datetime):
                if myel.endswith('Z'):
                    continue
                else:
                    lmu_datetime[i-1] = myel+'Z'

            mydict['last_metadata_update_datetime'] = lmu_datetime
            mydict['last_metadata_update_type'] = lmu_type
            mydict['last_metadata_update_note'] = lmu_note

        logger.info("Metadata status")
        if isinstance(mmd['mmd:metadata_status'], dict):
            mydict['metadata_status'] = mmd['mmd:metadata_status']['#text']
        else:
            mydict['metadata_status'] = mmd['mmd:metadata_status']

        logger.info("Collection")
        if 'mmd:collection' in mmd:
            mydict['collection'] = []
            if isinstance(mmd['mmd:collection'], list):
                for e in mmd['mmd:collection']:
                    if isinstance(e, dict):
                        mydict['collection'].append(e['#text'])
                    else:
                        mydict['collection'].append(e)
            else:
                mydict['collection'] = mmd['mmd:collection']

        logger.info("Title")
        if isinstance(mmd['mmd:title'], list):
            for e in mmd['mmd:title']:
                if '@xml:lang' in e:
                    if e['@xml:lang'] == 'en':
                        mydict['title'] = e['#text']
                elif '@lang' in e:
                    if e['@lang'] == 'en':
                        mydict['title'] = e['#text']
        else:
            if isinstance(mmd['mmd:title'], dict):
                if '@xml:lang' in mmd['mmd:title']:
                    if mmd['mmd:title']['@xml:lang'] == 'en':
                        mydict['title'] = mmd['mmd:title']['#text']
                if '@lang' in mmd['mmd:title']:
                    if mmd['mmd:title']['@lang'] == 'en':
                        mydict['title'] = mmd['mmd:title']['#text']
            else:
                mydict['title'] = str(mmd['mmd:title'])

        logger.info("Abstract")
        if isinstance(mmd['mmd:abstract'], list):
            for e in mmd['mmd:abstract']:
                if '@xml:lang' in e:
                    if e['@xml:lang'] == 'en':
                        mydict['abstract'] = e['#text']
                elif '@lang' in e:
                    if e['@lang'] == 'en':
                        mydict['abstract'] = e['#text']
        else:
            if isinstance(mmd['mmd:abstract'], dict):
                if '@xml:lang' in mmd['mmd:abstract']:
                    if mmd['mmd:abstract']['@xml:lang'] == 'en':
                        mydict['abstract'] = mmd['mmd:abstract']['#text']
                if '@lang' in mmd['mmd:abstract']:
                    if mmd['mmd:abstract']['@lang'] == 'en':
                        mydict['abstract'] = mmd['mmd:abstract']['#text']
            else:
                mydict['abstract'] = str(mmd['mmd:abstract'])

        logger.info("Temporal extent")
        if 'mmd:temporal_extent' in mmd:
            if isinstance(mmd['mmd:temporal_extent'], list):
                maxtime = dateutil.parser.parse('1000-01-01T00:00:00Z')
                mintime = dateutil.parser.parse('2099-01-01T00:00:00Z')
                for item in mmd['mmd:temporal_extent']:
                    for myval in item.values():
                        if myval != '':
                            mytime = dateutil.parser.parse(myval)
                        if mytime < mintime:
                            mintime = mytime
                        if mytime > maxtime:
                            maxtime = mytime
                mydict['temporal_extent_start_date'] = mintime.strftime('%Y-%m-%dT%H:%M:%SZ')
                mydict['temporal_extent_end_date'] = maxtime.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                mydict["temporal_extent_start_date"] = str(
                    mmd['mmd:temporal_extent']['mmd:start_date'])
                if 'mmd:end_date' in mmd['mmd:temporal_extent']:
                    if mmd['mmd:temporal_extent']['mmd:end_date'] is not None:
                        mydict["temporal_extent_end_date"] = str(
                            mmd['mmd:temporal_extent']['mmd:end_date'])

        logger.info("Geographical extent")
        # Assumes longitudes positive eastwards and in the are -180:180
        mmd_geographic_extent = mmd.get('mmd:geographic_excent', None)
        if mmd_geographic_extent is not None:
            if isinstance(mmd_geographic_extent, list):
                logger.warning('This is a challenge as multiple bounding boxes are not '
                               'supported in MMD yet, flattening information')
                latvals = []
                lonvals = []
                # Point or boundingbox check is only done on last item in mmd_geographic_extent
                for e in mmd_geographic_extent:
                    if e['mmd:rectangle']['mmd:north'] is not None:
                        latvals.append(float(e['mmd:rectangle']['mmd:north']))
                    if e['mmd:rectangle']['mmd:south'] is not None:
                        latvals.append(float(e['mmd:rectangle']['mmd:south']))
                    if e['mmd:rectangle']['mmd:east'] is not None:
                        lonvals.append(float(e['mmd:rectangle']['mmd:east']))
                    if e['mmd:rectangle']['mmd:west'] is not None:
                        lonvals.append(float(e['mmd:rectangle']['mmd:west']))

                if len(latvals) > 0 and len(lonvals) > 0:
                    mydict['geographic_extent_rectangle_north'] = max(latvals)
                    mydict['geographic_extent_rectangle_south'] = min(latvals)
                    mydict['geographic_extent_rectangle_west'] = min(lonvals)
                    mydict['geographic_extent_rectangle_east'] = max(lonvals)
                    mydict['bbox'] = "ENVELOPE("+str(min(lonvals))+","+str(max(lonvals))+"," +\
                        str(max(latvals))+","+str(min(latvals))+")"

                    # Check if we have a point or a boundingbox
                    if max(latvals) == min(latvals):
                        if max(lonvals) == min(lonvals):
                            point = shpgeo.Point(float(e['mmd:rectangle']['mmd:east']),
                                                 float(e['mmd:rectangle']['mmd:north']))
                            mydict['polygon_rpt'] = point.wkt
                            logger.info(mapping(point))
                    else:
                        bbox = box(min(lonvals), min(latvals), max(lonvals), max(latvals))
                        logger.info("First conditition")
                        logger.info(bbox)
                        polygon = bbox.wkt
                        mydict['polygon_rpt'] = polygon

                else:
                    mydict['geographic_extent_rectangle_north'] = 90.
                    mydict['geographic_extent_rectangle_south'] = -90.
                    mydict['geographic_extent_rectangle_west'] = -180.
                    mydict['geographic_extent_rectangle_east'] = 180.
            else:
                for item in mmd_geographic_extent['mmd:rectangle'].values:
                    if item is None:
                        logger.warning('Missing geographical element, will not process the file.')
                        mydict['metadata_status'] = 'Inactive'
                        raise Warning('Missing spatial bounds')

                north = float(mmd_geographic_extent['mmd:rectangle']['mmd:north'])
                south = float(mmd_geographic_extent['mmd:rectangle']['mmd:south'])
                east = float(mmd_geographic_extent['mmd:rectangle']['mmd:east'])
                west = float(mmd_geographic_extent['mmd:rectangle']['mmd:west'])
                mydict['geographic_extent_rectangle_north'] = north
                mydict['geographic_extent_rectangle_south'] = south
                mydict['geographic_extent_rectangle_east'] = east
                mydict['geographic_extent_rectangle_west'] = west
                srsname = mmd_geographic_extent['mmd:rectangle'].get('@srsName', None)
                if srsname is not None:
                    mydict['geographic_extent_rectangle_srsName'] = srsname

                mydict['bbox'] = "ENVELOPE("+west + "," + east + "," + north + "," + south + ")"

                logger.info("Second conditition")
                #  Check if we have a point or a boundingbox
                if south == north:
                    if east == west:
                        point = shpgeo.Point(east, north)
                        mydict['polygon_rpt'] = point.wkt
                        logger.info(mapping(point))

                else:
                    bbox = box(west, south, east, north, ccw=False)
                    polygon = bbox.wkt
                    logger.info(polygon)
                    mydict['polygon_rpt'] = polygon

        logger.info("Dataset production status")
        if 'mmd:dataset_production_status' in mmd:
            if isinstance(mmd['mmd:dataset_production_status'], dict):
                mydict['dataset_production_status'] = mmd['mmd:dataset_production_status']['#text']
            else:
                mydict['dataset_production_status'] = str(mmd['mmd:dataset_production_status'])

        logger.info("Dataset language")
        if 'mmd:dataset_language' in mmd:
            mydict['dataset_language'] = str(mmd['mmd:dataset_language'])

        logger.info("Operational status")
        if 'mmd:operational_status' in mmd:
            mydict['operational_status'] = str(mmd['mmd:operational_status'])

        logger.info("Access constraints")
        if 'mmd:access_constraint' in mmd:
            mydict['access_constraint'] = str(mmd['mmd:access_constraint'])

        logger.info("Use constraint")
        use_constraint = mmd.get('mmd:use_constraint', None)
        if use_constraint is not None:
            # Need both identifier and resource for use constraint
            if 'mmd:identifier' in use_constraint and 'mmd:resource' in use_constraint:
                mydict['use_constraint_identifier'] = str(use_constraint['mmd:identifier'])
                mydict['use_constraint_resource'] = str(use_constraint['mmd:resource'])
            else:
                logger.warning('Both license identifier and resource needed to index properly')
                mydict['use_constraint_identifier'] = "Not provided"
                mydict['use_constraint_resource'] = "Not provided"
            if 'mmd:license_text' in mmd['mmd:use_constraint']:
                mydict['use_constraint_license_text'] = str(use_constraint['mmd:license_text'])

        logger.info("Personnel")
        if 'mmd:personnel' in mmd:
            personnel_elements = mmd['mmd:personnel']

            # Only one element
            if isinstance(personnel_elements, dict):
                # make it an iterable list
                personnel_elements = [personnel_elements]

            # Facet elements
            mydict['personnel_role'] = []
            mydict['personnel_name'] = []
            mydict['personnel_organisation'] = []

            # Fix role based lists
            for role in personnel_role_LUT:
                mydict[f'personnel_{personnel_role_LUT[role]}_role'] = []
                mydict[f'personnel_{personnel_role_LUT[role]}_name'] = []
                mydict[f'personnel_{personnel_role_LUT[role]}_email'] = []
                mydict[f'personnel_{personnel_role_LUT[role]}_phone'] = []
                mydict[f'personnel_{personnel_role_LUT[role]}_fax'] = []
                mydict[f'personnel_{personnel_role_LUT[role]}_organisation'] = []
                mydict[f'personnel_{personnel_role_LUT[role]}_address'] = []
                # don't think this is needed Øystein Godøy, METNO/FOU, 2021-09-08
                # mydict[f'personnel_{personnel_role_LUT[role]}_address_address'] = []
                mydict[f'personnel_{personnel_role_LUT[role]}_address_city'] = []
                mydict[f'personnel_{personnel_role_LUT[role]}_address_province_or_state'] = []
                mydict[f'personnel_{personnel_role_LUT[role]}_address_postal_code'] = []
                mydict[f'personnel_{personnel_role_LUT[role]}_address_country'] = []

            # Fill lists with information
            for personnel in personnel_elements:
                role = personnel['mmd:role']
                if not role:
                    logger.warning('No role available for personnel')
                    break
                if role not in personnel_role_LUT:
                    logger.warning('Wrong role provided for personnel')
                    break
                for entry_key, entry in personnel.items():
                    entry_type = entry_key.split(':')[-1]
                    if entry_type == 'role':
                        mydict['personnel_{}_role'.format(personnel_role_LUT[role])].append(entry)
                        mydict['personnel_role'].append(personnel[entry_key])
                    else:
                        # Treat address specifically and handle faceting elements personnel_role,
                        # personnel_name, personnel_organisation.
                        if entry_type == 'contact_address':
                            for el_key, el in entry.items():
                                el_type = el_key.split(':')[-1]
                                if el_type == 'address':
                                    key = f'personnel_{personnel_role_LUT[role]}_address_{el_type}'
                                    mydict[key].append(el)
                                else:
                                    key = f'personnel_{personnel_role_LUT[role]}_address_{el_type}'
                                    mydict[key].append(el)
                        elif entry_type == 'name':
                            mydict[f'personnel_{personnel_role_LUT[role]}_{el_type}'].append(entry)
                            mydict['personnel_name'].append(entry)
                        elif entry_type == 'organisation':
                            mydict[f'personnel_{personnel_role_LUT[role]}_{el_type}'].append(entry)
                            mydict['personnel_organisation'].append(entry)
                        else:
                            mydict[f'personnel_{personnel_role_LUT[role]}_{el_type}'].append(entry)

        logger.info("Data center")
        if 'mmd:data_center' in mmd:
            data_center_elements = mmd['mmd:data_center']
            # Only one element
            if isinstance(data_center_elements, dict):
                # make it an iterable list
                data_center_elements = [data_center_elements]

            for data_center in data_center_elements:
                for key, value in data_center.items():
                    # if sub element is ordered dict
                    if isinstance(value, dict):
                        for key, val in value.items():
                            element_name = f'data_center_{key.split(":")[-1]}'
                            # create key in mydict
                            if element_name not in mydict.keys():
                                mydict[element_name] = []
                                mydict[element_name].append(val)
                            else:
                                mydict[element_name].append(val)
                    # sub element is not ordered dicts
                    else:
                        element_name = f'data_center_{key.split(":")[-1]}'
                        # create key in mydict. Repetition of above. Should be simplified.
                        if element_name not in mydict.keys():
                            mydict[element_name] = []
                            mydict[element_name].append(value)
                        else:
                            mydict[element_name].append(value)

        logger.info("Data access")
        # NOTE: This is identical to method above. Should in be simplified as method
        if 'mmd:data_access' in mmd:
            data_access_elements = mmd['mmd:data_access']
            # Only one element
            if isinstance(data_access_elements, dict):
                # make it an iterable list
                data_access_elements = [data_access_elements]
            # iterate over all data_center elements
            for data_access in data_access_elements:
                data_access_type = data_access['mmd:type'].replace(" ", "_").lower()
                mydict[f'data_access_url_{data_access_type}'] = data_access['mmd:resource']

                if 'mmd:wms_layers' in data_access and data_access_type == 'ogc_wms':
                    data_access_wms_layers_string = 'data_access_wms_layers'
                    # Map directly to list
                    data_access_wms_layers = list(data_access['mmd:wms_layers'])
                    # old version was [i for i in data_access_wms_layers.values()][0]
                    mydict[data_access_wms_layers_string] = data_access_wms_layers[0]

        logger.info("Related dataset")
        # TODO
        # Remember to add type of relation in the future ØG
        # Only interpreting parent for now since SolR doesn't take more
        # Added handling of namespace in identifiers
        # self.parent is never used again? JTL 2023 02 13
        self.parent = None
        if 'mmd:related_dataset' in mmd:
            if isinstance(mmd['mmd:related_dataset'], list):
                logger.warning('Too many fields in related_dataset...')
                for e in mmd['mmd:related_dataset']:
                    if '@mmd:relation_type' in e:
                        if e['@mmd:relation_type'] == 'parent':
                            if '#text' in dict(e):
                                mydict['related_dataset'] = e['#text']
                                mydict['related_dataset_id'] = mydict['related_dataset']
                                for e in IDREPLS:
                                    mydict['related_dataset_id'] = \
                                        mydict['related_dataset_id'].replace(e, '-')
            else:
                # Not sure if this is used??
                if '#text' in dict(mmd['mmd:related_dataset']):
                    mydict['related_dataset'] = mmd['mmd:related_dataset']['#text']
                    mydict['related_dataset_id'] = mydict['related_dataset']
                    for e in IDREPLS:
                        mydict['related_dataset_id'] = mydict['related_dataset_id'].replace(e, '-')

        logger.info("Storage information")
        storage_information = mmd.get("mmd:storage_information", None)
        if storage_information is not None:
            file_name = storage_information.get("mmd:file_name", None)
            file_location = storage_information.get("mmd:file_location", None)
            file_format = storage_information.get("mmd:file_format", None)
            file_size = storage_information.get("mmd:file_size", None)
            checksum = storage_information.get("mmd:checksum", None)
            if file_name is not None:
                mydict['storage_information_file_name'] = str(file_name)
            if file_location is not None:
                mydict['storage_information_file_location'] = str(file_location)
            if file_format is not None:
                mydict['storage_information_file_format'] = str(file_format)
            if file_size is not None:
                if isinstance(file_size, dict):
                    mydict['storage_information_file_size'] = str(file_size['#text'])
                    mydict['storage_information_file_size_unit'] = str(file_size['@unit'])
                else:
                    logger.warning("Filesize unit not specified, skipping field")
            if checksum is not None:
                if isinstance(checksum, dict):
                    mydict['storage_information_file_checksum'] = str(checksum['#text'])
                    mydict['storage_information_file_checksum_type'] = str(checksum['@type'])
                else:
                    logger.warning("Checksum type is not specified, skipping field")

        logger.info("Related information")
        if 'mmd:related_information' in mmd:
            related_information_elements = mmd['mmd:related_information']

            # Only one element
            if isinstance(related_information_elements, dict):
                # make it an iterable list
                related_information_elements = [related_information_elements]

            for related_information in related_information_elements:
                for key, value in related_information.items():
                    element_name = f'related_information_{key.split(":")[-1]}'

                    if value in related_information_LUT.keys():
                        mydict[f'related_url_{related_information_LUT[value]}'] = \
                            related_information['mmd:resource']
                        if 'mmd:description' in related_information:
                            mydict[f'related_url_{related_information_LUT[value]}_desc'] = \
                                related_information['mmd:description']

        logger.info("ISO TopicCategory")

        if 'mmd:iso_topic_category' in mmd:
            mydict['iso_topic_category'] = []
            if isinstance(mmd['mmd:iso_topic_category'], list):
                for iso_topic_category in mmd['mmd:iso_topic_category']:
                    mydict['iso_topic_category'].append(iso_topic_category)
            else:
                mydict['iso_topic_category'].append(mmd['mmd:iso_topic_category'])

        logger.info("Keywords")
        # Added double indexing of GCMD keywords. keywords_gcmd (and keywords_wigos) are for
        # faceting in SolR. What is shown in data portal is keywords_keyword.
        if 'mmd:keywords' in mmd:
            mydict['keywords_keyword'] = []
            mydict['keywords_vocabulary'] = []
            mydict['keywords_gcmd'] = []
            # Not used yet
            mydict['keywords_wigos'] = []
            # If there is only one keyword list
            if isinstance(mmd['mmd:keywords'], dict):
                vocab = mmd['mmd:keywords']['@vocabulary']
                if isinstance(mmd['mmd:keywords']['mmd:keyword'], str):
                    if vocab == "GCMDSK":
                        mydict['keywords_gcmd'].append(mmd['mmd:keywords']['mmd:keyword'])
                    mydict['keywords_keyword'].append(mmd['mmd:keywords']['mmd:keyword'])
                    mydict['keywords_vocabulary'].append(vocab)
                else:
                    for elem in mmd['mmd:keywords']['mmd:keyword']:
                        if isinstance(elem, str):
                            if vocab == "GCMDSK":
                                mydict['keywords_gcmd'].append(elem)
                            mydict['keywords_vocabulary'].append(vocab)
                            mydict['keywords_keyword'].append(elem)
            # If there are multiple keyword lists
            elif isinstance(mmd['mmd:keywords'], list):
                for elem in mmd['mmd:keywords']:
                    if isinstance(elem, dict):
                        # Check for empty lists
                        if len(elem) < 2:
                            continue
                        if isinstance(elem['mmd:keyword'], list):
                            for keyword in elem['mmd:keyword']:
                                if elem['@vocabulary'] == "GCMDSK":
                                    mydict['keywords_gcmd'].append(keyword)
                                mydict['keywords_vocabulary'].append(elem['@vocabulary'])
                                mydict['keywords_keyword'].append(keyword)
                        else:
                            if mmd['mmd:keywords'][i]['@vocabulary'] == "GCMDSK":
                                mydict['keywords_gcmd'].append(elem['mmd:keyword'])
                            mydict['keywords_vocabulary'].append(elem['@vocabulary'])
                            mydict['keywords_keyword'].append(elem['mmd:keyword'])

            else:
                if mmd['mmd:keywords']['@vocabulary'] == "GCMDSK":
                    mydict['keywords_gcmd'].append(mmd['mmd:keywords']['mmd:keyword'])
                mydict['keywords_vocabulary'].append(mmd['mmd:keywords']['@vocabulary'])
                mydict['keywords_keyword'].append(mmd['mmd:keywords']['mmd:keyword'])

        logger.info("Project")
        mydict['project_short_name'] = []
        mydict['project_long_name'] = []
        if 'mmd:project' in mmd:
            if mmd['mmd:project'] is None:
                mydict['project_short_name'].append('Not provided')
                mydict['project_long_name'].append('Not provided')
            elif isinstance(mmd['mmd:project'], list):
                # Check if multiple nodes are present
                for e in mmd['mmd:project']:
                    mydict['project_short_name'].append(e['mmd:short_name'])
                    mydict['project_long_name'].append(e['mmd:long_name'])
            else:
                # Extract information as appropriate
                e = mmd['mmd:project']
                if 'mmd:short_name' in e:
                    mydict['project_short_name'].append(e['mmd:short_name'])
                else:
                    mydict['project_short_name'].append('Not provided')

                if 'mmd:long_name' in e:
                    mydict['project_long_name'].append(e['mmd:long_name'])
                else:
                    mydict['project_long_name'].append('Not provided')

        logger.info("Platform")
        # FIXME add check for empty sub elements...
        if 'mmd:platform' in mmd:
            platform_elements = mmd['mmd:platform']
            # Only one element
            if isinstance(platform_elements, dict):
                # make it an iterable list
                platform_elements = [platform_elements]

            for platform in platform_elements:
                for platform_key, platform_value in platform.items():
                    # if sub element is ordered dict
                    if isinstance(platform_value, dict):
                        for key, val in platform_value.items():
                            local_key = key.split(":")[-1]
                            element_name = f'platform_{platform_key.split(":")[-1]}_{local_key}'
                            # create key in mydict
                            if element_name not in mydict.keys():
                                mydict[element_name] = []
                                mydict[element_name].append(val)
                            else:
                                mydict[element_name].append(val)
                    # sub element is not ordered dicts
                    else:
                        element_name = 'platform_{}'.format(platform_key.split(':')[-1])
                        # create key in mydict. Repetition of above. Should be simplified.
                        if element_name not in mydict.keys():
                            mydict[element_name] = []
                            mydict[element_name].append(platform_value)
                        else:
                            mydict[element_name].append(platform_value)

                # Add platform_sentinel for NBS
                initial_platform = mydict['platform_long_name'][0]
                if initial_platform.startswith('Sentinel'):
                    mydict['platform_sentinel'] = initial_platform[:-1]

        logger.info("Activity type")
        if 'mmd:activity_type' in mmd:
            mydict['activity_type'] = []
            if isinstance(mmd['mmd:activity_type'], list):
                for activity_type in mmd['mmd:activity_type']:
                    mydict['activity_type'].append(activity_type)
            else:
                mydict['activity_type'].append(mmd['mmd:activity_type'])

        logger.info("Dataset citation")
        if 'mmd:dataset_citation' in mmd:
            dataset_citation_elements = mmd['mmd:dataset_citation']
            # Only one element
            if isinstance(dataset_citation_elements, dict):
                # make it an iterable list
                dataset_citation_elements = [dataset_citation_elements]

            for dataset_citation in dataset_citation_elements:
                for k, v in dataset_citation.items():
                    element_suffix = k.split(':')[-1]
                    # Fix issue between MMD and SolR schema, SolR requires full datetime, MMD not.
                    if element_suffix == 'publication_date':
                        if v is not None:
                            v += 'T12:00:00Z'
                    mydict['dataset_citation_{}'.format(element_suffix)] = v

        """ Quality control """
        if 'mmd:quality_control' in mmd and mmd['mmd:quality_control'] is not None:
            mydict['quality_control'] = str(mmd['mmd:quality_control'])

        """ Adding MMD document as base64 string"""
        # Check if this can be simplified in the workflow.
        xml_root = ET.parse(str(self.filename))
        xml_string = ET.tostring(xml_root)
        encoded_xml_string = base64.b64encode(xml_string)
        xml_b64 = (encoded_xml_string).decode('utf-8')
        mydict['mmd_xml_file'] = xml_b64

        return mydict


class IndexMMD:
    """ Class for indexing SolR representation of MMD to SolR server. Requires
    a list of dictionaries representing MMD as input.
    """

    def __init__(self, mysolrserver, always_commit=False, authentication=None):
        # Set up logging
        logger.info('Creating an instance of IndexMMD')

        # level variables

        self.level = None

        # Thumbnail variables
        self.wms_layer = None
        self.wms_style = None
        self.wms_zoom_level = 0
        self.wms_timeout = None
        self.add_coastlines = None
        self.projection = None
        self.thumbnail_type = None
        self.thumbnail_extent = None

        # Connecting to core
        try:
            self.solrc = pysolr.Solr(mysolrserver, always_commit=always_commit, timeout=1020,
                                     auth=authentication)
            logger.info("Connection established to: %s", str(mysolrserver))
        except Exception as e:
            logger.error("Something failed in SolR init: %s", str(e))
            raise e

    # Function for sending explicit commit to solr
    def commit(self):
        self.solrc.commit()

    def index_record(self, input_record, addThumbnail, level=None, wms_layer=None, wms_style=None,
                     wms_zoom_level=0, add_coastlines=True, projection=ccrs.PlateCarree(),
                     wms_timeout=120, thumbnail_extent=None):
        """ Add thumbnail to SolR
            Args:
                input_record() : input MMD file to be indexed in SolR
                addThumbnail (bool): If thumbnail should be added or not
                level (int): 1 or 2 depending if MMD is Level-1 or Level-2,
                            respectively. If None, assume to be Level-1
                wms_layer (str): WMS layer name
                wms_style (str): WMS style name
                wms_zoom_level (float): Negative zoom. Fixed value added in
                                        all directions (E,W,N,S)
                add_coastlines (bool): If coastlines should be added
                projection (ccrs): Cartopy projection object or name (i.e. string)
                wms_timeout (int): timeout for WMS service
                thumbnail_extent (list): Spatial extent of the thumbnail in
                                      lat/lon [x0, x1, y0, y1]
            Returns:
                bool
        """
        if level == 1 or level is None:
            input_record.update({'dataset_type': 'Level-1'})
            input_record.update({'isParent': 'false'})
        elif level == 2:
            input_record.update({'dataset_type': 'Level-2'})
        else:
            logger.error('Invalid level given: {}. Hence terminating'.format(level))

        if input_record['metadata_status'] == 'Inactive':
            logger.warning('Skipping record')
            return False
        myfeature = None
        if 'data_access_url_opendap' in input_record:
            # Thumbnail of timeseries to be added
            # Or better do this as part of get_feature_type?
            try:
                myfeature = self.get_feature_type(input_record['data_access_url_opendap'])
            except Exception as e:
                logger.error("Something failed while retrieving feature type: %s", str(e))
            if myfeature:
                logger.info('feature_type found: %s', myfeature)
                input_record.update({'feature_type': myfeature})

        self.id = input_record['id']
        if 'data_access_url_ogc_wms' in input_record and addThumbnail:
            logger.info("Checking thumbnails...")
            getCapUrl = input_record['data_access_url_ogc_wms']
            if not myfeature:
                self.thumbnail_type = 'wms'
            self.wms_layer = wms_layer
            self.wms_style = wms_style
            self.wms_zoom_level = wms_zoom_level
            self.add_coastlines = add_coastlines
            self.projection = projection
            self.wms_timeout = wms_timeout
            self.thumbnail_extent = thumbnail_extent
            thumbnail_data = self.add_thumbnail(url=getCapUrl)

            if not thumbnail_data:
                logger.warning('Could not properly parse WMS GetCapabilities document')
                # If WMS is not available, remove this data_access element from the XML that
                # is indexed
                del input_record['data_access_url_ogc_wms']
            else:
                input_record.update({'thumbnail_data': thumbnail_data})

        logger.info("Adding records to core...")

        mmd_record = list()
        mmd_record.append(input_record)

        try:
            self.solrc.add(mmd_record)
        except Exception as e:
            logger.error("Something failed in SolR adding document: %s", str(e))
            return False
        logger.info("Record successfully added.")

        return True

    def add_level2(self, myl2record, addThumbnail=False, projection=ccrs.Mercator(),
                   wms_layer=None, wms_style=None, wms_zoom_level=0, add_coastlines=True,
                   wms_timeout=120, thumbnail_extent=None):
        """ Add a level 2 dataset, i.e. update level 1 as well """
        mmd_record2 = list()

        # Fix for NPI data...
        myl2record['related_dataset'] = myl2record['related_dataset'].replace(
            'http://data.npolar.no/dataset/', '')
        myl2record['related_dataset'] = myl2record['related_dataset'].replace(
            'https://data.npolar.no/dataset/', '')
        myl2record['related_dataset'] = myl2record['related_dataset'].replace(
            'http://api.npolar.no/dataset/', '')
        myl2record['related_dataset'] = myl2record['related_dataset'].replace(
            '.xml', '')

        # Add additonal helper fields for handling in SolR and Drupal
        myl2record['isChild'] = 'true'

        myfeature = None
        if 'data_access_url_opendap' in myl2record:
            # Thumbnail of timeseries to be added
            # Or better do this as part of get_feature_type?
            try:
                myfeature = self.get_feature_type(myl2record['data_access_url_opendap'])
            except Exception as e:
                logger.error("Something failed while retrieving feature type: %s", str(e))

            if myfeature:
                logger.info('feature_type found: %s', myfeature)
                myl2record.update({'feature_type': myfeature})

        self.id = myl2record['id']
        # Add thumbnail for WMS supported datasets
        if 'data_access_url_ogc_wms' in myl2record and addThumbnail:
            logger.info("Checking tumbnails...")
            if not myfeature:
                self.thumbnail_type = 'wms'
            self.wms_layer = wms_layer
            self.wms_style = wms_style
            self.wms_zoom_level = wms_zoom_level
            self.add_coastlines = add_coastlines
            self.projection = projection
            self.wms_timeout = wms_timeout
            self.thumbnail_extent = thumbnail_extent
            if 'data_access_url_ogc_wms' in myl2record.keys():
                getCapUrl = myl2record['data_access_url_ogc_wms']
                try:
                    thumbnail_data = self.add_thumbnail(url=getCapUrl)
                except Exception as e:
                    logger.error("Something failed in adding thumbnail: %s", str(e))
                    warnings.warning("Couldn't add thumbnail.")

        if addThumbnail and thumbnail_data:
            myl2record.update({'thumbnail_data': thumbnail_data})

        mmd_record2.append(myl2record)

        """ Retrieve level 1 record """
        myparid = myl2record['related_dataset']
        for e in IDREPLS:
            myparid = myparid.replace(e, '-')
        try:
            myresults = self.solrc.search('id:' + myparid, **{'wt': 'python', 'rows': 100})
        except Exception as e:
            logger.error("Something failed in searching for parent dataset, " + str(e))

        # Check that only one record is returned
        if len(myresults) != 1:
            logger.warning("Didn't find unique parent record, skipping record")
            return
        # Convert from pySolr results object to dict and return.
        for result in myresults:
            if 'full_text' in result:
                result.pop('full_text')
            if 'bbox__maxX' in result:
                result.pop('bbox__maxX')
            if 'bbox__maxY' in result:
                result.pop('bbox__maxY')
            if 'bbox__minX' in result:
                result.pop('bbox__minX')
            if 'bbox__minY' in result:
                result.pop('bbox__minY')
            if 'bbox_rpt' in result:
                result.pop('bbox_rpt')
            if 'ss_access' in result:
                result.pop('ss_access')
            if '_version_' in result:
                result.pop('_version_')
                myresults = result
        myresults['isParent'] = 'true'

        # Check that the parent found has related_dataset set and
        # update this, but first check that it doesn't already exists
        if 'related_dataset' in myresults:
            myl2id = myl2record['metadata_identifier'].replace(':', '_')
            # Need to check that this doesn't already exist...
            if myl2id not in myresults['related_dataset']:
                myresults['related_dataset'].append(myl2id)
        else:
            logger.info('This dataset was not found in parent, creating it...')
            myresults['related_dataset'] = []
            logger.info('Adding dataset with identifier %s to parent %s',
                        myl2id,
                        myl2record['related_dataset'])
            myresults['related_dataset'].append(myl2id)
        mmd_record1 = list()
        mmd_record1.append(myresults)

        logger.info("Index level 2 dataset")
        try:
            self.solrc.add(mmd_record2)
        except Exception as e:
            raise Exception("Something failed in SolR add level 2", str(e))
        logger.info("Level 2 record successfully added.")

        logger.info("Update level 1 record with id of this dataset")
        try:
            self.solrc.add(mmd_record1)
        except Exception as e:
            raise Exception("Something failed in SolR update level 1 for level 2", str(e))
        logger.info("Level 1 record successfully updated.")

    def add_thumbnail(self, url, thumbnail_type='wms'):
        """ Add thumbnail to SolR
            Args:
                type: Thumbnail type. (wms, ts)
            Returns:
                thumbnail: base64 string representation of image
        """
        logger.info("adding thumbnail for: ", url)
        if thumbnail_type == 'wms':
            try:
                thumbnail = self.create_wms_thumbnail(url)
                return thumbnail
            except Exception as e:
                logger.error("Thumbnail creation from OGC WMS failed: %s", e)
                return None
        # time_series
        elif thumbnail_type == 'ts':
            # create_ts_thumbnail(...)
            thumbnail = 'TMP'
            return thumbnail
        else:
            logger.error('Invalid thumbnail type: {}'.format(thumbnail_type))
            return None

    def create_wms_thumbnail(self, url):
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
            logger.info('Creating WMS thumbnail for layer: {}'.format(wms_layer))

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
            # Not accessed
            # cartopy_extent = [wms_extent[0], wms_extent[2], wms_extent[1], wms_extent[3]]

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

        ax.add_wms(wms, wms_layer, wms_kwargs={'transparent': False, 'styles': wms_style})

        if add_coastlines:
            ax.coastlines(resolution="50m", linewidth=0.5)
        if map_projection == ccrs.PlateCarree():
            ax.set_extent(cartopy_extent_zoomed)
        else:
            ax.set_extent(cartopy_extent_zoomed, ccrs.PlateCarree())

        thumbnail_fname = 'thumbnail_{}.png'.format(self.id)
        fig.savefig(thumbnail_fname, format='png', bbox_inches='tight')
        plt.close('all')

        with open(thumbnail_fname, 'rb') as infile:
            data = infile.read()
            encode_string = base64.b64encode(data)

        thumbnail_b64 = b'data:image/png;base64,' +\
                        encode_string.decode('utf-8')

        # Remove thumbnail
        os.remove(thumbnail_fname)
        return thumbnail_b64

    def create_ts_thumbnail(self):
        """ Create a base64 encoded thumbnail """
        pass

    def get_feature_type(self, myopendap):
        """ Set feature type from OPeNDAP """
        logger.info("Now in get_feature_type")

        # Open as OPeNDAP
        try:
            ds = netCDF4.Dataset(myopendap)
        except Exception as e:
            logger.error("Something failed reading dataset: %s", str(e))

        # Try to get the global attribute featureType
        try:
            featureType = ds.getncattr('featureType')
        except Exception as e:
            logger.error("Something failed extracting featureType: %s", str(e))
            raise
        ds.close()

        if featureType not in ['point', 'timeSeries', 'trajectory', 'profile', 'timeSeriesProfile',
                               'trajectoryProfile']:
            logger.warning("The featureType found - %s - is not valid", featureType)
            logger.warning("Fixing this locally")
            if featureType.lower() == "timeseries":
                featureType = 'timeSeries'
            elif featureType == "timseries":
                featureType = 'timeSeries'
            else:
                logger.warning("The featureType found is a new typo...")
        return featureType

    def delete_level1(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        logger.info("Deleting %s from level 1.", datasetid)
        try:
            self.solrc.delete(id=datasetid)
        except Exception as e:
            logger.error("Something failed in SolR delete: %s", str(e))
            raise

        logger.info("Record successfully deleted from Level 1 core")

    def delete_level2(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        logger.info("Deleting %s from level 2.", datasetid)
        try:
            self.solr2.delete(id=datasetid)
        except Exception as e:
            logger.error("Something failed in SolR delete: %s", str(e))
            raise

        logger.info("Records successfully deleted from Level 2 core")

    def delete_thumbnail(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        logger.info("Deleting %s from thumbnail core.", datasetid)
        try:
            self.solrt.delete(id=datasetid)
        except Exception as e:
            logger.error("Something failed in SolR delete: %s", str(e))
            raise

        logger.info("Records successfully deleted from thumbnail core")

    def search(self):
        """ Require Id as input """
        try:
            results = pysolr.search('mmd_title:Sea Ice Extent', df='text_en', rows=100)
        except Exception as e:
            logger.error("Something failed during search: %s", str(e))

        return results

    def darextract(self, mydar):
        mylinks = {}
        for i in range(len(mydar)):
            if isinstance(mydar[i], bytes):
                mystr = str(mydar[i], 'utf-8')
            else:
                mystr = mydar[i]
            if mystr.find('description') != -1:
                t1, t2 = mystr.split(',', 1)
            else:
                t1 = mystr
            t2 = t1.replace('"', '')
            proto, myurl = t2.split(':', 1)
            mylinks[proto] = myurl

        return (mylinks)
