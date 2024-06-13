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


import base64
import pysolr
import netCDF4
import logging
import xmltodict
import requests
import dateutil.parser
# from dateutil.parser import ParserError
import lxml.etree as ET

import shapely.geometry as shpgeo

from metvocab.mmdgroup import MMDGroup
from shapely.geometry import box, mapping

from solrindexer.tools import rewrap, process_feature_type
from solrindexer.tools import to_solr_id, parse_date

logger = logging.getLogger(__name__)


class MMD4SolR:
    """ Read and check MMD files, convert to dictionary """

    def __init__(self, filename=None, mydoc=None, bulkFile=None):
        logger.debug('Creating an instance of MMD4SolR')
        logger.debug("filename is %s. mydoc is %s", filename, type(mydoc))
        self.filename = filename
        if filename is not None:
            try:
                with open(self.filename, encoding='utf-8') as fd:
                    self.mydoc = xmltodict.parse(fd.read())
            except Exception as e:
                logger.error('Could not open file: %s.\n Reason: %s', self.filename, e)
                raise

        if mydoc is not None and isinstance(mydoc, dict):
            logger.debug("Storing mydoc.")
            self.filename = bulkFile
            self.mydoc = mydoc

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
                logger.debug('Checking for: %s', requirement)
                if requirement in mmd:
                    if mmd[requirement] is not None:
                        logger.debug('%s is present and non empty', requirement)
                        mmd_requirements[requirement] = True
                    else:
                        logger.warning('Required element %s is missing, setting it to unknown',
                                       requirement)
                        mmd[requirement] = 'Unknown'
                else:
                    logger.warning('Required element %s is missing, setting it to unknown.',
                                   requirement)
                    mmd[requirement] = 'Unknown'

        logger.debug("Checking controlled vocabularies")
        # Should be collected from
        #    https://github.com/steingod/scivocab/tree/master/metno
        #  Is fetched from vocab.met.no via https://github.com/metno/met-vocab-tools

        mmd_iso_topic_category = MMDGroup(
            'mmd', 'https://vocab.met.no/mmd/ISO_Topic_Category')
        mmd_collection = MMDGroup(
            'mmd', 'https://vocab.met.no/mmd/Collection_Keywords')
        mmd_dataset_status = MMDGroup(
            'mmd', 'https://vocab.met.no/mmd/Dataset_Production_Status')
        mmd_quality_control = MMDGroup(
            'mmd', 'https://vocab.met.no/mmd/Quality_Control')
        mmd_controlled_elements = {
            'mmd:iso_topic_category': mmd_iso_topic_category,
            'mmd:collection': mmd_collection,
            'mmd:dataset_production_status': mmd_dataset_status,
            'mmd:quality_control': mmd_quality_control,
        }
        for element in mmd_controlled_elements.keys():
            logger.debug(
                'Checking %s for compliance with controlled vocabulary', element)
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
                        logger.warning(
                            '%s contains non valid content: %s', element, myvalue)

        """
        Check that keywords also contain GCMD keywords
        Need to check contents more specifically...
        """
        gcmd = False
        logger.debug("Checking for gmcd keywords")
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
        logger.debug("Checking last_metadata_update")
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
                                # myvalue = '0000-00-00:T00:00:00Z'
                                if isinstance(mydateels, list):
                                    for mydaterec in mydateels:
                                        # if mydaterec['mmd:datetime'] > myvalue:
                                        myvalue = parse_date(mydaterec['mmd:datetime'])
                                        if myvalue is None:
                                            raise ValueError("Date could not be parsed: %s",
                                                             mydaterec['mmd:datetime'])
                                else:
                                    myvalue = parse_date(mydateels['mmd:datetime'])
                                    if myvalue is None:
                                        raise ValueError("Date could not be parsed: %s",
                                                         mydateels['mmd:datetime'])

            else:
                # To be removed when all records are transformed into the
                # new format
                logger.warning('Removed D7 format in last_metadata_update')
                myvalue = parse_date(mmd['mmd:last_metadata_update'])
                if myvalue is None:
                    raise ValueError("Date could not be parsed: %s",
                                     mmd['mmd:last_metadata_update'])

        logger.debug("Checking temporal extent.")
        if 'mmd:temporal_extent' in mmd:
            # logger.debug(mmd['mmd:temporal_extent'])
            if isinstance(mmd['mmd:temporal_extent'], list):
                for item in mmd['mmd:temporal_extent']:
                    for mykey in item:
                        if (item[mykey] is None) or (item[mykey] == '--'):
                            mydate = ''
                            item[mykey] = mydate
                        else:
                            try:
                                mydate = dateutil.parser.parse(str(item[mykey]))
                                item[mykey] = mydate.strftime('%Y-%m-%dT%H:%M:%SZ')
                            except Exception as e:
                                logger.error(
                                    'Date format could not be parsed: %s', e)
            else:
                # logger.debug(mmd['mmd:temporal_extent'].items())
                for mykey, myitem in mmd['mmd:temporal_extent'].items():
                    if mykey == 'mmd:start_date' and myitem is None:
                        raise ValueError(
                            "Missing mmd:temporal_extent start_date. File will be skipped.")
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
                            logger.error(
                                'Date format could not be parsed: %s', e)

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
                                   'Software': 'software',
                                   }

        # As of python 3.6 Dictionaries are ordered by insertion (as OrderedDict)
        mydict = {}

        # Cheeky shorthand
        mmd = self.mydoc['mmd:mmd']

        # SolR Can't use the mmd:metadata_identifier as identifier if it contains :, replace :
        # and other characters like / etc by _ in the id field, let metadata_identifier be
        # the correct one.

        logger.debug("Identifier and metadata_identifier")
        if isinstance(mmd['mmd:metadata_identifier'], dict):
            myid = mmd['mmd:metadata_identifier']['#text']
            myid = to_solr_id(myid)
            mydict['id'] = myid
            mydict['metadata_identifier'] = \
                mmd['mmd:metadata_identifier']['#text']
        else:
            myid = mmd['mmd:metadata_identifier']
            myid = to_solr_id(myid)
            mydict['id'] = myid
            mydict['metadata_identifier'] = mmd['mmd:metadata_identifier']
        logger.debug("Got metadata_identifier: %s", mydict['metadata_identifier'])
        logger.debug("Last metadata update")
        if 'mmd:last_metadata_update' in mmd:
            last_metadata_update = mmd['mmd:last_metadata_update']
            lmu_datetime = []
            lmu_type = []
            lmu_note = []
            # FIXME check if this works correctly
            # Only one last_metadata_update element
            if isinstance(last_metadata_update['mmd:update'], dict):
                lmu_datetime.append(
                    str(last_metadata_update['mmd:update']['mmd:datetime']))
                lmu_type.append(last_metadata_update['mmd:update']['mmd:type'])
                lmu_note.append(
                    last_metadata_update['mmd:update'].get('mmd:note', ''))
            # Multiple last_metadata_update elements
            else:
                for i, e in enumerate(last_metadata_update['mmd:update']):
                    lmu_datetime.append(str(e['mmd:datetime']))
                    lmu_type.append(e['mmd:type'])
                    if 'mmd:note' in e.keys():
                        lmu_note.append(e['mmd:note'])
                    else:
                        lmu_note.append('Not provided')

            # Check  and fixdate format validity
            for i, _date in enumerate(lmu_datetime):
                date = parse_date(_date)
                lmu_datetime[i] = date

            mydict['last_metadata_update_datetime'] = lmu_datetime
            mydict['last_metadata_update_type'] = lmu_type
            mydict['last_metadata_update_note'] = lmu_note

        logger.debug("Metadata status")
        if isinstance(mmd['mmd:metadata_status'], dict):
            mydict['metadata_status'] = mmd['mmd:metadata_status']['#text']
        else:
            mydict['metadata_status'] = mmd['mmd:metadata_status']

        logger.debug("Collection")
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

        logger.debug("Title")
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

        logger.debug("Abstract")
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

        logger.debug("Temporal extent")
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
                mydict['temporal_extent_start_date'] = mintime.strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
                mydict['temporal_extent_end_date'] = maxtime.strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            else:
                mydict["temporal_extent_start_date"] = str(
                    mmd['mmd:temporal_extent']['mmd:start_date'])
                if 'mmd:end_date' in mmd['mmd:temporal_extent']:
                    if mmd['mmd:temporal_extent']['mmd:end_date'] is not None:
                        mydict["temporal_extent_end_date"] = str(
                            mmd['mmd:temporal_extent']['mmd:end_date'])

            if "temporal_extent_end_date" in mydict:
                logger.debug('Creating daterange with end date')
                st = str(mydict["temporal_extent_start_date"])
                end = str(mydict["temporal_extent_end_date"])
                mydict['temporal_extent_period_dr'] = '[' + st + ' TO ' + end + ']'
            else:
                logger.debug('Creating daterange with open end date')
                st = str(mydict["temporal_extent_start_date"])
                mydict['temporal_extent_period_dr'] = '[' + st + ' TO *]'
            logger.debug("Temporal extent date range: %s", mydict['temporal_extent_period_dr'])
        logger.debug("Geographical extent")
        # Assumes longitudes positive eastwards and in the are -180:180
        mmd_geographic_extent = mmd.get('mmd:geographic_extent', None)
        # logger.debug(type(mmd_geographic_extent))
        # logger.debug(mmd_geographic_extent)
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

                    # Test for numbers < -180 and > 180, and fix.
                    minlon = min(lonvals)
                    if minlon < -180.0:
                        minlon = rewrap(minlon)
                    maxlon = max(lonvals)
                    if maxlon > 180.0:
                        maxlon = rewrap(maxlon)
                    lonvals.clear()
                    lonvals.append(minlon)
                    lonvals.append(maxlon)

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
                            mydict['geospatial_bounds'] = mydict['bbox']
                            logger.debug(mapping(point))
                    else:
                        bbox = box(min(lonvals), min(latvals),
                                   max(lonvals), max(latvals))
                        logger.debug("First conditition")
                        logger.debug(bbox)
                        polygon = bbox.wkt
                        mydict['polygon_rpt'] = polygon
                        if not mydict['bbox'] == "ENVELOPE(-180.0,180.0,90,-90)":
                            mydict['geospatial_bounds'] = mydict['bbox']

                else:
                    mydict['geographic_extent_rectangle_north'] = 90.
                    mydict['geographic_extent_rectangle_south'] = -90.
                    mydict['geographic_extent_rectangle_west'] = -180.
                    mydict['geographic_extent_rectangle_east'] = 180.
            else:
                # logger.debug(type(mmd_geographic_extent['mmd:rectangle']))
                # logger.debug(mmd_geographic_extent['mmd:rectangle'])
                for item in mmd_geographic_extent['mmd:rectangle']:
                    if item is None:
                        logger.warning(
                            'Missing geographical element, will not process the file.')
                        mydict['metadata_status'] = 'Inactive'
                        raise Warning('Missing spatial bounds')

                # Test for numbers < -180 and > 180, and fix.
                minlon = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:west'])
                if minlon < -180.0:
                    minlon = rewrap(minlon)
                maxlon = float(
                    self.mydoc['mmd:mmd']['mmd:geographic_extent']['mmd:rectangle']['mmd:east'])
                if maxlon > 180.0:
                    maxlon = rewrap(maxlon)
                lonvals = []
                lonvals.append(minlon)
                lonvals.append(maxlon)

                north = float(
                    mmd_geographic_extent['mmd:rectangle']['mmd:north'])
                south = float(
                    mmd_geographic_extent['mmd:rectangle']['mmd:south'])
                east = float(
                    mmd_geographic_extent['mmd:rectangle']['mmd:east'])
                west = float(
                    mmd_geographic_extent['mmd:rectangle']['mmd:west'])
                mydict['geographic_extent_rectangle_north'] = north
                mydict['geographic_extent_rectangle_south'] = south
                mydict['geographic_extent_rectangle_east'] = east
                mydict['geographic_extent_rectangle_west'] = west

                """
                Check if bounding box is correct
                """
                if not mydict['geographic_extent_rectangle_north'] >= south:
                    logger.warning(
                        'Northernmost boundary is south of southernmost, will not process...')
                    mydict['metadata_status'] = 'Inactive'
                    raise Warning('Error in spatial bounds')
                if not mydict['geographic_extent_rectangle_east'] >= west:
                    logger.warning(
                        'Easternmost boundary is west of westernmost, will not process...')
                    mydict['metadata_status'] = 'Inactive'
                    raise Warning('Error in spatial bounds')
                if (east > 180 or west > 180 or east < -180 or west < -180):
                    logger.warning(
                        'Longitudes outside valid range, will not process...')
                    mydict['metadata_status'] = 'Inactive'
                    raise Warning('Error in longitude bounds')
                if (north > 90 or south > 90 or north < -90 or south < -90):
                    logger.warning(
                        'Latitudes outside valid range, will not process...')
                    mydict['metadata_status'] = 'Inactive'
                    raise Warning('Error in latitude bounds')

                srsname = mmd_geographic_extent['mmd:rectangle'].get(
                    '@srsName', None)
                if srsname is not None:
                    mydict['geographic_extent_rectangle_srsName'] = srsname

                mydict['bbox'] = "ENVELOPE(" + str(west) + "," + \
                    str(east) + "," + str(north) + "," + str(south) + ")"

                logger.debug("Second conditition")
                #  Check if we have a point or a boundingbox
                if south == north:
                    if east == west:
                        point = shpgeo.Point(east, north)
                        mydict['polygon_rpt'] = point.wkt
                        mydict['geospatial_bounds'] = point.wkt
                        logger.debug(mapping(point))

                else:
                    bbox = box(west, south, east, north, ccw=False)
                    polygon = bbox.wkt
                    logger.debug(polygon)
                    mydict['polygon_rpt'] = polygon
                    if not mydict['bbox'] == "ENVELOPE(-180.0,180.0,90,-90)":
                        mydict['geospatial_bounds'] = mydict['bbox']

        logger.debug("Dataset production status")
        if 'mmd:dataset_production_status' in mmd:
            if isinstance(mmd['mmd:dataset_production_status'], dict):
                mydict['dataset_production_status'] = mmd['mmd:dataset_production_status']['#text']
            else:
                mydict['dataset_production_status'] = str(
                    mmd['mmd:dataset_production_status'])

        logger.debug("Dataset language")
        if 'mmd:dataset_language' in mmd:
            mydict['dataset_language'] = str(mmd['mmd:dataset_language'])

        logger.debug("Operational status")
        if 'mmd:operational_status' in mmd:
            mydict['operational_status'] = str(mmd['mmd:operational_status'])

        logger.debug("Access constraints")
        if 'mmd:access_constraint' in mmd:
            mydict['access_constraint'] = str(mmd['mmd:access_constraint'])

        logger.debug("Use constraint")
        use_constraint = mmd.get('mmd:use_constraint', None)
        if use_constraint is not None:
            # Need both identifier and resource for use constraint
            if 'mmd:identifier' in use_constraint and 'mmd:resource' in use_constraint:
                mydict['use_constraint_identifier'] = str(
                    use_constraint['mmd:identifier'])
                mydict['use_constraint_resource'] = str(
                    use_constraint['mmd:resource'])
            else:
                logger.warning(
                    'Both license identifier and resource needed to index properly')
                mydict['use_constraint_identifier'] = "Not provided"
                mydict['use_constraint_resource'] = "Not provided"
            if 'mmd:license_text' in mmd['mmd:use_constraint']:
                mydict['use_constraint_license_text'] = str(
                    use_constraint['mmd:license_text'])

        logger.debug("Personnel")
        if 'mmd:personnel' in self.mydoc['mmd:mmd']:
            personnel_elements = self.mydoc['mmd:mmd']['mmd:personnel']

            if isinstance(personnel_elements, dict):  # Only one element
                # make it an iterable list
                personnel_elements = [personnel_elements]

            # Facet elements
            mydict['personnel_role'] = []
            mydict['personnel_name'] = []
            mydict['personnel_organisation'] = []
            # Fix role based lists
            for role in personnel_role_LUT:
                mydict['personnel_{}_role'.format(
                    personnel_role_LUT[role])] = []
                mydict['personnel_{}_name'.format(
                    personnel_role_LUT[role])] = []
                mydict['personnel_{}_email'.format(
                    personnel_role_LUT[role])] = []
                mydict['personnel_{}_phone'.format(
                    personnel_role_LUT[role])] = []
                mydict['personnel_{}_fax'.format(
                    personnel_role_LUT[role])] = []
                mydict['personnel_{}_organisation'.format(
                    personnel_role_LUT[role])] = []
                mydict['personnel_{}_address'.format(
                    personnel_role_LUT[role])] = []
                # don't think this is needed Øystein Godøy, METNO/FOU, 2021-09-08
                # mydict['personnel_{}_address_address'.format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_address_city'
                       .format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_address_province_or_state'
                       .format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_address_postal_code'
                       .format(personnel_role_LUT[role])] = []
                mydict['personnel_{}_address_country'
                       .format(personnel_role_LUT[role])] = []

            # Fill lists with information
            for personnel in personnel_elements:
                role = personnel['mmd:role']
                if not role:
                    logger.warning('No role available for personnel')
                    break
                if role not in personnel_role_LUT:
                    logger.warning('Wrong role provided for personnel')
                    break
                for entry in personnel:
                    entry_type = entry.split(':')[-1]
                    if entry_type == 'role':
                        mydict['personnel_{}_role'.format(personnel_role_LUT[role])] \
                            .append(personnel[entry])
                        mydict['personnel_role'].append(personnel[entry])
                    else:
                        # Treat address specifically and handle faceting elements
                        # personnel_role, personnel_name, personnel_organisation.
                        if entry_type == 'contact_address':
                            for el in personnel[entry]:
                                el_type = el.split(':')[-1]
                                if el_type == 'address':
                                    mydict['personnel_{}_{}'.
                                           format(personnel_role_LUT[role], el_type)] \
                                        .append(personnel[entry][el])
                                else:
                                    mydict['personnel_{}_address_{}'
                                           .format(personnel_role_LUT[role], el_type)] \
                                        .append(personnel[entry][el])
                        elif entry_type == 'name':
                            mydict['personnel_{}_{}'.
                                   format(personnel_role_LUT[role], entry_type)] \
                                .append(personnel[entry])
                            mydict['personnel_name'].append(personnel[entry])
                        elif entry_type == 'organisation':
                            mydict['personnel_{}_{}'.
                                   format(personnel_role_LUT[role], entry_type)] \
                                .append(personnel[entry])
                            mydict['personnel_organisation'].append(
                                personnel[entry])
                        else:
                            mydict['personnel_{}_{}'.
                                   format(personnel_role_LUT[role], entry_type)] \
                                .append(personnel[entry])

        logger.debug("Data center")
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

        logger.debug("Data access")
        # NOTE: This is identical to method above. Should in be simplified as method
        if 'mmd:data_access' in mmd:
            data_access_elements = mmd['mmd:data_access']
            # Only one element
            if isinstance(data_access_elements, dict):
                # make it an iterable list
                data_access_elements = [data_access_elements]
            # iterate over all data_center elements
            for data_access in data_access_elements:
                data_access_type = data_access['mmd:type'].replace(
                    " ", "_").lower()
                mydict[f'data_access_url_{data_access_type}'] = data_access['mmd:resource']

                if 'mmd:wms_layers' in data_access and data_access_type == 'ogc_wms':
                    data_access_wms_layers_string = 'data_access_wms_layers'
                    # Map directly to list
                    data_access_wms_layers = None
                    da_wms = data_access['mmd:wms_layers']['mmd:wms_layer']
                    if isinstance(da_wms, str):
                        data_access_wms_layers = [da_wms]
                    if isinstance(da_wms, list):
                        data_access_wms_layers = list(da_wms)
                    # old version was [i for i in data_access_wms_layers.values()][0]
                    if data_access_wms_layers is not None:
                        mydict[data_access_wms_layers_string] = data_access_wms_layers
                        logger.debug("WMS layers: %s", data_access_wms_layers)

        logger.debug("Related dataset")
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
                                myid = to_solr_id(
                                    mydict['related_dataset_id'])
                                mydict['related_dataset_id'] = myid
            else:
                # Not sure if this is used??
                if '#text' in dict(mmd['mmd:related_dataset']):
                    mydict['related_dataset'] = mmd['mmd:related_dataset']['#text']
                    mydict['related_dataset_id'] = mydict['related_dataset']
                    myid = to_solr_id(mydict['related_dataset_id'])
                    mydict['related_dataset_id'] = myid

        logger.debug("Storage information")
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
                mydict['storage_information_file_location'] = str(
                    file_location)
            if file_format is not None:
                mydict['storage_information_file_format'] = str(file_format)
            if file_size is not None:
                if isinstance(file_size, dict):
                    mydict['storage_information_file_size'] = str(
                        file_size['#text'])
                    mydict['storage_information_file_size_unit'] = str(
                        file_size['@unit'])
                else:
                    logger.warning(
                        "Filesize unit not specified, skipping field")
            if checksum is not None:
                if isinstance(checksum, dict):
                    mydict['storage_information_file_checksum'] = str(
                        checksum['#text'])
                    mydict['storage_information_file_checksum_type'] = str(
                        checksum['@type'])
                else:
                    logger.warning(
                        "Checksum type is not specified, skipping field")

        logger.debug("Related information")
        if 'mmd:related_information' in mmd:
            related_information_elements = mmd['mmd:related_information']

            # Only one element
            if isinstance(related_information_elements, dict):
                # make it an iterable list
                related_information_elements = [related_information_elements]

            for related_information in related_information_elements:
                value = related_information['mmd:type']
                if value in related_information_LUT.keys():
                    # if list does not exist, create it
                    if 'related_url_{}'.format(
                            related_information_LUT[value]) not in mydict.keys():
                        mydict['related_url_{}'.format(related_information_LUT[value])] = []
                        mydict['related_url_{}_desc'.format(related_information_LUT[value])] = []

                    # append elements to lists
                    mydict['related_url_{}'.format(
                        related_information_LUT[value])].append(
                            related_information['mmd:resource'])
                    ts = 'mmd:description'
                    if ts in related_information and related_information[ts] is not None:
                        mydict['related_url_{}_desc'.format(
                            related_information_LUT[value])].append(
                                related_information[ts])
                    else:
                        mydict['related_url_{}_desc'.format(
                            related_information_LUT[value])].append('Not Available')
        logger.debug("ISO TopicCategory")

        if 'mmd:iso_topic_category' in mmd:
            mydict['iso_topic_category'] = []
            if isinstance(mmd['mmd:iso_topic_category'], list):
                for iso_topic_category in mmd['mmd:iso_topic_category']:
                    mydict['iso_topic_category'].append(iso_topic_category)
            else:
                mydict['iso_topic_category'].append(
                    mmd['mmd:iso_topic_category'])

        logger.debug("Keywords")
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
                        mydict['keywords_gcmd'].append(
                            mmd['mmd:keywords']['mmd:keyword'])
                    mydict['keywords_keyword'].append(
                        mmd['mmd:keywords']['mmd:keyword'])
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
                                mydict['keywords_vocabulary'].append(
                                    elem['@vocabulary'])
                                mydict['keywords_keyword'].append(keyword)
                        else:
                            # logger.debug(type(elem))
                            if elem['@vocabulary'] == "None" or elem['@vocabulary'] == "GCMDSK":
                                mydict['keywords_gcmd'].append(
                                    elem['mmd:keyword'])
                            mydict['keywords_vocabulary'].append(
                                elem['@vocabulary'])
                            mydict['keywords_keyword'].append(
                                elem['mmd:keyword'])

            else:
                if mmd['mmd:keywords']['@vocabulary'] == "GCMDSK":
                    mydict['keywords_gcmd'].append(
                        mmd['mmd:keywords']['mmd:keyword'])
                mydict['keywords_vocabulary'].append(
                    mmd['mmd:keywords']['@vocabulary'])
                mydict['keywords_keyword'].append(
                    mmd['mmd:keywords']['mmd:keyword'])

        logger.debug("Project")
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

        logger.debug("Platform")
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
                        element_name = 'platform_{}'.format(
                            platform_key.split(':')[-1])
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

        logger.debug("Activity type")
        if 'mmd:activity_type' in mmd:
            mydict['activity_type'] = []
            if isinstance(mmd['mmd:activity_type'], list):
                for activity_type in mmd['mmd:activity_type']:
                    mydict['activity_type'].append(activity_type)
            else:
                mydict['activity_type'].append(mmd['mmd:activity_type'])

        logger.debug("Dataset citation")
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
                            logger.debug("Got publication date %s", v)
                            v = parse_date(v)

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

        """Set defualt parent/child flags"""
        mydict['isParent'] = False
        mydict['isChild'] = False

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
        self.thumbClass = None

        # Solr authentication
        self.authentication = authentication

        # Keep track of solr endpoint
        self.solr_url = mysolrserver

        # Connecting to core
        try:
            self.solrc = pysolr.Solr(mysolrserver, always_commit=always_commit, timeout=1020,
                                     auth=self.authentication)
            logger.info("Connection established to: %s", str(mysolrserver))
        except Exception as e:
            logger.error("Something failed in SolR init: %s", str(e))
            raise e

    # Function for sending explicit commit to solr
    def commit(self):
        self.solrc.commit()

    def get_status(self):
        """Get SolR core status information"""
        tmp = self.solr_url.split('/')
        core = tmp[-1]
        base_url = '/'.join(tmp[0:-1])
        logger.debug("Getting status with url %s and core %s", base_url, core)
        res = None
        try:
            res = requests.get(base_url + '/admin/cores?wt=json&action=STATUS&core=' + core,
                               auth=self.authentication)
            res.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            logger.error("Http Error: %s", errh)
        except requests.exceptions.ConnectionError as errc:
            logger.error("Error Connecting: %s", errc)
        except requests.exceptions.Timeout as errt:
            logger.error("Timeout Error: %s", errt)
        except requests.exceptions.RequestException as err:
            logger.error("OOps: Something Else went wrong: %s", err)

        if res is None:
            return None
        else:
            status = res.json()
            return status['status'][core]['index']

    def add_thumbnail(self, url, wms_layers_mmd, thumbnail_type='wms'):
        """ Add thumbnail to SolR
            Args:
                type: Thumbnail type. (wms, ts)
            Returns:
                thumbnail: base64 string representation of image
        """
        logger.info("adding thumbnail for: %s" % url)
        if thumbnail_type == 'wms':
            try:
                thumbnail = self.thumbClass.create_wms_thumbnail(url, self.id, wms_layers_mmd)
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

    def index_record(self, records2ingest, addThumbnail, level=None, thumbClass=None):
        # FIXME, update the text below Øystein Godøy, METNO/FOU, 2023-03-19
        """ Add thumbnail to SolR
            Args:
                input_record : list of solr dicts or a single solr dict, to be indexed in SolR
                addThumbnail (bool): If thumbnail should be added or not
                level (1,2,None): Explicitt tell indexer what level the record have.
                thumbClass (Class): A class with the thumbnail genration.

            Returns:
                bool, msg
        """

        # Check if we got a list of records or single record:
        if not isinstance(records2ingest, list):
            records2ingest = [records2ingest]

        """Handle thumbnail generator"""
        self.thumbClass = thumbClass
        if thumbClass is None:
            addThumbnail = False

        logger.debug("Thumbnail flag is: %s", addThumbnail)

        mmd_records = list()
        norec = len(records2ingest)
        i = 1
        for input_record in records2ingest:
            logger.info("====>")
            logger.info("Processing record %d of %d", i, norec)
            i += 1
            # Do some checking of content
            self.id = input_record['id']
            if input_record['metadata_status'] == 'Inactive':
                logger.warning('This record will be set inactive...')
                # return False
            myfeature = None

            """ Handle explicit dataset level parent/children relations"""
            if level == 1:
                input_record.update({'dataset_type': 'Level-1'})
            if level == 2:
                input_record.update({'dataset_type': 'Level-2'})
                input_record.update({'isChild': True})

            """
            If OGC WMS is available, no point in looking for featureType in OPeNDAP.
            """

            if 'data_access_url_ogc_wms' in input_record and addThumbnail:
                logger.info("Checking thumbnails...")
                getCapUrl = input_record['data_access_url_ogc_wms']
                mmd_layers = None
                if 'data_access_wms_layers' in input_record:
                    mmd_layers = input_record['data_access_wms_layers']
                if not myfeature:
                    self.thumbnail_type = 'wms'
                thumbnail_data = self.add_thumbnail(getCapUrl, mmd_layers)

                if thumbnail_data is None:
                    logger.warning(
                        'Could not properly parse WMS GetCapabilities document')
                    # If WMS is not available, remove this data_access element
                    # from the XML that is indexed
                    del input_record['data_access_url_ogc_wms']
                else:
                    input_record.update({'thumbnail_data': thumbnail_data})

            if 'data_access_url_opendap' in input_record:
                # Thumbnail of timeseries to be added
                # Or better do this as part of get_feature_type?
                logger.info("Processing feature type")
                input_record = process_feature_type(input_record)

            logger.info("Adding records to list...")
            mmd_records.append(input_record)

        """
        Send information to SolR
        """
        logger.info("Adding records to SolR core.")
        try:
            self.solrc.add(mmd_records)
        except Exception as e:
            msg = "Something failed in SolR adding document: %s" % str(e)
            logger.critical(msg)
            return False, msg
        msg = "Record successfully added."
        logger.info("Record successfully added.")

        del mmd_records

        return True, msg

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
        except AttributeError:
            raise
        except Exception as e:
            logger.error("Something failed extracting featureType: %s", str(e))
            raise
        ds.close()

        if featureType not in ['point', 'timeSeries', 'trajectory', 'profile', 'timeSeriesProfile',
                               'trajectoryProfile']:
            logger.warning(
                "The featureType found - %s - is not valid", featureType)
            logger.warning("Fixing this locally")
            if featureType.lower() == "timeseries":
                featureType = 'timeSeries'
            elif featureType == "timseries":
                featureType = 'timeSeries'
            else:
                logger.warning("The featureType found is a new typo...")
        return featureType

    def create_thumbnail(self, doc):
        """ Add thumbnail to SolR
            Args:
                type: solr document
            Returns:
                solr document with thumbnail
        """
        url = str(doc['data_access_url_ogc_wms']).strip()
        logger.debug("adding thumbnail for: %s", url)
        id = str(doc['id']).strip()
        try:
            thumbnail_data = self.thumbClass.create_wms_thumbnail(url, id)
            doc.update({'thumbnail_data': thumbnail_data})
            return doc
        except Exception as e:
            logger.error("Thumbnail creation from OGC WMS failed: %s", e)
            return doc

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

    def delete_thumbnail(self, datasetid):
        """ Require ID as input """
        """ Rewrite to take full metadata record as input """
        logger.info("Deleting %s from thumbnail core.", datasetid)
        try:
            self.solrc.delete(id=datasetid)
        except Exception as e:
            logger.error("Something failed in SolR delete: %s", str(e))
            raise

        logger.info("Records successfully deleted from thumbnail core")

    def search(self):
        """ Require Id as input """
        try:
            results = pysolr.search(
                'mmd_title:Sea Ice Extent', df='text_en', rows=100)
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

    def delete(self, id, commit=False):
        """Delete document with given metadata identifier"""
        solr_id = to_solr_id(id)
        doc_exsists = self.get_dataset(solr_id)
        if (doc_exsists["doc"] is None):
            return False, "Document %s not found in index." % id
        try:
            self.solrc.delete(id=solr_id)
        except Exception as e:
            logger.error(
                "Something went wrong deleting doucument with id: %s", id)
            return False, e
        logger.info("Sucessfully deleted document with id: %s", id)
        if commit:
            logger.info("Commiting deletion")
            self.commit()
        return True, "Document %s sucessfully deleted" % id

    def get_dataset(self, id):
        """
        Use real-time get to fetch latest dataset
        based on id.
        """
        res = None
        try:
            res = requests.get(self.solr_url + '/get?wt=json&id=' + id,
                               auth=self.authentication)
            res.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            logger.error("Http Error: %s", errh)
        except requests.exceptions.ConnectionError as errc:
            logger.error("Error Connecting: %s", errc)
        except requests.exceptions.Timeout as errt:
            logger.error("Timeout Error: %s", errt)
        except requests.exceptions.RequestException as err:
            logger.error("OOps: Something Else went wrong: %s", err)

        if res is None:
            return None
        else:
            dataset = res.json()
            return dataset

    @staticmethod
    def _solr_update_parent_doc(parent):
        """
        Update the parent document we got from solr.
        some fields need to be removed for solr to accept the update.
        """
        if 'full_text' in parent:
            parent.pop('full_text')
        if 'bbox__maxX' in parent:
            parent.pop('bbox__maxX')
        if 'bbox__maxY' in parent:
            parent.pop('bbox__maxY')
        if 'bbox__minX' in parent:
            parent.pop('bbox__minX')
        if 'bbox__minY' in parent:
            parent.pop('bbox__minY')
        if 'bbox_rpt' in parent:
            parent.pop('bbox_rpt')
        if 'ss_access' in parent:
            parent.pop('ss_access')
        if '_version_' in parent:
            parent.pop('_version_')

        parent['isParent'] = True
        return parent

    def update_parent(self, parentid, fail_on_missing=False, handle_missing_status=True):
        """Search index for parent and update parent flag.

            Parameters:
                parentid - (str) the parent id to find and update
                fail_on_missing - (bool) - Return False on missing parents if set to True
                handle_missing_status - (bool) If fail_on_missing is false,
                                        this parameter is used to return false or true
                                        back to the calling code. Logs a warning about
                                        missing parent.
        """
        myparent = self.get_dataset(parentid)

        if myparent is None:
            return False, "No parent found in index."
        else:
            if myparent['doc'] is None:
                if fail_on_missing is True:
                    return False, "Parent %s is not in the index. Index parent first." % parentid
                else:
                    logger.warn("Parent %s is not in the index. Make sure to index parent first.",
                                parentid)
                    msg = "WARNING! Parent is not in the index. "
                    msg += "Make sure to index parent and then the children "
                    msg += "for relation to be updated."
                    return (handle_missing_status, msg)

            logger.info("Got parent: %s",
                        myparent['doc']['metadata_identifier'])
            if bool(myparent['doc']['isParent']):
                logger.info("Dataset already marked as parent.")
                return True, "Already updated."
            else:
                # doc = {'id': parentid, 'isParent': True} TODO: Fix schema so atomic updates works
                doc = self._solr_update_parent_doc(myparent['doc'])
                try:
                    # self.solrc.add([doc],fieldUpdates={'isParent': 'set'})TODO:fix atomic updates
                    self.solrc.add([doc])
                except Exception as e:
                    logger.error(
                        "Atomic update failed on parent %s. Error is: ", (parentid, e))
                    return False, e
                logger.info("Parent sucessfully updated in SolR.")
                return True, "Parent updated."
