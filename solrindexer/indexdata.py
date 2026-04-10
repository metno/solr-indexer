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
"""

import html
import json
import logging
import os
import sys

import lxml.etree as ET
import netCDF4
import pysolr
import requests
from metvocab.mmdgroup import MMDGroup

from solrindexer.thumb.thumbnail_api import create_wms_thumbnail_api
from solrindexer.tools import (
    add_nbs_thumbnail,
    parse_date,
    process_feature_type,
    to_solr_id,
)

logger = logging.getLogger(__name__)


class MMD4SolR:
    """Read and validate MMD XML, convert directly to a Solr-ready dictionary."""

    NS_MMD = "http://www.met.no/schema/mmd"
    NS_GML = "http://www.opengis.net/gml"
    NSMAP = {"mmd": NS_MMD, "gml": NS_GML}

    REQUIRED_ELEMENTS = (
        "metadata_identifier",
        "title",
        "abstract",
        "metadata_status",
        "dataset_production_status",
        "collection",
        "last_metadata_update",
        "iso_topic_category",
        "keywords",
    )

    CONTROLLED_ELEMENTS = {
        "iso_topic_category": "https://vocab.met.no/mmd/ISO_Topic_Category",
        "collection": "https://vocab.met.no/mmd/Collection_Keywords",
        "dataset_production_status": "https://vocab.met.no/mmd/Dataset_Production_Status",
        "quality_control": "https://vocab.met.no/mmd/Quality_Control",
        "metadata_source": "https://vocab.met.no/mmd/Metadata_Source",
    }

    def __init__(self, filename=None, mydoc=None, bulkFile=None):
        logger.debug("Creating an instance of MMD4SolR")
        self.filename = filename if filename is not None else bulkFile
        self.root = None

        if filename is not None:
            try:
                self.root = ET.parse(str(filename)).getroot()
            except Exception as exc:
                logger.error("Could not open file %s. Reason: %s", filename, exc)
                raise
        elif mydoc is not None:
            if isinstance(mydoc, ET._Element):
                self.root = mydoc
            elif isinstance(mydoc, ET._ElementTree):
                self.root = mydoc.getroot()
            else:
                raise TypeError(f"Unsupported MMD document type: {type(mydoc)}")

        if self.root is None:
            raise ValueError("No XML content available for MMD4SolR")

    def _icon(self, kind):
        ascii_icons = os.getenv("SOLRINDEXER_ASCII_ICONS", "0") == "1"
        if ascii_icons:
            return {"ok": "[OK]", "warn": "[WARN]", "fail": "[FAIL]"}[kind]
        return {"ok": "✔", "warn": "⚠", "fail": "✖"}[kind]

    def _nodes(self, xpath):
        return self.root.xpath(xpath, namespaces=self.NSMAP)

    @staticmethod
    def _text(node):
        if node is None:
            return None
        value = "".join(node.itertext()).strip()
        return value or None

    def _first_text(self, xpath):
        nodes = self._nodes(xpath)
        if not nodes:
            return None
        return self._text(nodes[0])

    def _all_text(self, xpath):
        return [value for value in (self._text(node) for node in self._nodes(xpath)) if value]

    @staticmethod
    def _normalize_datetime(value):
        if not value:
            return None
        try:
            return parse_date(value)
        except Exception:
            return value

    def _first_text_for(self, node, xpath):
        nodes = node.xpath(xpath, namespaces=self.NSMAP)
        if not nodes:
            return None
        return self._text(nodes[0])

    def check_mmd(self):
        try:
            return self._check_mmd_body()
        except Exception as exc:
            logger.error("%s check_mmd failed for %s: %s", self._icon("fail"), self.filename, exc)
            return False

    def _check_mmd_body(self):
        status_ok = True
        for tag in self.REQUIRED_ELEMENTS:
            value = self._first_text(f"./mmd:{tag}")
            if value:
                logger.info("%s check_mmd mmd:%s", self._icon("ok"), tag)
            else:
                status_ok = False
                logger.warning("%s check_mmd missing required mmd:%s", self._icon("fail"), tag)

        for tag, vocab_url in self.CONTROLLED_ELEMENTS.items():
            values = self._all_text(f"./mmd:{tag}")
            if not values:
                continue
            group = MMDGroup("mmd", vocab_url)
            for value in values:
                if not group.search(value):
                    logger.warning(
                        "%s mmd:%s has non-controlled value: %s",
                        self._icon("warn"),
                        tag,
                        value,
                    )

        gcmd_values = []
        for keywords in self._nodes("./mmd:keywords"):
            vocabulary = (keywords.attrib.get("vocabulary") or "").upper()
            if vocabulary == "GCMDSK":
                gcmd_values.extend(
                    [self._text(node) for node in keywords.xpath("./mmd:keyword", namespaces=self.NSMAP)]
                )
        if not gcmd_values:
            logger.warning("%s Keywords in GCMD are not available", self._icon("warn"))

        return status_ok

    @staticmethod
    def _set_multilang(entries, base_name, target):
        default_value = None
        for lang, value in entries:
            if not value:
                continue
            if default_value is None:
                default_value = value
            lang_norm = (lang or "").lower()
            if lang_norm.startswith("en"):
                target[f"{base_name}_en"] = value
            elif lang_norm.startswith("no"):
                target[f"{base_name}_no"] = value
        if default_value is not None:
            target[base_name] = default_value
            target.setdefault(f"{base_name}_en", default_value)

    def _extract_last_metadata_update(self, solr_doc):
        updates = self._nodes("./mmd:last_metadata_update/mmd:update")
        if not updates:
            return
        updates_sorted = sorted(
            updates,
            key=lambda node: self._normalize_datetime(self._first_text_for(node, "./mmd:datetime")) or "",
        )
        latest = updates_sorted[-1]
        dt = self._normalize_datetime(self._first_text_for(latest, "./mmd:datetime"))
        typ = self._first_text_for(latest, "./mmd:type")
        note = self._first_text_for(latest, "./mmd:note")
        if dt:
            solr_doc["last_metadata_update_datetime"] = dt
        if typ:
            solr_doc["last_metadata_update_type"] = typ
        if note:
            solr_doc["last_metadata_update_note"] = note

    def _extract_temporal_extent(self, solr_doc):
        starts = []
        ends = []
        periods = []
        for extent in self._nodes("./mmd:temporal_extent"):
            start = self._normalize_datetime(self._first_text_for(extent, "./mmd:start_date"))
            end = self._normalize_datetime(self._first_text_for(extent, "./mmd:end_date"))
            if not start:
                continue
            starts.append(start)
            if end:
                ends.append(end)
                periods.append(f"[{start} TO {end}]")
            else:
                periods.append(f"[{start} TO *]")

        if starts:
            solr_doc["temporal_extent_start_date"] = starts[0] if len(starts) == 1 else starts
        if ends:
            solr_doc["temporal_extent_end_date"] = ends[0] if len(ends) == 1 else ends
        if periods:
            solr_doc["temporal_extent_period_dr"] = periods[0] if len(periods) == 1 else periods

    def _extract_rectangle(self, geo_extent):
        north = self._first_text_for(geo_extent, "./mmd:rectangle/mmd:north")
        south = self._first_text_for(geo_extent, "./mmd:rectangle/mmd:south")
        east = self._first_text_for(geo_extent, "./mmd:rectangle/mmd:east")
        west = self._first_text_for(geo_extent, "./mmd:rectangle/mmd:west")
        if None in (north, south, east, west):
            return None
        return float(north), float(south), float(east), float(west)

    def _extract_geographic_extent(self, solr_doc):
        extents = self._nodes("./mmd:geographic_extent")
        if not extents:
            return
        rectangle = self._extract_rectangle(extents[0])
        if rectangle is None:
            return
        north, south, east, west = rectangle
        solr_doc["geographic_extent_rectangle_north"] = north
        solr_doc["geographic_extent_rectangle_south"] = south
        solr_doc["geographic_extent_rectangle_east"] = east
        solr_doc["geographic_extent_rectangle_west"] = west
        solr_doc["bbox"] = f"ENVELOPE({west},{east},{north},{south})"
        if solr_doc["bbox"] != "ENVELOPE(-180.0,180.0,90.0,-90.0)":
            solr_doc["geospatial_bounds"] = solr_doc["bbox"]

    def _extract_keywords(self, solr_doc):
        keyword_all = []
        vocab_all = []
        keyword_targets = {
            "GCMDSK": "keywords_gcmd",
            "WIGOS": "keywords_wigos",
            "GCMDLOC": "keywords_gcmdloc",
            "GCMDPROV": "keywords_gcmdprov",
            "CFSTDN": "keywords_cfstdn",
            "GEMET": "keywords_gemet",
            "NORTHEMES": "keywords_northemes",
        }

        for keywords in self._nodes("./mmd:keywords"):
            vocab = (keywords.attrib.get("vocabulary") or "none").upper()
            items = [self._text(node) for node in keywords.xpath("./mmd:keyword", namespaces=self.NSMAP)]
            items = [item for item in items if item]
            if not items:
                continue
            keyword_all.extend(items)
            vocab_all.extend([vocab] * len(items))
            target_field = keyword_targets.get(vocab, "keywords_none")
            solr_doc.setdefault(target_field, []).extend(items)

        if keyword_all:
            solr_doc["keywords_keyword"] = keyword_all
            solr_doc["keywords_vocabulary"] = vocab_all

    def _extract_related_dataset(self, solr_doc):
        for node in self._nodes("./mmd:related_dataset"):
            relation_type = (node.attrib.get("relation_type") or "").lower()
            value = self._text(node)
            if not value:
                continue
            if relation_type in ("parent", ""):
                solr_doc["related_dataset"] = value
                solr_doc["related_dataset_id"] = to_solr_id(value)
            elif relation_type == "auxiliary":
                solr_doc["related_dataset_auxiliary"] = value
                solr_doc["related_dataset_auxiliary_id"] = to_solr_id(value)

    def _extract_personnel(self, solr_doc):
        personnel_json = []
        roles, names, orgs = [], [], []
        for node in self._nodes("./mmd:personnel"):
            role = self._first_text_for(node, "./mmd:role")
            name = self._first_text_for(node, "./mmd:name")
            organisation = self._first_text_for(node, "./mmd:organisation")
            if role:
                roles.append(role)
            if name:
                names.append(name)
            if organisation:
                orgs.append(organisation)
            personnel_json.append(
                {
                    "role": role,
                    "name": name,
                    "organisation": organisation,
                    "email": self._first_text_for(node, "./mmd:email"),
                }
            )

        if roles:
            solr_doc["personnel_role"] = sorted(set(roles))
        if names:
            dedup_names = sorted(set(names))
            solr_doc["personnel_name"] = dedup_names
            solr_doc["personnel_name_facet"] = dedup_names
        if orgs:
            dedup_orgs = sorted(set(orgs))
            solr_doc["personnel_organisation"] = dedup_orgs
            solr_doc["personnel_organisation_facet"] = dedup_orgs
        if personnel_json:
            solr_doc["personnel_json"] = json.dumps(personnel_json, ensure_ascii=False, separators=(",", ":"))

    def _extract_data_access(self, solr_doc):
        urls, opendap = [], []
        for node in self._nodes("./mmd:data_access"):
            resource = self._first_text_for(node, "./mmd:resource")
            if not resource:
                continue
            urls.append(resource)
            access_type = (self._first_text_for(node, "./mmd:type") or "").lower()
            if "opendap" in access_type:
                opendap.append(resource)
        if urls:
            solr_doc["data_access_url"] = urls
            solr_doc["data_access_url_http"] = urls
        if opendap:
            solr_doc["data_access_url_opendap"] = opendap

    def _extract_projects(self, solr_doc):
        short_names, long_names, project_names = [], [], []
        for node in self._nodes("./mmd:project"):
            short = self._first_text_for(node, "./mmd:short_name") or "Not provided"
            long = self._first_text_for(node, "./mmd:long_name") or "Not provided"
            short_names.append(short)
            long_names.append(long)
            project_names.append(f"{short}: {long}")
        if project_names:
            solr_doc["project_short_name"] = short_names
            solr_doc["project_long_name"] = long_names
            solr_doc["project_name"] = project_names

    def tosolr(self):
        solr_doc = {}

        metadata_identifier = self._first_text("./mmd:metadata_identifier")
        if metadata_identifier:
            solr_doc["metadata_identifier"] = metadata_identifier
            solr_doc["id"] = to_solr_id(metadata_identifier)

        solr_doc["metadata_status"] = self._first_text("./mmd:metadata_status") or "Unknown"
        solr_doc["dataset_production_status"] = self._first_text("./mmd:dataset_production_status") or "Unknown"

        collections = self._all_text("./mmd:collection")
        if collections:
            solr_doc["collection"] = collections

        titles = [
            (node.attrib.get("{http://www.w3.org/XML/1998/namespace}lang"), self._text(node))
            for node in self._nodes("./mmd:title")
        ]
        abstracts = [
            (node.attrib.get("{http://www.w3.org/XML/1998/namespace}lang"), self._text(node))
            for node in self._nodes("./mmd:abstract")
        ]
        self._set_multilang(titles, "title", solr_doc)
        self._set_multilang(abstracts, "abstract", solr_doc)

        dataset_language = self._first_text("./mmd:dataset_language")
        if dataset_language:
            solr_doc["dataset_language"] = dataset_language

        operational_status = self._first_text("./mmd:operational_status")
        if operational_status:
            solr_doc["operational_status"] = operational_status

        access_constraint = self._first_text("./mmd:access_constraint")
        if access_constraint:
            solr_doc["access_constraint"] = access_constraint

        use_constraint = self._nodes("./mmd:use_constraint")
        if use_constraint:
            node = use_constraint[0]
            solr_doc["use_constraint_identifier"] = self._first_text_for(node, "./mmd:identifier") or "Not provided"
            solr_doc["use_constraint_resource"] = self._first_text_for(node, "./mmd:resource") or "Not provided"
            license_text = self._first_text_for(node, "./mmd:license_text")
            if license_text:
                solr_doc["use_constraint_license_text"] = license_text

        self._extract_last_metadata_update(solr_doc)
        self._extract_temporal_extent(solr_doc)
        self._extract_geographic_extent(solr_doc)
        self._extract_keywords(solr_doc)
        self._extract_related_dataset(solr_doc)
        self._extract_personnel(solr_doc)
        self._extract_data_access(solr_doc)
        self._extract_projects(solr_doc)

        iso_topic_category = self._all_text("./mmd:iso_topic_category")
        if iso_topic_category:
            solr_doc["iso_topic_category"] = iso_topic_category

        quality_control = self._first_text("./mmd:quality_control")
        if quality_control:
            solr_doc["quality_control"] = quality_control

        metadata_source = self._first_text("./mmd:metadata_source")
        if metadata_source:
            solr_doc["metadata_source"] = metadata_source

        xml_string = ET.tostring(self.root, pretty_print=True, encoding="unicode")
        solr_doc["mmd_xml_file"] = html.unescape(xml_string)

        solr_doc["isParent"] = False
        solr_doc["isChild"] = False

        return solr_doc

class IndexMMD:
    """Class for indexing SolR representation of MMD to SolR server. Requires
    a list of dictionaries representing MMD as input.
    """

    def __init__(self, mysolrserver, always_commit=False, authentication=None, config=None):
        # Set up logging
        if mysolrserver is None:
            logger.error("No Solr URL was provided")
            sys.exit(1)

        logger.info("Creating an instance of IndexMMD")
        logger.info(f"Always commit is: {always_commit}")

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

        # The config object
        self.config = {} if config is None else config

        # Solr authentication
        self.authentication = authentication

        # Keep track of solr endpoint
        self.solr_url = mysolrserver

        # Keep track of wms url tasks for later results.
        self.wms_task_list = []

        # Connecting to core
        try:
            self.solrc = pysolr.Solr(
                mysolrserver, always_commit=always_commit, timeout=1020, auth=self.authentication
            )
            logger.info("Connection established to: %s", str(mysolrserver))
        except Exception as e:
            logger.error("Something failed in SolR init: %s", str(e))
            raise e
        try:
            pong = self.solrc.ping()
            status = json.loads(pong)["status"]
            if status == "OK":
                logger.info("Solr ping with status %s", status)
            else:
                logger.error("Error! Solr ping with status %s", status)
                sys.exit(1)

        except pysolr.SolrError as e:
            logger.error(f"Could not contact solr server: {e}")
            sys.exit(1)

    # Function for sending explicit commit to solr
    def commit(self):
        self.solrc.commit()

    def get_status(self):
        """Get SolR core status information"""
        tmp = self.solr_url.split("/")
        core = tmp[-1]
        base_url = "/".join(tmp[0:-1])
        logger.debug("Getting status with url %s and core %s", base_url, core)
        res = None
        try:
            res = requests.get(base_url + "/admin/cores?wt=json&action=STATUS&core=" + core, auth=self.authentication)
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
            return status["status"][core]["index"]

    def add_thumbnail(self, url, wms_layers_mmd, thumbnail_type="wms"):
        """Add thumbnail to SolR
        Args:
            type: Thumbnail type. (wms, ts)
        Returns:
            thumbnail: base64 string representation of image
        """
        logger.info("adding thumbnail for: %s" % url)
        if thumbnail_type == "wms":
            try:
                thumbnail = self.thumbClass.create_wms_thumbnail(url, self.id, wms_layers_mmd)
                return thumbnail
            except Exception as e:
                logger.error("Thumbnail creation from OGC WMS failed: %s", e)
                return None
        # time_series
        elif thumbnail_type == "ts":
            # create_ts_thumbnail(...)
            thumbnail = "TMP"
            return thumbnail
        else:
            logger.error(f"Invalid thumbnail type: {thumbnail_type}")
            return None

    def add_thumbnail_api(self, wmsconfig):
        """Create thumnails using the thumbnail-generator-api"""

        getCapUrl = wmsconfig.get("wms_url")
        if getCapUrl is not None:
            logger.debug("Got WMS url: %s. Creating thumbnail using API", getCapUrl)
            wmsconfig.update({"id": self.id})
            logger.debug("Creating wms with config: %s", wmsconfig)
            response = create_wms_thumbnail_api(wmsconfig)
            return response
        else:
            logger.debug("No wms url. Skipping thumbnail generation")
            return None

    def index_record(self, records2ingest, addThumbnail, level=None, thumbClass=None):
        # FIXME, update the text below Øystein Godøy, METNO/FOU, 2023-03-19
        """Add thumbnail to SolR
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
        progress_string = "=>"
        for input_record in records2ingest:
            progress_string = "=" + progress_string
            logger.info(progress_string)
            logger.info("Processing record %d of %d", i, norec)
            i += 1
            # Do some checking of content
            self.id = input_record["id"]
            if input_record["metadata_status"] == "Inactive":
                logger.warning("This record will be set inactive...")
                # return False
            myfeature = None

            """ Handle explicit dataset level parent/children relations"""
            if level == 1:
                input_record.update({"dataset_type": "Level-1"})
            if level == 2:
                input_record.update({"dataset_type": "Level-2"})
                input_record.update({"isChild": True})

            """
            If OGC WMS is available, no point in looking for featureType in OPeNDAP.
            """

            if "data_access_url_ogc_wms" in input_record and addThumbnail:
                logger.info("Checking thumbnails...")
                getCapUrl = input_record["data_access_url_ogc_wms"]
                if isinstance(getCapUrl, list):
                    getCapUrl = getCapUrl[0]

                # logger.debug(type(getCapUrl))
                # logger.debug(getCapUrl)
                thumb_impl = self.config.get("thumbnail_impl", "legacy")
                mmd_layers = None

                if "data_access_wms_layers" in input_record:
                    mmd_layers = input_record["data_access_wms_layers"]
                if mmd_layers is None:
                    mmd_layers = []
                if not myfeature:
                    self.thumbnail_type = "wms"
                if isinstance(thumbClass, dict) and thumb_impl == "fastapi":
                    logger.debug("Creating WMS thumbnail using new API using url %s", getCapUrl)
                    thumbClass.update({"wms_url": getCapUrl})
                    thumbClass.update({"wms_layers_mmd": mmd_layers})

                    response = self.add_thumbnail_api(thumbClass)
                    logger.debug("WMS api response: %s", response)
                    error = response.get("error")
                    status_code = response.get("status_code")
                    if error is None and status_code == 200:
                        thumbnail_url = response.get("data", None).get("thumbnail_url", None)
                        if thumbnail_url is not None:
                            logger.debug("Adding thumbnail_url field with value: %s", thumbnail_url)
                            input_record.update({"thumbnail_url": thumbnail_url})
                        # else:
                        #     logger.warning("Could not properly generate thumbnail")
                        #     # If WMS is not available, remove this data_access element
                        #     # from the XML that is indexed
                        #     del input_record['data_access_url_ogc_wms']
                    else:
                        logger.error("Could not generate thumbnail, reason: %s, status_code %s", error, status_code)
                    # Store task id for later processing
                    task_id = response.get("data", None).get("task_id", None)
                    if task_id is not None:
                        logger.debug("Added task_id: %s to list.", task_id)
                        self.wms_task_list.append(task_id)
                elif self.config.get("scope", "") == "NBS":
                    logger.debug("Calling add_nbs_thumbnail()-function")
                    input_record = add_nbs_thumbnail(input_record, self.config)
                else:
                    logger.debug("Creating WMS thumbnail using legacy method using url: %s", getCapUrl)
                    thumbnail_data = self.add_thumbnail(getCapUrl, mmd_layers)

                    if thumbnail_data is None:
                        logger.warning("Could not properly parse WMS GetCapabilities document")
                        # If WMS is not available, remove this data_access element
                        # from the XML that is indexed
                        del input_record["data_access_url_ogc_wms"]
                    else:
                        input_record.update({"thumbnail_data": thumbnail_data})

            if "data_access_url_opendap" in input_record:
                # Thumbnail of timeseries to be added
                # Or better do this as part of get_feature_type?

                skip_feature_type = self.config.get("skip-feature-type", False)
                if skip_feature_type is True:
                    logger.info("skip-feature-type is True in config. Skipping feature type..")
                else:
                    logger.info("Processing feature type")
                    input_record = process_feature_type(input_record)

            logger.info("Adding records to list...")
            mmd_records.append(input_record)

        """
        Send information to SolR
        """
        logger.info("Adding records to SolR core.")
        res = None
        try:
            res = self.solrc.add(mmd_records)
        except Exception as e:
            msg = "Solr index error: %s" % str(e)
            logger.critical(msg)
            return False, msg
        msg = "Record successfully added."
        logger.info("Record successfully added.")
        logger.debug(res)

        del mmd_records

        return True, msg

    def get_feature_type(self, myopendap):
        """Set feature type from OPeNDAP"""
        logger.info("Now in get_feature_type")

        # Open as OPeNDAP
        try:
            ds = netCDF4.Dataset(myopendap)
        except Exception as e:
            logger.error("Something failed reading dataset: %s", str(e))

        # Try to get the global attribute featureType
        try:
            featureType = ds.getncattr("featureType")
        except AttributeError:
            raise
        except Exception as e:
            logger.error("Something failed extracting featureType: %s", str(e))
            raise
        ds.close()

        if featureType not in [
            "point",
            "timeSeries",
            "trajectory",
            "profile",
            "timeSeriesProfile",
            "trajectoryProfile",
        ]:
            logger.warning("The featureType found - %s - is not valid", featureType)
            logger.warning("Fixing this locally")
            if featureType.lower() == "timeseries" or featureType == "timseries":
                featureType = "timeSeries"
            else:
                logger.warning("The featureType found is a new typo...")
        return featureType

    def create_thumbnail(self, doc):
        """Add thumbnail to SolR
        Args:
            type: solr document
        Returns:
            solr document with thumbnail
        """
        url = str(doc["data_access_url_ogc_wms"]).strip()
        logger.debug("adding thumbnail for: %s", url)
        id = str(doc["id"]).strip()
        try:
            thumbnail_data = self.thumbClass.create_wms_thumbnail(url, id)
            doc.update({"thumbnail_data": thumbnail_data})
            return doc
        except Exception as e:
            logger.error("Thumbnail creation from OGC WMS failed: %s", e)
            return doc

    def delete_level1(self, datasetid):
        """Require ID as input"""
        """ Rewrite to take full metadata record as input """
        logger.info("Deleting %s from level 1.", datasetid)
        try:
            self.solrc.delete(id=datasetid)
        except Exception as e:
            logger.error("Something failed in SolR delete: %s", str(e))
            raise

        logger.info("Record successfully deleted from Level 1 core")

    def delete_thumbnail(self, datasetid):
        """Require ID as input"""
        """ Rewrite to take full metadata record as input """
        logger.info("Deleting %s from thumbnail core.", datasetid)
        try:
            self.solrc.delete(id=datasetid)
        except Exception as e:
            logger.error("Something failed in SolR delete: %s", str(e))
            raise

        logger.info("Records successfully deleted from thumbnail core")

    def search(self):
        """Require Id as input"""
        try:
            results = pysolr.search("mmd_title:Sea Ice Extent", df="text_en", rows=100)
        except Exception as e:
            logger.error("Something failed during search: %s", str(e))

        return results

    def darextract(self, mydar):
        mylinks = {}
        for i in range(len(mydar)):
            if isinstance(mydar[i], bytes):
                mystr = str(mydar[i], "utf-8")
            else:
                mystr = mydar[i]
            if mystr.find("description") != -1:
                t1, t2 = mystr.split(",", 1)
            else:
                t1 = mystr
            t2 = t1.replace('"', "")
            proto, myurl = t2.split(":", 1)
            mylinks[proto] = myurl

        return mylinks

    def delete(self, id, commit=False):
        """Delete document with given metadata identifier"""
        solr_id = to_solr_id(id)
        doc_exsists = self.get_dataset(solr_id)
        if doc_exsists["doc"] is None:
            return False, "Document %s not found in index." % id
        try:
            self.solrc.delete(id=solr_id)
        except Exception as e:
            logger.error("Something went wrong deleting doucument with id: %s", id)
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
            res = requests.get(self.solr_url + "/get?wt=json&id=" + id, auth=self.authentication)
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
        if "full_text" in parent:
            parent.pop("full_text")
        if "bbox__maxX" in parent:
            parent.pop("bbox__maxX")
        if "bbox__maxY" in parent:
            parent.pop("bbox__maxY")
        if "bbox__minX" in parent:
            parent.pop("bbox__minX")
        if "bbox__minY" in parent:
            parent.pop("bbox__minY")
        if "bbox_rpt" in parent:
            parent.pop("bbox_rpt")
        if "ss_access" in parent:
            parent.pop("ss_access")
        if "_version_" in parent:
            parent.pop("_version_")

        parent["isParent"] = True
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
            if myparent["doc"] is None:
                if fail_on_missing is True:
                    return False, "Parent %s is not in the index. Index parent first." % parentid
                else:
                    logger.warn("Parent %s is not in the index. Make sure to index parent first.", parentid)
                    msg = "WARNING! Parent is not in the index. "
                    msg += "Make sure to index parent and then the children "
                    msg += "for relation to be updated."
                    return (handle_missing_status, msg)

            logger.info("Got parent: %s", myparent["doc"]["metadata_identifier"])
            if bool(myparent["doc"]["isParent"]):
                logger.info("Dataset already marked as parent.")
                return True, "Already updated."
            else:
                # doc = {'id': parentid, 'isParent': True}
                doc = self._solr_update_parent_doc(myparent['doc'])
                #doc = myparent["doc"]
                doc["isParent"] = True
                try:
                    #self.solrc.add([doc], fieldUpdates={"isParent": "set"})
                    self.solrc.add([doc])
                except Exception as e:
                    logger.error("Atomic update failed on parent %s. Error is: ", (parentid, e))
                    return False, e
                logger.info("Parent sucessfully updated in SolR.")
                return True, "Parent updated."
