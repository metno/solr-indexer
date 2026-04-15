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

import contextlib
import html
import json
import logging
import os
import sys

import lxml.etree as ET
import netCDF4
import pysolr
import requests

from solrindexer.spatial import handle_solr_spatial
from solrindexer.tools import (
    add_nbs_thumbnail,
    get_dataset,
    parse_date,
    process_feature_type,
    set_parent_flag,
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

    def __init__(
        self,
        filename=None,
        mydoc=None,
        bulkFile=None,
        xsd_path=None,
        warning_callback=None,
        vocabulary_loader=None,
    ):
        logger.debug("Creating an instance of MMD4SolR")
        self.filename = filename if filename is not None else bulkFile
        self.xsd_path = xsd_path
        self.warning_callback = warning_callback
        self.vocabulary_loader = vocabulary_loader
        self.root = None

        if filename is not None:
            try:
                self.root = ET.parse(str(filename)).getroot()
            except Exception as exc:
                logger.error("Could not open file %s. Reason: %s", filename, exc)
                if isinstance(exc, OSError):
                    raise FileNotFoundError(str(exc)) from exc
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
        return {"ok": "✅", "warn": "⚠️ ", "fail": "❌"}[kind]

    def _record_warning(self, msg, *args, warning_stage="validation"):
        """Log warning and optionally forward it to a per-document collector."""
        logger.warning(msg, *args)
        if not callable(self.warning_callback):
            return
        try:
            rendered = msg % args if args else msg
        except Exception:
            rendered = str(msg)
        self.warning_callback(rendered, warning_stage)

    def get_metadata_identifier(self):
        """
        Extract the metadata_identifier from the MMD document.

        Used for failure reporting to identify documents even when validation fails.
        Returns None if not found.
        """
        try:
            return self._first_text("./mmd:metadata_identifier")
        except Exception:
            return None

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

    def _validate_xsd(self):
        """Validate the MMD document against the configured XSD schema.

        Does not abort processing: logs a warning listing all validation
        errors when the document does not conform to the schema.
        Returns True when valid (or when no schema is configured).
        """
        if not self.xsd_path:
            return True
        try:
            schema_doc = ET.parse(self.xsd_path)
            schema = ET.XMLSchema(schema_doc)
        except Exception as exc:
            self._record_warning(
                "%s Could not load XSD from %s: %s",
                self._icon("warn"),
                self.xsd_path,
                exc,
            )
            return True  # config error — do not block indexing

        if schema.validate(self.root):
            logger.debug("%s XSD validation passed for %s", self._icon("ok"), self.filename)
            return True

        errors = schema.error_log
        self._record_warning(
            "%s XSD validation failed for %s — %d error(s):",
            self._icon("warn"),
            self.filename,
            len(errors),
        )
        for err in errors:
            self._record_warning("line %d: %s", err.line, err.message)
        return False  # warning-only; caller decides whether to continue

    def check_mmd(self):
        self._validate_xsd()  # warn-only; does not affect required-field result
        try:
            return self._check_mmd_body()
        except Exception as exc:
            logger.error("%s check_mmd failed for %s: %s", self._icon("fail"), self.filename, exc)
            return False

    def _check_mmd_body(self):
        status_ok = True
        logger.debug("Checking mmd file %s", self.filename)
        for tag in self.REQUIRED_ELEMENTS:
            value = self._first_text(f"./mmd:{tag}")
            if value:
                logger.debug("%s check_mmd mmd:%s", self._icon("ok"), tag)
            else:
                status_ok = False
                self._record_warning(
                    "%s check_mmd missing required mmd:%s", self._icon("fail"), tag
                )

        for tag, vocab_url in self.CONTROLLED_ELEMENTS.items():
            values = self._all_text(f"./mmd:{tag}")
            if not values:
                continue
            # Use vocabulary_loader if available, otherwise skip vocabulary validation
            if self.vocabulary_loader is None:
                continue
            for value in values:
                if not self.vocabulary_loader.search(vocab_url, value):
                    self._record_warning(
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
                    [
                        self._text(node)
                        for node in keywords.xpath("./mmd:keyword", namespaces=self.NSMAP)
                    ]
                )
        if not gcmd_values:
            self._record_warning("%s Keywords in GCMD are not available", self._icon("warn"))

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
            key=lambda node: (
                self._normalize_datetime(self._first_text_for(node, "./mmd:datetime")) or ""
            ),
        )
        updates_json = []
        for update in updates_sorted:
            entry = {
                "type": self._first_text_for(update, "./mmd:type"),
                "datetime": self._normalize_datetime(
                    self._first_text_for(update, "./mmd:datetime")
                ),
                "note": self._first_text_for(update, "./mmd:note"),
            }
            # Keep only non-empty values in each history entry.
            updates_json.append({k: v for k, v in entry.items() if v})

        if updates_json:
            solr_doc["last_metadata_update_json"] = json.dumps(
                updates_json,
                ensure_ascii=False,
                separators=(",", ":"),
            )

        earliest = updates_sorted[0]
        earliest_dt = self._normalize_datetime(self._first_text_for(earliest, "./mmd:datetime"))
        if earliest_dt:
            solr_doc["last_metadata_created_date"] = earliest_dt

        latest = updates_sorted[-1]
        latest_dt = self._normalize_datetime(self._first_text_for(latest, "./mmd:datetime"))
        if latest_dt:
            solr_doc["last_metadata_updated_date"] = latest_dt

    def _extract_alternate_identifier(self, solr_doc):
        alt_ids, types = [], []
        for node in self._nodes("./mmd:alternate_identifier"):
            alt_type = node.attrib.get("type") or ""
            value = self._text(node)
            if not value:
                continue
            alt_ids.append(value)
            types.append(alt_type)
        solr_doc["alternate_identifier"] = alt_ids
        solr_doc["alternate_identifier_type"] = types

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
        extent = extents[0]
        rectangle = self._extract_rectangle(extent)
        if rectangle is None:
            return

        rectangle_nodes = extent.xpath("./mmd:rectangle", namespaces=self.NSMAP)
        if rectangle_nodes:
            srs_name = rectangle_nodes[0].attrib.get("srsName") or "EPSG:4326"
            solr_doc["geographic_extent_rectangle_srsName"] = srs_name
            logger.debug("geographic extent srsName: %s", srs_name)

        north, south, east, west = rectangle
        solr_doc["geographic_extent_rectangle_north"] = north
        solr_doc["geographic_extent_rectangle_south"] = south
        solr_doc["geographic_extent_rectangle_east"] = east
        solr_doc["geographic_extent_rectangle_west"] = west
        # Call handle_solr_spatial to add geometry_wkt, geometry_geojson, geospatial_bounds3d
        solr_doc = handle_solr_spatial(solr_doc, north, east, south, west)

    def _extract_keywords(self, solr_doc):
        keyword_all = []
        vocab_all = []
        keyword_targets = {
            "GCMDSK": "keywords_gcmd",
            "GCMDPLT": "keywords_gcmdplt",
            "GCMDINST": "keywords_gcmdinst",
            "GCMDLOC": "keywords_gcmdloc",
            "GCMDPROV": "keywords_gcmdprov",
            "CFSTDN": "keywords_cfstdn",
            "GEMET": "keywords_gemet",
            "NORTHEMES": "keywords_northemes",
            "EXV": "keywords_exv",
        }

        for keywords in self._nodes("./mmd:keywords"):
            vocab = (keywords.attrib.get("vocabulary") or "none").upper()
            items = [
                self._text(node) for node in keywords.xpath("./mmd:keyword", namespaces=self.NSMAP)
            ]
            items = [item for item in items if item]
            if not items:
                continue
            keyword_all.extend(items)
            vocab_all.extend([vocab] * len(items))
            target_field = keyword_targets.get(vocab, "keywords_none")
            solr_doc.setdefault(target_field, []).extend(items)

        if keyword_all:
            solr_doc["keywords_keyword"] = keyword_all
            solr_doc["keywords_vocabulary"] = sorted(set(vocab_all))

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
            personnel_type = self._first_text_for(node, "./mmd:type")
            name = self._first_text_for(node, "./mmd:name")
            organisation = self._first_text_for(node, "./mmd:organisation")
            email = self._first_text_for(node, "./mmd:email")
            phone = self._first_text_for(node, "./mmd:phone")
            name_nodes = node.xpath("./mmd:name", namespaces=self.NSMAP)
            organisation_nodes = node.xpath("./mmd:organisation", namespaces=self.NSMAP)
            orcid_uri = name_nodes[0].attrib.get("uri") if name_nodes else None
            ror_uri = organisation_nodes[0].attrib.get("uri") if organisation_nodes else None

            contact_address = {
                "address": self._first_text_for(node, "./mmd:contact_address/mmd:address"),
                "city": self._first_text_for(node, "./mmd:contact_address/mmd:city"),
                "province_or_state": self._first_text_for(
                    node,
                    "./mmd:contact_address/mmd:province_or_state",
                ),
                "postal_code": self._first_text_for(node, "./mmd:contact_address/mmd:postal_code"),
                "country": self._first_text_for(node, "./mmd:contact_address/mmd:country"),
            }
            contact_address = {k: v for k, v in contact_address.items() if v}

            if role:
                roles.append(role)
            if name:
                names.append(name)
            if organisation:
                orgs.append(organisation)

            personnel_entry = {
                "role": role,
                "name": name,
                "organisation": organisation,
                "email": email,
                "phone": phone,
            }
            if organisation:
                personnel_entry["organisation"] = organisation
            if email:
                personnel_entry["email"] = email
            if phone:
                personnel_entry["phone"] = phone

            if personnel_type:
                personnel_entry["type"] = personnel_type
            if orcid_uri:
                personnel_entry["orcid_uri"] = orcid_uri
            if ror_uri:
                personnel_entry["ror_uri"] = ror_uri
            if contact_address:
                personnel_entry["contact_address"] = contact_address

            personnel_entry = {k: v for k, v in personnel_entry.items() if v}
            if personnel_entry:
                personnel_json.append(personnel_entry)

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
            solr_doc["personnel_json"] = json.dumps(
                personnel_json, ensure_ascii=False, separators=(",", ":")
            )

    def _extract_data_access(self, solr_doc):
        urls_by_field = {
            "data_access_url_opendap": [],
            "data_access_url_ogc_wms": [],
            "data_access_url_ogc_wfs": [],
            "data_access_url_ogc_wcs": [],
            "data_access_url_http": [],
            "data_access_url_odata": [],
            "data_access_url_ftp": [],
        }

        type_to_field = {
            "HTTP": "data_access_url_http",
            "OPENDAP": "data_access_url_opendap",
            "OGC WMS": "data_access_url_ogc_wms",
            "OGC WFS": "data_access_url_ogc_wfs",
            "OGC WCS": "data_access_url_ogc_wcs",
            "FTP": "data_access_url_ftp",
            "ODATA": "data_access_url_odata",
        }
        data_access_json = []

        for node in self._nodes("./mmd:data_access"):
            access_type_raw = self._first_text_for(node, "./mmd:type")
            description = self._first_text_for(node, "./mmd:description")
            resource = self._first_text_for(node, "./mmd:resource")
            wms_layers = [
                self._text(layer)
                for layer in node.xpath("./mmd:wms_layers/mmd:wms_layer", namespaces=self.NSMAP)
                if self._text(layer)
            ]

            data_access_entry = {
                "type": access_type_raw,
                "description": description,
                "resource": resource,
            }
            if wms_layers:
                data_access_entry["wms_layers"] = wms_layers

            data_access_json.append(data_access_entry)

            if not resource:
                continue

            access_type = (access_type_raw or "").strip().upper()
            target_field = type_to_field.get(access_type)
            if target_field is None:
                continue

            urls_by_field[target_field].append(resource)

        for field_name, values in urls_by_field.items():
            if values:
                solr_doc[field_name] = values

        if data_access_json:
            solr_doc["data_access_json"] = json.dumps(
                data_access_json,
                ensure_ascii=False,
                separators=(",", ":"),
            )

    def _extract_related_information(self, solr_doc):
        """Extract mmd:related_information elements and map to Solr fields.

        Creates:
        - related_information_json: complete JSON with type, description, resource
        - related_information_type: list of all types
        - related_information_resource: list of all resources
        - related_information_description: list of all descriptions
        - related_url_*: type-specific URLs
        - related_url_*_desc: type-specific descriptions
        """
        urls_by_field = {
            "related_url_landing_page": [],
            "related_url_user_guide": [],
            "related_url_home_page": [],
            "related_url_obs_facility": [],
            "related_url_ext_metadata": [],
            "related_url_scientific_publication": [],
            "related_url_data_paper": [],
            "related_url_data_management_plan": [],
            "related_url_other_documentation": [],
            "related_url_software": [],
            "related_url_data_server_landing_page": [],
        }

        descs_by_field = {
            "related_url_landing_page_desc": [],
            "related_url_user_guide_desc": [],
            "related_url_home_page_desc": [],
            "related_url_obs_facility_desc": [],
            "related_url_ext_metadata_desc": [],
            "related_url_scientific_publication_desc": [],
            "related_url_data_paper_desc": [],
            "related_url_data_management_plan_desc": [],
            "related_url_other_documentation_desc": [],
            "related_url_software_desc": [],
            "related_url_data_server_landing_page_desc": [],
        }

        type_to_field = {
            "Dataset landing page": "related_url_landing_page",
            "Users guide": "related_url_user_guide",
            "Project home page": "related_url_home_page",
            "Observation facility": "related_url_obs_facility",
            "Extended metadata": "related_url_ext_metadata",
            "Scientific publication": "related_url_scientific_publication",
            "Data paper": "related_url_data_paper",
            "Data management plan": "related_url_data_management_plan",
            "Other documentation": "related_url_other_documentation",
            "Software": "related_url_software",
            "Data server landing page": "related_url_data_server_landing_page",
        }

        related_information_json = []
        all_types = []
        all_resources = []
        all_descriptions = []

        for node in self._nodes("./mmd:related_information"):
            info_type = self._first_text_for(node, "./mmd:type")
            description = self._first_text_for(node, "./mmd:description")
            resource = self._first_text_for(node, "./mmd:resource")

            # Add to general lists
            if info_type:
                all_types.append(info_type)
            if resource:
                all_resources.append(resource)
            if description:
                all_descriptions.append(description)

            # Create JSON entry
            related_info_entry = {
                "type": info_type,
                "description": description,
                "resource": resource,
            }
            related_information_json.append(related_info_entry)

            # Map to type-specific fields if resource exists
            if resource:
                target_field = type_to_field.get(info_type)
                if target_field:
                    urls_by_field[target_field].append(resource)
                    if description:
                        descs_by_field[target_field + "_desc"].append(description)

        # Add type-specific URL fields
        for field_name, values in urls_by_field.items():
            if values:
                solr_doc[field_name] = values

        # Add type-specific description fields
        for field_name, values in descs_by_field.items():
            if values:
                solr_doc[field_name] = values

        # Add general arrays
        if all_types:
            solr_doc["related_information_type"] = all_types
        if all_resources:
            solr_doc["related_information_resource"] = all_resources
        if all_descriptions:
            solr_doc["related_information_description"] = all_descriptions

        # Add JSON representation
        if related_information_json:
            solr_doc["related_information_json"] = json.dumps(
                related_information_json,
                ensure_ascii=False,
                separators=(",", ":"),
            )

    def _extract_projects(self, solr_doc):
        short_names, long_names, project_names = [], [], []
        for node in self._nodes("./mmd:project"):
            short = self._first_text_for(node, "./mmd:short_name")
            long = self._first_text_for(node, "./mmd:long_name")

            if short:
                short_names.append(short)
            if long:
                long_names.append(long)
            if short or long:
                project_names.append(short or long)
        if project_names:
            solr_doc["project_short_name"] = short_names
            solr_doc["project_long_name"] = long_names
            solr_doc["project_name"] = project_names

    def _extract_data_center(self, solr_doc):
        short_names, long_names, names, urls = [], [], [], []

        for node in self._nodes("./mmd:data_center"):
            short = self._first_text_for(node, "./mmd:data_center_name/mmd:short_name")
            long = self._first_text_for(node, "./mmd:data_center_name/mmd:long_name")
            url = self._first_text_for(node, "./mmd:data_center_url")

            if short:
                short_names.append(short)
            if long:
                long_names.append(long)
            if short or long:
                names.append(short or long)
            if url:
                urls.append(url)

        if short_names:
            solr_doc["data_center_short_name"] = short_names
        if long_names:
            solr_doc["data_center_long_name"] = long_names
        if names:
            solr_doc["data_center_name"] = names
        if urls:
            solr_doc["data_center_url"] = urls

    def _extract_storage_information(self, solr_doc):
        nodes = self._nodes("./mmd:storage_information")
        if not nodes:
            return

        node = nodes[0]
        file_name = self._first_text_for(node, "./mmd:file_name")
        file_location = self._first_text_for(node, "./mmd:file_location")
        file_format = self._first_text_for(node, "./mmd:file_format")
        file_size_nodes = node.xpath("./mmd:file_size", namespaces=self.NSMAP)
        checksum_nodes = node.xpath("./mmd:checksum", namespaces=self.NSMAP)
        storage_expiry_date = self._normalize_datetime(
            self._first_text_for(node, "./mmd:storage_expiry_date")
        )

        if file_name:
            solr_doc["storage_information_file_name"] = file_name
        if file_location:
            solr_doc["storage_information_file_location"] = file_location
        if file_format:
            solr_doc["storage_information_file_format"] = file_format

        if file_size_nodes:
            file_size_node = file_size_nodes[0]
            file_size = self._text(file_size_node)
            file_size_unit = file_size_node.attrib.get("unit")
            if file_size:
                solr_doc["storage_information_file_size"] = file_size
            if file_size_unit:
                solr_doc["storage_information_file_size_unit"] = file_size_unit

        if checksum_nodes:
            checksum_node = checksum_nodes[0]
            checksum = self._text(checksum_node)
            checksum_type = checksum_node.attrib.get("type")
            if checksum:
                solr_doc["storage_information_file_checksum"] = checksum
            if checksum_type:
                solr_doc["storage_information_file_checksum_type"] = checksum_type

        if storage_expiry_date:
            solr_doc["storage_information_file_storage_expiry_date"] = storage_expiry_date

    def _extract_platform(self, solr_doc):
        acc = {
            "platform_short_name": [],
            "platform_long_name": [],
            "platform_name": [],
            "platform_resource": [],
            "platform_orbit_relative": [],
            "platform_orbit_absolute": [],
            "platform_orbit_direction": [],
            "platform_instrument_short_name": [],
            "platform_instrument_long_name": [],
            "platform_instrument_name": [],
            "platform_instrument_resource": [],
            "platform_instrument_mode": [],
            "platform_instrument_polarisation": [],
            "platform_instrument_product_type": [],
            "platform_ancillary_cloud_coverage": [],
            "platform_ancillary_scene_coverage": [],
            "platform_ancillary_timeliness": [],
        }
        platform_json = []

        for node in self._nodes("./mmd:platform"):
            short = self._first_text_for(node, "./mmd:short_name")
            long = self._first_text_for(node, "./mmd:long_name")
            resource = self._first_text_for(node, "./mmd:resource")
            orbit_rel_raw = self._first_text_for(node, "./mmd:orbit_relative")
            orbit_abs_raw = self._first_text_for(node, "./mmd:orbit_absolute")
            orbit_dir = self._first_text_for(node, "./mmd:orbit_direction")

            if short:
                acc["platform_short_name"].append(short)
            if long:
                acc["platform_long_name"].append(long)
            if short or long:
                acc["platform_name"].append(short or long)
            if resource:
                acc["platform_resource"].append(resource)
            if orbit_rel_raw:
                with contextlib.suppress(ValueError):
                    acc["platform_orbit_relative"].append(int(orbit_rel_raw))
            if orbit_abs_raw:
                with contextlib.suppress(ValueError):
                    acc["platform_orbit_absolute"].append(int(orbit_abs_raw))
            if orbit_dir:
                acc["platform_orbit_direction"].append(orbit_dir)

            platform_entry = {}
            if short:
                platform_entry["short_name"] = short
            if long:
                platform_entry["long_name"] = long
            if resource:
                platform_entry["resource"] = resource
            if orbit_dir:
                platform_entry["orbit_direction"] = orbit_dir
            if orbit_rel_raw:
                platform_entry["orbit_relative"] = orbit_rel_raw
            if orbit_abs_raw:
                platform_entry["orbit_absolute"] = orbit_abs_raw

            instrument_nodes = node.xpath("./mmd:instrument", namespaces=self.NSMAP)
            if instrument_nodes:
                inst = instrument_nodes[0]
                inst_short = self._first_text_for(inst, "./mmd:short_name")
                inst_long = self._first_text_for(inst, "./mmd:long_name")
                inst_resource = self._first_text_for(inst, "./mmd:resource")
                inst_mode = self._first_text_for(inst, "./mmd:mode")
                inst_pol = self._first_text_for(inst, "./mmd:polarisation")
                inst_prod = self._first_text_for(inst, "./mmd:product_type")

                if inst_short:
                    acc["platform_instrument_short_name"].append(inst_short)
                if inst_long:
                    acc["platform_instrument_long_name"].append(inst_long)
                if inst_short or inst_long:
                    acc["platform_instrument_name"].append(inst_short or inst_long)
                if inst_resource:
                    acc["platform_instrument_resource"].append(inst_resource)
                if inst_mode:
                    acc["platform_instrument_mode"].append(inst_mode)
                if inst_pol:
                    acc["platform_instrument_polarisation"].append(inst_pol)
                if inst_prod:
                    acc["platform_instrument_product_type"].append(inst_prod)

                inst_entry = {
                    k: v
                    for k, v in {
                        "short_name": inst_short,
                        "long_name": inst_long,
                        "resource": inst_resource,
                        "mode": inst_mode,
                        "polarisation": inst_pol,
                        "product_type": inst_prod,
                    }.items()
                    if v
                }
                if inst_entry:
                    platform_entry["instrument"] = inst_entry

            ancillary_nodes = node.xpath("./mmd:ancillary", namespaces=self.NSMAP)
            if ancillary_nodes:
                anc = ancillary_nodes[0]
                cloud_raw = self._first_text_for(anc, "./mmd:cloud_coverage")
                scene_raw = self._first_text_for(anc, "./mmd:scene_coverage")
                timeliness = self._first_text_for(anc, "./mmd:timeliness")

                if cloud_raw:
                    with contextlib.suppress(ValueError):
                        acc["platform_ancillary_cloud_coverage"].append(float(cloud_raw))
                if scene_raw:
                    with contextlib.suppress(ValueError):
                        acc["platform_ancillary_scene_coverage"].append(float(scene_raw))
                if timeliness:
                    acc["platform_ancillary_timeliness"].append(timeliness)

                anc_entry = {
                    k: v
                    for k, v in {
                        "cloud_coverage": cloud_raw,
                        "scene_coverage": scene_raw,
                        "timeliness": timeliness,
                    }.items()
                    if v
                }
                if anc_entry:
                    platform_entry["ancillary"] = anc_entry

            platform_json.append(platform_entry)

        for field, values in acc.items():
            if values:
                solr_doc[field] = values

        if platform_json:
            solr_doc["platform_json"] = json.dumps(
                platform_json, ensure_ascii=False, separators=(",", ":")
            )

    def _serialize_mmd_xml(self):
        """Serialize MMD XML for storage after removing sensitive file locations."""
        root_copy = ET.fromstring(ET.tostring(self.root))
        file_location_nodes = root_copy.xpath(
            "./mmd:storage_information/mmd:file_location",
            namespaces=self.NSMAP,
        )
        for node in file_location_nodes:
            parent = node.getparent()
            if parent is not None:
                parent.remove(node)
        return html.unescape(ET.tostring(root_copy, pretty_print=True, encoding="unicode"))

    def tosolr(self):
        solr_doc = {}

        metadata_identifier = self._first_text("./mmd:metadata_identifier")
        if metadata_identifier:
            solr_doc["id"] = to_solr_id(metadata_identifier)
            solr_doc["metadata_identifier"] = metadata_identifier

        self._extract_alternate_identifier(solr_doc)

        solr_doc["metadata_status"] = self._first_text("./mmd:metadata_status") or "Unknown"
        solr_doc["dataset_production_status"] = (
            self._first_text("./mmd:dataset_production_status") or "Unknown"
        )

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
            solr_doc["use_constraint_identifier"] = (
                self._first_text_for(node, "./mmd:identifier") or "Not provided"
            )
            solr_doc["use_constraint_resource"] = (
                self._first_text_for(node, "./mmd:resource") or "Not provided"
            )
            license_text = self._first_text_for(node, "./mmd:license_text")
            if license_text:
                solr_doc["use_constraint_license_text"] = license_text
        spatial_rep = self._first_text("./mmd:spatial_representation")
        if spatial_rep:
            solr_doc["spatial_representation"] = spatial_rep

        self._extract_last_metadata_update(solr_doc)
        self._extract_temporal_extent(solr_doc)
        self._extract_geographic_extent(solr_doc)
        self._extract_keywords(solr_doc)
        self._extract_related_dataset(solr_doc)
        self._extract_personnel(solr_doc)
        self._extract_data_access(solr_doc)
        self._extract_related_information(solr_doc)
        self._extract_projects(solr_doc)
        self._extract_data_center(solr_doc)
        self._extract_storage_information(solr_doc)
        self._extract_platform(solr_doc)

        iso_topic_category = self._all_text("./mmd:iso_topic_category")
        if iso_topic_category:
            solr_doc["iso_topic_category"] = iso_topic_category

        activity_type = self._all_text("./mmd:activity_type")
        if activity_type:
            solr_doc["activity_type"] = activity_type

        quality_control = self._first_text("./mmd:quality_control")
        if quality_control:
            solr_doc["quality_control"] = quality_control

        metadata_source = self._first_text("./mmd:metadata_source")
        if metadata_source:
            solr_doc["metadata_source"] = metadata_source

        solr_doc["mmd_xml_file"] = self._serialize_mmd_xml()

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
        logger.info("Always commit is: %s", always_commit)

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
            raise
        try:
            pong = self.solrc.ping()
            status = json.loads(pong)["status"]
            if status == "OK":
                logger.info("Solr ping with status %s", status)
            else:
                logger.error("Error! Solr ping with status %s", status)
                sys.exit(1)

        except pysolr.SolrError as e:
            logger.error("Could not contact solr server: %s", e)
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
            res = requests.get(
                base_url + "/admin/cores?wt=json&action=STATUS&core=" + core,
                auth=self.authentication,
                timeout=20,
            )
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
        status = res.json()
        return status["status"][core]["index"]

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

        # Handle thumbnail generator.
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

            # Handle explicit dataset level parent/children relations.
            if level == 1:
                input_record.update({"dataset_type": "Level-1"})
            if level == 2:
                input_record.update({"dataset_type": "Level-2"})
                input_record.update({"isChild": True})

            # If OGC WMS is available, no point in looking for featureType in OPeNDAP.

            if "data_access_url_ogc_wms" in input_record and addThumbnail:
                if self.config.get("scope", "") == "NBS":
                    logger.debug("Calling add_nbs_thumbnail()-function")
                    input_record = add_nbs_thumbnail(input_record, self.config)
                else:
                    logger.warning(
                        "Skipping thumbnail generation for record %s: only NBS scope is supported",
                        input_record.get("id", "unknown"),
                    )

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

        # Send information to SolR.
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

    def delete_level1(self, datasetid):
        """Require ID as input"""
        logger.info("Deleting %s from level 1.", datasetid)
        try:
            self.solrc.delete(id=datasetid)
        except Exception as e:
            logger.error("Something failed in SolR delete: %s", str(e))
            raise

        logger.info("Record successfully deleted from Level 1 core")

    def delete_thumbnail(self, datasetid):
        """Require ID as input"""
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

    def delete(self, metadata_id, commit=False):
        """Delete document with given metadata identifier"""
        solr_id = to_solr_id(metadata_id)
        doc_exsists = self.get_dataset(solr_id)
        if doc_exsists["doc"] is None:
            return False, "Document %s not found in index." % metadata_id
        try:
            self.solrc.delete(id=solr_id)
        except Exception as e:
            logger.error("Something went wrong deleting doucument with id: %s", metadata_id)
            return False, e
        logger.info("Sucessfully deleted document with id: %s", metadata_id)
        if commit:
            logger.info("Commiting deletion")
            self.commit()
        return True, "Document %s sucessfully deleted" % metadata_id

    def get_dataset(self, metadata_id):
        """
        Use real-time get to fetch latest dataset
        based on id.
        """
        return get_dataset(metadata_id, solr_client=self.solrc)

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
        if myparent["doc"] is None:
            if fail_on_missing is True:
                return False, "Parent %s is not in the index. Index parent first." % parentid
            logger.warning(
                "Parent %s is not in the index. Make sure to index parent first.", parentid
            )
            msg = "WARNING! Parent is not in the index. "
            msg += "Make sure to index parent and then the children "
            msg += "for relation to be updated."
            return (handle_missing_status, msg)

        logger.info("Got parent: %s", myparent["doc"]["metadata_identifier"])
        if bool(myparent["doc"]["isParent"]):
            logger.info("Dataset already marked as parent.")
            return True, "Already updated."
        try:
            set_parent_flag(parentid, solr_client=self.solrc)
        except Exception as e:
            logger.error("Atomic update failed on parent %s. Error is: %s", parentid, e)
            return False, e
        logger.info("Parent sucessfully updated in SolR.")
        return True, "Parent updated."
