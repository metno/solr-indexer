"""XML helpers built on lxml for direct MMD parsing."""

from lxml import etree as ET


def parse_xml_string(xml_text: str) -> ET._Element:
    """Parse XML text and return the root element."""
    parser = ET.XMLParser(remove_blank_text=False, recover=False)
    return ET.fromstring(xml_text.encode("utf-8"), parser=parser)


def parse_xml_file(file_path: str) -> ET._Element:
    """Parse an XML file and return the root element."""
    parser = ET.XMLParser(remove_blank_text=False, recover=False)
    tree = ET.parse(file_path, parser)
    return tree.getroot()
