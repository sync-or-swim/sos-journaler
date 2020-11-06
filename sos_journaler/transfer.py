import xml.etree.ElementTree as ET
import re
from typing import List

from .transfer_hooks import run_hooks

_NAMESPACE_PATTERN = re.compile(r"{.*}")
"""Matches to XML namespaces as they are formatted by the ElementTree parser"""


def message_to_point(message: ET.Element) -> dict:
    """
    :param message: A single FIXM message from a MessageCollection
    :return: An InfluxDB point
    """
    fields = {}

    def add_to_output(prefix: List[str], value: str) -> None:
        name = ".".join(prefix)

        if name in fields:
            raise RuntimeError(f"Non-unique name {name} among fields {fields}")

        fields[name] = value

    def add_item(item: ET.Element, prefix: List[str]) -> None:
        # Remove the namespace from the tag name
        item.tag = _NAMESPACE_PATTERN.sub("", item.tag)

        # Add the body of the tag
        if item.text is not None and item.text.strip() != "":
            add_to_output(prefix, item.text)

        # Add all the tag's attributes
        for name, value in item.attrib.items():
            name = _NAMESPACE_PATTERN.sub("", name)
            add_to_output(prefix + [name], value)

        children = {}
        # Add a number to any children with duplicate names
        for child in item:
            if child.tag in children:
                duplicate_count = 1
                while f"{child.tag}{duplicate_count}" in children:
                    duplicate_count += 1
                child.tag += str(duplicate_count)

            tag = _NAMESPACE_PATTERN.sub("", child.tag)
            children[tag] = child

        # Recursively add all children of the tag
        for name, child in children.items():
            add_item(child, prefix + [name])

    add_item(message, [])

    point = {
        "measurement": fields["flight.source"],
        "tags": {},
        "time": fields["flight.timestamp"],
        "fields": fields,
    }
    run_hooks(point)

    return point
