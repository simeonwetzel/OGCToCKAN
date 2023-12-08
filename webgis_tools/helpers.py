import xml.etree.ElementTree as ET
import logging

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


def create_metadata_tag(datatype, text):
    return '<{0}}>{1}</{0}>'.format(datatype, text)


def filtered_dict(original):
    return {k: v for k, v in original.items() if v is not None}


def add_basic_metadata(xml_template, property_name, value):
    root = ET.fromstring(xml_template)
    attr = root.find(property_name)
    attr.text = value
    return ET.tostring(root, encoding='utf-8', method='xml')


def add_extra_metadata(xml_template, attr_datatype, attr, val):
    root = ET.fromstring(xml_template)
    metadata = root.find('metadata')
    map_tag = metadata[0].find('map')
    # Get all currently existing metadata attributes
    existing_entries = [e[0].text for e in map_tag]

    # log.debug("attr = {}".format(attr))
    # log.debug("existing attr {}".format(existing_entries))

    if not attr in existing_entries:  # Insert
        map_tag.append(ET.Element('entry'))
        recent_entry = map_tag[-1]
        new_entry_attr = ET.Element('string')
        new_entry_attr.text = attr
        recent_entry.append(new_entry_attr)

        if isinstance(val, list):
            list_element = ET.Element('list')
            recent_entry.append(list_element)
            for item in val:
                new_entry_val = ET.Element(attr_datatype)
                new_entry_val.text = item
                log.debug("Appending entry: {}".format(new_entry_val.text))
                list_element.append(new_entry_val)
        else:
            new_entry_val = ET.Element(attr_datatype)
            new_entry_val.text = val
            # Append new entry
            recent_entry.append(new_entry_val)

    else:  # Update existing attribute
        for entry in map_tag:
            if entry[0].text == attr:
                "update"
                if isinstance(val, list):
                    previous_list_elements = list(entry[1].iter())
                    previous_list_values = [element.text for element in previous_list_elements]
                    for item in val:
                        if item in previous_list_values:
                            log.debug(
                                "\t ---Entry '{0}' for attribute '{1}' already exists".format(item, entry[0].text))
                        else:
                            new_entry = ET.Element(attr_datatype)
                            new_entry.text = item
                            # TODO:
                            # Wenn hier upgedated wird, ist das <list> tag vorhanden... muss aber erstellt werden beim ersten mal
                            # <entry>
                            #   <string>keyword-free</string>
                            #   <list>
                            #       <string>Teileinzuggebiet</string>
                            #       <string>Teileinzuggebiet2</string>
                            #   </list>
                            # </entry>
                            log.debug("Appending entry: {}".format(new_entry.text))
                            entry[1].append(new_entry)

                        # remove <null> tag
                    if entry[1].find('null') is not None:
                        entry[1].remove(entry[1].find('null'))
                else:
                    if entry.find('null') is not None:
                        # remove <null> tag
                        entry.remove(entry.find('null'))

                        new_entry = ET.Element(attr_datatype)
                        new_entry.text = val
                        entry.append(new_entry)
                    else:
                        entry[1].text = val

    return ET.tostring(root, encoding='utf-8', method='xml')


def update_extra_metadata_attributes(xml_template, clean_dict):
    # iterate through dict and add values
    template_instance = xml_template
    for key, value in clean_dict.items():
        log.debug('Updating following attribute // {0} : {1}'.format(key, value))
        template_instance = add_extra_metadata(template_instance, 'string', key, value)

    return template_instance.decode()
    # return clean_xml_template(template_instance, clean_dict).decode()


def create_metadata_section(xml_template):
    featureType = ET.fromstring(xml_template)
    existing_elements = [e.tag for e in featureType.iter()]
    log.debug("Check if metadata section exists in schema...")
    if 'metadata' not in existing_elements:
        log.debug("Metadata section does not yet exists... creating it now")
        metadata = ET.Element('metadata')
        featureType.append(metadata)
        entry = ET.Element('entry', {'key': 'custom'})
        metadata.append(entry)
        map = ET.Element('map')
        entry.append(map)
    else:
        log.debug("Metadata section already exists... skipping this step")
    return ET.tostring(featureType, encoding='utf-8', method='xml')
