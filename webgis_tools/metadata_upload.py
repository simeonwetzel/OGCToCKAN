import requests
import uuid
from helpers import *
from templates import extras
import logging
import yaml
import pandas
from pandas import ExcelFile
from gs_config import GeoserverConfig

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())
config = yaml.safe_load(open('config.yml'))

# Init geoserver class
gs = GeoserverConfig()

# declare workspace
workspace = config['gs_config']['workspace']
store_name = config['gs_config']['store_name']

# get list of data_stores of current workspace
data_stores = gs.get_list_of_datastores_of_a_workspace(workspace)


# declare external metadata template
# external_metadata_resource = create_metadata_dict_from_xls_file(config['external_metadata_resources']['excel'])


def create_metadata_dict_from_xls_file(xls_path):
    xls = ExcelFile(xls_path)
    df = xls.parse(xls.sheet_names[0])
    xls.close()

    # Remove first three data rows (example dummies)
    df = df[3:]
    df = df.where(pandas.notnull(df), None)
    # Creating dict like follows:
    # dict like {column -> {index -> value}}
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_dict.html
    metadata_dict = df.to_dict()
    return metadata_dict


def create_metadata_dict_for_datastore(datastore_name, external_metadata_dict):
    meta_dict = {}
    if datastore_name in external_metadata_dict['dataset_name'].values():
        datasets = external_metadata_dict['dataset_name']
        idx_in_dict = list(datasets.keys())[list(datasets.values()).index(datastore_name)]
        for key in external_metadata_dict.keys():
            meta_dict[key] = external_metadata_dict[key][idx_in_dict]
    else:
        log.info("No external Metadata found for dataset: {}".format(datastore_name))
    return meta_dict


def generate_uuid():
    return str(uuid.uuid1())


def update_metadata_for_datastore(workspace, store_name):
    # Collect metadata from template
    metadata_dict_from_template = create_metadata_dict_from_xls_file(config['external_metadata_resources']['excel'])

    meta_dict_for_datastore = create_metadata_dict_for_datastore(datastore_name=store_name,
                                                                 external_metadata_dict=metadata_dict_from_template)
    if meta_dict_for_datastore:
        """
        metadata schema of template:
        
        {'dataset_name': None, 
        'title': None, 
        'identifier': None, <= extras['data-identifier']
        'should_be_uploaded': 'y', 
        'name': None, 
        'alternate_ckan_org': None, 
        'notes': None, <= basic['abstract]
        'path_to_file': None, 
        'documentation': None, 
        'contact_name': None,   <= extras['metadata-contact/contactinfo/persname']
        'contact_uri': None,  <= extras['metadata-contact/contactinfo/website']
        'url': None, 
        'licence-ID': None, 
        'conforms_to': 'http://www.opengis.net/def/crs/EPSG/0/*', 
        'spatial_resolution': None, 
        'spatial_resolution_type': None, 
        'temporal_start': None, <= extras['data-valid-date/start-date']
        'temporal_end': None,   <= extras['data-valid-date/end-date']
        'temporal_resolution': None, 
        'tag_string': None, 
        'theme': 'Hydrography', 
        'inspire_theme_url': 'http://inspire.ec.europa.eu/theme/*', <= extras['keyword-inspire-theme-ref']
        'is_version_of': None, 
        'related_resource': None, 
        'derived_from': None, 
        'owner_org': '*', <= extras[''metadata-contact/organisation']
        ' ': "[
            {'url': 'https://*/geoserver/*/wms?request=GetCapabilities', 
                'name': None, 'format': 'wms', 'licence': None, 'description': None}, 
            {'url': 'https://*/geoserver/*/wfs?request=GetCapabilities', 
                'name': None, 'format': 'wfs', 'licence': None, 'description': None}]"}
        """
        # Remove fields where data providers did not fill in metadata
        external_metadata = filtered_dict(meta_dict_for_datastore)
        log.debug(external_metadata)

        # Create url for specific datastore
        url = gs.create_url_for_datastore(workspace=workspace, store_name=store_name)
        log.debug(url)

        # Request GML from datastore
        xml_tmpl = gs.get_metadata_from_datastore(url)

        # Create metadata section if not exists
        data = create_metadata_section(xml_tmpl)

        # Add custom parameters to the metadata-dict template
        uuid = generate_uuid()

        # Match basic metadata fields
        data = add_basic_metadata(data, 'name', store_name)

        # if 'notes' in external_metadata:
        #    # todo: find better way to handle missing fields
        #    data = add_basic_metadata(data, 'abstract', meta_dict_for_datastore['notes'])

        # Match extra metadata fields
        extras['data-identifier'] = uuid
        # extras['keyword-free'] = ['Teileinzuggebiet', 'KlimaKonform']
        # extras['metadata-contact/contactinfo/orcid'] = '0000-0001-7144-3376'
        extras['metadata-contact/organisation'] = meta_dict_for_datastore['contact_uri']
        extras['metadata-contact/contactinfo/persname'] = meta_dict_for_datastore['contact_name']
        extras['keyword-inspire-theme-ref'] = meta_dict_for_datastore['inspire_theme_url']

        ## Cleanse the dictionary => only keep modified/added params inside and remove rest
        ## Todo: Attention.. this removes all exisiting metadata values
        clean_dict = filtered_dict(extras)

        data = update_extra_metadata_attributes(data, clean_dict)

        # Update metadata via put request
        gs.upload_metadata_for_datastore(url, data)
    else:
        return

for lyr in data_stores:
    update_metadata_for_datastore(workspace=workspace,
                                  store_name=lyr)

"""
# Create url for specific datastore
url = gs.create_url_for_datastore(workspace=workspace, store_name=store_name)
log.debug(url)

# Request GML from datastore
xml_tmpl = gs.get_metadata_from_datastore(url)

# Add custom parameters to the metadata-dict template
uuid = generate_uuid()
extras['data-identifier'] = uuid
extras['keyword-free'] = ['Teileinzuggebiet', 'KlimaKonform']
extras['metadata-contact/organisation'] = 'TU Dresden'
extras['metadata-contact/contactinfo/persname'] = 'Simeon Wetzel'
extras['metadata-contact/contactinfo/orcid'] = '0000-0001-7144-3376'

# Cleanse the dictionary => only keep modified/added params inside and remove rest
# Todo: Attention.. this removes all exisiting metadata values
clean_dict = filtered_dict(extras)

# Create metadata section if not exists
data = create_metadata_section(xml_tmpl)

# Update xml with custom set metadata fields
data = update_extra_metadata_attributes(data, clean_dict)
data = add_basic_metadata(data, 'name', store_name)
# log.debug('xml to pass:')
# log.debug(data.decode())

# Update metadata via put request
gs.upload_metadata_for_datastore(url, data)

"""
