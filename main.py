import json
import logging
import urllib

import geojson
import yaml
from dateutil import parser
from owslib.wfs import WebFeatureService
from owslib.wms import WebMapService
from pyproj import CRS, Transformer

config = yaml.safe_load(open('config.yml'))

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


def crs_transformation(coords, epsg_old, epsg_new):
    old_crs = CRS.from_epsg(epsg_old)
    new_crs = CRS.from_epsg(epsg_new)
    transformer = Transformer.from_crs(old_crs, new_crs)
    transformed_coords = transformer.transform(coords[1], coords[0])

    return transformed_coords[0], transformed_coords[1]


def create_dataset_dicts_from_wms(wms):
    # log.debug(wms.identification.type)
    # log.debug(wms.identification.title)
    # log.debug(wms.identification.abstract)

    wms_contents = list(wms.contents)
    log.debug(wms_contents)

    dataset_dicts = []

    for layer in wms_contents:
        cur_wms = wms[layer]
        gs_datastore = cur_wms.id.split(':')[0]
        resource_url = config['geoserver']['url'] + gs_datastore + '/wms?request=GetCapabilities'
        title = cur_wms.title
        abstract = cur_wms.abstract
        bbox = cur_wms.boundingBox
        crs = cur_wms.crs_list[1][4].split(':')[1]
        log.debug("crs = " + crs)
        '''BBOX format ymin, xmin, ymax, xmax'''
        bbox_ymin = bbox[0]
        bbox_ymax = bbox[2]
        bbox_xmin = bbox[1]
        bbox_xmax = bbox[3]

        spatial = geojson.MultiPolygon([[[
            crs_transformation([bbox_ymin, bbox_xmin], crs, 4326),
            crs_transformation([bbox_ymin, bbox_xmax], crs, 4326),
            crs_transformation([bbox_ymax, bbox_xmax], crs, 4326),
            crs_transformation([bbox_ymax, bbox_xmin], crs, 4326),
            crs_transformation([bbox_ymin, bbox_xmin], crs, 4326),
        ]]])

        keywords = cur_wms.keywords
        timepositions = cur_wms.timepositions

        conforms_to = 'http://www.opengis.net/def/crs/EPSG/0/{}'.format(crs)
        scale_hint = cur_wms.scaleHint

        '''Handle temporal resolution'''
        if timepositions is not None:
            timepositions_prep = timepositions[0].split('/')
            temporal_start = parser.parse(timepositions_prep[0])
            temporal_end = parser.parse(timepositions_prep[1])
        else:
            temporal_start = None
            temporal_end = None

        organization = wms.provider.contact.organization
        contact_name = wms.provider.contact.name
        contact_email = wms.provider.contact.email
        # dataUrls = cur_wms.dataUrls
        # metadataUrl = cur_wms.metadataUrls

        log.debug('Layer details: \n'
                  'title: {0}, \n'
                  'abstract: {1}, \n'
                  'bbox:  {2}, \n'
                  'keywords: {3}, \n'
                  'timepositions: {4}, \n'
                  'temporal_start: {5} \n'
                  'temporal_start: {6} \n'
                  'organization: {7}, \n'
                  'contact_name: {8} \n'
                  'contact_email: {9}, \n'
                  'resource_url: {10} \n'
                  'crs: {11} \n'
                  'conforms_to {12} \n'
                  'scale_hint {13} \n'
                  'bbox_geojson: {14}'
                  # 'dataUrls: {8}, \n'
                  # 'metadataUrl: {9}'
                  .format(title,
                          abstract,
                          bbox,
                          keywords,
                          timepositions,
                          temporal_start,
                          temporal_end,
                          organization,
                          contact_name,
                          contact_email,
                          resource_url,
                          crs,
                          conforms_to,
                          scale_hint,
                          spatial
                          ))
        # ,dataUrls,
        # metadataUrl))

        """Package for ckan upload"""
        resource_dict = {
            'url': resource_url,
            'name': title,
            'format': 'wms',
            'licence': None,
            'description': None,
        }
        # handle url
        # Todo make this better
        ckan_dataset_url = title \
            .lower() \
            .replace('(', '') \
            .replace(')', '') \
            .strip() \
            .replace(' ', '-')

        package_dict = {
            'title': title,
            'identifier': title,  # Name is mandatory
            'name': ckan_dataset_url,  # url
            'notes': abstract,
            'documentation': None,
            'contact_name': contact_name,
            'contact_uri': None,
            'url': None,
            'tag_string': 'test',
            # 'theme': keywords,
            'theme': 'Boden',  # hardcoded for testing
            'spatial': str(spatial),
            'conforms_to': conforms_to,
            'spatial_resolution': None,
            'spatial_resolution_type': None,
            'temporal_start': None,
            'temporal_end': None,
            'temporal_resolution': None,
            'is_version_of': None,
            'related_resource': None,
            'derived_from': None,
            'owner_org': 'testing',
            'license_id': None,
            'resources': [resource_dict]
        }
        dataset_dicts.append(package_dict)

    return dataset_dicts


def create_dataset_dicts_from_wfs(wfs):
    # log.debug(wms.identification.type)
    # log.debug(wms.identification.title)
    # log.debug(wms.identification.abstract)

    wfs_contents = list(wfs.contents)
    log.debug(wfs_contents)

    dataset_dicts = []

    for layer in wfs_contents:
        cur_wfs = wfs[layer]
        gs_datastore = cur_wfs.id.split(':')[0]
        resource_url_wms = config['geoserver']['url'] + gs_datastore + '/wms?request=GetCapabilities'
        resource_url_wfs = config['geoserver']['url'] + gs_datastore + '/wfs?request=GetCapabilities'
        title = cur_wfs.title
        abstract = cur_wfs.abstract
        bbox = cur_wfs.boundingBox
        # crs = cur_wfs.crs_list[1][4].split(':')[1]
        crs = cur_wfs.crsOptions[0].getcode().split(':')[1]
        log.debug("CRS Options {}".format(crs))
        '''BBOX format ymin, xmin, ymax, xmax'''
        bbox_ymin = bbox[0]
        bbox_ymax = bbox[2]
        bbox_xmin = bbox[1]
        bbox_xmax = bbox[3]

        spatial = geojson.MultiPolygon([[[
            crs_transformation([bbox_ymin, bbox_xmin], crs, 4326),
            crs_transformation([bbox_ymin, bbox_xmax], crs, 4326),
            crs_transformation([bbox_ymax, bbox_xmax], crs, 4326),
            crs_transformation([bbox_ymax, bbox_xmin], crs, 4326),
            crs_transformation([bbox_ymin, bbox_xmin], crs, 4326),
        ]]])

        keywords = cur_wfs.keywords
        timepositions = cur_wfs.timepositions

        conforms_to = 'http://www.opengis.net/def/crs/EPSG/0/{}'.format(crs)

        '''Handle temporal resolution'''
        if timepositions is not None:
            timepositions_prep = timepositions[0].split('/')
            temporal_start = parser.parse(timepositions_prep[0])
            temporal_end = parser.parse(timepositions_prep[1])
        else:
            temporal_start = None
            temporal_end = None

        organization = wfs.provider.contact.organization
        contact_name = wfs.provider.contact.name
        contact_email = wfs.provider.contact.email
        # dataUrls = cur_wms.dataUrls
        # metadataUrl = cur_wms.metadataUrls

        """Package for ckan upload"""
        resource_dict = [
            {'url': resource_url_wms,
             'name': title,
             'format': 'wms',
             'licence': None,
             'description': None,
             },
            {'url': resource_url_wfs,
             'name': title,
             'format': 'wfs',
             'licence': None,
             'description': None,
             }
        ]
        # handle url
        # Todo make this better
        ckan_dataset_url = title \
            .lower() \
            .replace('(', '') \
            .replace(')', '') \
            .strip() \
            .replace(' ', '-')

        package_dict = {
            'title': title,
            'identifier': title,  # Name is mandatory
            'name': ckan_dataset_url,  # url
            'notes': abstract,
            'documentation': None,
            'contact_name': contact_name,
            'contact_uri': None,
            'url': None,
            'tag_string': 'test',
            # 'theme': keywords,
            'theme': 'Boden',  # hardcoded for testing
            'spatial': str(spatial),
            'conforms_to': conforms_to,
            'spatial_resolution': None,
            'spatial_resolution_type': None,
            'temporal_start': temporal_start,
            'temporal_end': temporal_end,
            'temporal_resolution': None,
            'is_version_of': None,
            'related_resource': None,
            'derived_from': None,
            'owner_org': 'testing',
            'license_id': None,
            'resources': resource_dict
        }
        dataset_dicts.append(package_dict)

        # Filter all None values
        none_value_keys = []
        for k, v in package_dict.items():
            if v is None:
                none_value_keys.append(k)

        log.info("Following properties are passed as None-values: {}".format(none_value_keys))

    return dataset_dicts


def create_dataset_dicts_from_wfs(wcs):
    # log.debug(wms.identification.type)
    # log.debug(wms.identification.title)
    # log.debug(wms.identification.abstract)

    wfs_contents = list(wfs.contents)
    log.debug(wfs_contents)

    dataset_dicts = []

    for layer in wfs_contents:
        cur_wfs = wfs[layer]
        gs_datastore = cur_wfs.id.split(':')[0]
        resource_url_wms = config['geoserver']['url'] + gs_datastore + '/wms?request=GetCapabilities'
        resource_url_wfs = config['geoserver']['url'] + gs_datastore + '/wfs?request=GetCapabilities'
        title = cur_wfs.title
        abstract = cur_wfs.abstract
        bbox = cur_wfs.boundingBox
        # crs = cur_wfs.crs_list[1][4].split(':')[1]
        crs = cur_wfs.crsOptions[0].getcode().split(':')[1]
        log.debug("CRS Options {}".format(crs))
        '''BBOX format ymin, xmin, ymax, xmax'''
        bbox_ymin = bbox[0]
        bbox_ymax = bbox[2]
        bbox_xmin = bbox[1]
        bbox_xmax = bbox[3]

        spatial = geojson.MultiPolygon([[[
            crs_transformation([bbox_ymin, bbox_xmin], crs, 4326),
            crs_transformation([bbox_ymin, bbox_xmax], crs, 4326),
            crs_transformation([bbox_ymax, bbox_xmax], crs, 4326),
            crs_transformation([bbox_ymax, bbox_xmin], crs, 4326),
            crs_transformation([bbox_ymin, bbox_xmin], crs, 4326),
        ]]])

        keywords = cur_wfs.keywords
        timepositions = cur_wfs.timepositions

        conforms_to = 'http://www.opengis.net/def/crs/EPSG/0/{}'.format(crs)

        '''Handle temporal resolution'''
        if timepositions is not None:
            timepositions_prep = timepositions[0].split('/')
            temporal_start = parser.parse(timepositions_prep[0])
            temporal_end = parser.parse(timepositions_prep[1])
        else:
            temporal_start = None
            temporal_end = None

        organization = wfs.provider.contact.organization
        contact_name = wfs.provider.contact.name
        contact_email = wfs.provider.contact.email
        # dataUrls = cur_wms.dataUrls
        # metadataUrl = cur_wms.metadataUrls

        """Package for ckan upload"""
        resource_dict = [
            {'url': resource_url_wms,
             'name': title,
             'format': 'wms',
             'licence': None,
             'description': None,
             },
            {'url': resource_url_wfs,
             'name': title,
             'format': 'wfs',
             'licence': None,
             'description': None,
             }
        ]
        # handle url
        # Todo make this better
        ckan_dataset_url = title \
            .lower() \
            .replace('(', '') \
            .replace(')', '') \
            .strip() \
            .replace(' ', '-')

        package_dict = {
            'title': title,
            'identifier': title,  # Name is mandatory
            'name': ckan_dataset_url,  # url
            'notes': abstract,
            'documentation': None,
            'contact_name': contact_name,
            'contact_uri': None,
            'url': None,
            'tag_string': 'test',
            # 'theme': keywords,
            'theme': 'Boden',  # hardcoded for testing
            'spatial': str(spatial),
            'conforms_to': conforms_to,
            'spatial_resolution': None,
            'spatial_resolution_type': None,
            'temporal_start': temporal_start,
            'temporal_end': temporal_end,
            'temporal_resolution': None,
            'is_version_of': None,
            'related_resource': None,
            'derived_from': None,
            'owner_org': 'testing',
            'license_id': None,
            'resources': resource_dict
        }
        dataset_dicts.append(package_dict)

        # Filter all None values
        none_value_keys = []
        for k, v in package_dict.items():
            if v is None:
                none_value_keys.append(k)

        log.info("Following properties are passed as None-values: {}".format(none_value_keys))

    return dataset_dicts


def get_packages_of_ckan_instance():
    ckan_url = config['ckan']['url']
    url = ckan_url + '/api/action/package_list'

    with urllib.request.urlopen(url) as response:
        contents = json.loads(response.read().decode('utf8'))
        packages = contents['result']

    return packages


def push_dataset_to_ckan(dataset_dict):
    ckan_url = config['ckan']['url']

    dataset_name = dataset_dict['name']
    dataset_dict['id'] = dataset_dict['name']

    # Use the json module to dump the dictionary to a string for posting.
    data_string = urllib.parse.quote(json.dumps(dataset_dict))
    # data_string = urllib.parse.urlencode(dataset_dict)
    data = data_string.encode('ascii')

    header = {'Authorization': config['ckan']['apikey']}

    '''New Packages'''
    if dataset_name not in get_packages_of_ckan_instance():
        log.debug("Inserting following new dataset: {}".format(dataset_name))
        # We'll use the package_create function to create a new dataset.
        # Creating a dataset requires an authorization header.
        url = ckan_url + '/api/action/package_create'
        req = urllib.request.Request(url, data, header)

        # Make the HTTP request.
        with urllib.request.urlopen(req) as response:
            the_page = response.read()
            assert response.code == 200

            '''
            # Use the json module to load CKAN's response into a dictionary.
            response_dict = json.loads(response.read())
            log.debug(response_dict)
            assert response_dict['success'] is True
    
            # package_create returns the created package as its result.
            created_package = response_dict['result']
            plog.debug.plog.debug(created_package)
            '''

    else:
        '''Update of package'''

        log.debug("Updating following dataset: {}".format(dataset_name))

        # Creating a dataset requires an authorization header.
        # Uses package_patch function because this will only overwrite given parameters
        # Change url to package_update if you prefer
        url = ckan_url + '/api/action/package_patch'
        req = urllib.request.Request(url, data, header)

        # Make the HTTP request.
        with urllib.request.urlopen(req) as response:
            the_page = response.read()
            assert response.code == 200


if __name__ == '__main__':
    geoserver_url = config['geoserver']['url']
    wms_version = config['geoserver']['wms']['version']
    wfs_version = config['geoserver']['wfs']['version']

    wms = WebMapService(geoserver_url + 'ows?service=wms&request=GetCapabilities&version=' + wms_version,
                        version=wms_version)

    wfs = WebFeatureService(geoserver_url + 'ows?service=wfs&request=GetCapabilities&version=' + wfs_version,
                            version=wfs_version)

    # datasets = create_dataset_dicts_from_wms(wms)
    datasets = create_dataset_dicts_from_wfs(wfs)

    test_dict = {
        'identifier': 'api_test2',
        'title': 'api_test2',
        'name': 'api_test2',
        'contact_name': 'Simeon',
        'spatial': '{"type":"MultiPolygon","coordinates":'
                   '[[['
                   '[11.75097495317459,50.557418843325934],'
                   '[11.75097495317459,51.38755183511753],'
                   '[13.201170265674591,51.38755183511753],'
                   '[13.201170265674591,50.557418843325934],'
                   '[11.75097495317459,50.557418843325934]'
                   ']]]'
                   '}',
        'theme': 'Boden',
        'owner_org': 'testing'
    }
    # push_dataset_to_ckan(test_dict)
    # log.debug(datasets[2])

    log.info("Pushing all wms into ckan")

    #for dataset in datasets:
    #    push_dataset_to_ckan(dataset)
