import parser

import pandas
from owslib.wcs import WebCoverageService
from owslib.wfs import WebFeatureService
from owslib.wms import WebMapService
from pandas import ExcelFile
import yaml
import logging
from ckan_tools.ckan_tools import CkanInstance
import geojson


from pyproj import CRS, Transformer

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

config = yaml.safe_load(open('../config/config.yml'))


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


def create_metadata_dict_for_dataset(dataset_name, external_metadata_dict):
    meta_dict = {}
    if dataset_name in external_metadata_dict['dataset_name'].values():
        datasets = external_metadata_dict['dataset_name']
        idx_in_dict = list(datasets.keys())[list(datasets.values()).index(dataset_name)]
        for key in external_metadata_dict.keys():
            meta_dict[key] = external_metadata_dict[key][idx_in_dict]
    else:
        log.info("No external Metadata found for dataset: {}".format(dataset_name))
    return meta_dict


def crs_transformation(coords, epsg_old, epsg_new):
    old_crs = CRS.from_epsg(epsg_old)
    new_crs = CRS.from_epsg(epsg_new)
    transformer = Transformer.from_crs(old_crs, new_crs)
    transformed_coords = transformer.transform(coords[1], coords[0])

    return transformed_coords[1], transformed_coords[0]


def make_geojson_object_from_bbox(bbox_coords, bbox_crs):
    """Creating BBOX In EPSG 4326 compliant with CKAN spatial"""
    # BBOX format ymin, xmin, ymax, xmax'''
    bbox_ymin = bbox_coords[0]
    bbox_ymax = bbox_coords[2]
    bbox_xmin = bbox_coords[1]
    bbox_xmax = bbox_coords[3]

    spatial = geojson.MultiPolygon([[[
        crs_transformation([bbox_ymin, bbox_xmin], bbox_crs, 4326),
        crs_transformation([bbox_ymin, bbox_xmax], bbox_crs, 4326),
        crs_transformation([bbox_ymax, bbox_xmax], bbox_crs, 4326),
        crs_transformation([bbox_ymax, bbox_xmin], bbox_crs, 4326),
        crs_transformation([bbox_ymin, bbox_xmin], bbox_crs, 4326),
    ]]])

    return spatial


class OGCHelperClass:
    """
    This class provides helper functions to request necessary data from OGC-Services
    in use of the owslib classes
    """

    def __init__(self):
        self.ckan = CkanInstance()
        self.geoserver_url = config['gs_config']['service_url']
        self.wms_version = config['geoserver']['wms']['version']
        self.wfs_version = config['geoserver']['wfs']['version']
        self.wcs_version = config['geoserver']['wcs']['version']

        self.wms = WebMapService(
            self.geoserver_url + 'ows?service=wms&request=GetCapabilities&version=' + self.wms_version,
            version=self.wms_version)

        self.wfs = WebFeatureService(
            self.geoserver_url + 'ows?service=wfs&request=GetCapabilities&version=' + self.wfs_version,
            version=self.wfs_version)

        self.wcs = WebCoverageService(
            self.geoserver_url + 'ows?service=WCS&request=GetCapabilities&version=' + self.wcs_version,
            version=self.wcs_version)

        self.external_metadata_resource = create_metadata_dict_from_xls_file('../ressources/metadata_template.xlsx')

    def create_dataset_dicts_from_wfs_or_wcs(self, service_type):
        if service_type == 'wcs':
            """WCS"""
            service = self.wcs
            service_contents = [layer.replace('__', ':') for layer in
                                service.contents]  # datastore:layername is needed for wms
        else:
            """WFS"""
            service = self.wfs
            service_contents = list(service.contents)

        dataset_dicts = []

        for layer in service_contents:
            if service_type == 'wcs':
                cur_service = self.wms[layer]
            else:
                cur_service = service[layer]

            gs_datastore = cur_service.id.split(':')[0]

            resource_url_wms = self.geoserver_url + gs_datastore + '/wms?request=GetCapabilities'
            resource_url_wcs = self.geoserver_url + gs_datastore + '/wcs?request=GetCapabilities'
            resource_url_wfs = self.geoserver_url + gs_datastore + '/wfs?request=GetCapabilities'
            # title = cur_wfs.title

            # Search in Metadata from excel-source and create dict
            meta_dict = create_metadata_dict_for_dataset(dataset_name=cur_service.title,
                                                         external_metadata_dict=self.external_metadata_resource)

            title = cur_service.title
            log.info('---collecting metadata for layer: {0} / {1}'.format(service_type, title))
            owner_org = gs_datastore
            abstract = cur_service.abstract
            inspire_theme = 'http://inspire.ec.europa.eu/theme/ac'  # default
            tags = None
            local_resource_paths = None
            if meta_dict:
                if meta_dict['should_be_uploaded'] is None:
                    log.info('Skipping dataset because of configuration in metadata sheet')
                    continue # Leaving the for loop for this dataset because parameter set in metadata excel
                if meta_dict['name'] is not None:
                    title = meta_dict['name']
                if meta_dict['alternate_ckan_org'] is not None:
                    owner_org = meta_dict['alternate_ckan_org']
                if meta_dict['notes'] is not None:
                    abstract = meta_dict['notes']
                if meta_dict['inspire_theme_url'] is not None:
                    inspire_theme = meta_dict['inspire_theme_url']
                if meta_dict['tag_string'] is not None:
                    tags = meta_dict['tag_string']
                if meta_dict['path_to_file'] is not None:
                    local_resources = meta_dict['path_to_file'].split(',')
                    local_resource_paths = [resource_paths.replace('\\', '/') for resource_paths in local_resources]

            # Create Organization with gs_datastore name in ckan if not already exists
            self.ckan.create_ckan_organization_if_not_exists(owner_org)
            owner_org_url = self.ckan.create_ckan_compliant_url_from_name(owner_org)

            bbox = cur_service.boundingBox

            # crs = cur_wfs.crs_list[1][4].split(':')[1]
            if service_type == 'wfs':
                crs = cur_service.crsOptions[0].getcode().split(':')[1]
                bbox_crs = bbox[4].getcode().split(':')[1]
            else:
                crs = cur_service.crs_list[1][4].split(':')[1]
                if bbox[4] == 'CRS:84':
                    bbox_crs = 4326
                else:
                    bbox_crs = bbox[4].split(':')[1]
            # log.debug("CRS Options {}".format(crs))

            spatial = make_geojson_object_from_bbox(bbox, bbox_crs)

            keywords = cur_service.keywords
            timepositions = cur_service.timepositions

            conforms_to = 'http://www.opengis.net/def/crs/EPSG/0/{}'.format(crs)

            if cur_service == 'wcs':
                scale_hint = cur_service.scaleHint

            '''Handle temporal resolution'''
            if timepositions is not None:
                timepositions_prep = timepositions[0].split('/')
                temporal_start = parser.parse(timepositions_prep[0])
                temporal_end = parser.parse(timepositions_prep[1])
            else:
                temporal_start = None
                temporal_end = None

            # Take organization and contact name from geoserver provider
            # organization = service.provider.contact.organization
            # contact_name = service.provider.contact.name

            contact_name = "Hydro TUD"
            contact_email = service.provider.contact.email
            # dataUrls = cur_wms.dataUrls
            # metadataUrl = cur_wms.metadataUrls

            """Package for ckan upload"""
            resource_dict = [
                {'url': resource_url_wms,
                 'name': title,
                 'format': 'wms',
                 'licence': None,
                 'description': None,
                 }
            ]

            if service_type == 'wcs':
                resource_dict.append(
                    {'url': resource_url_wcs,
                     'name': title,
                     'format': 'wfs',
                     'licence': None,
                     'description': None,
                     }
                )
            else:
                resource_dict.append(
                    {'url': resource_url_wfs,
                     'name': title,
                     'format': 'wfs',
                     'licence': None,
                     'description': None,
                     }
                )

            # handle url
            # Todo make this better
            ckan_dataset_url = cur_service.title \
                .lower() \
                .replace('(', '') \
                .replace(')', '') \
                .strip() \
                .replace(' ', '-')

            package_dict = {
                'title': title,
                'identifier': cur_service.title,  # Name is mandatory
                'name': ckan_dataset_url,  # url
                'notes': abstract,
                'documentation': None,
                'contact_name': contact_name,
                'contact_uri': None,
                'url': local_resource_paths,  # store paths temporarily here
                'tag_string': tags,
                # 'theme': keywords,
                'theme': inspire_theme,
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
                'owner_org': owner_org_url,
                'license_id': None,
                'resources': resource_dict
            }
            dataset_dicts.append(package_dict)

            # Filter all None values
            none_value_keys = []
            for k, v in package_dict.items():
                if v is None:
                    none_value_keys.append(k)

        # log.info("Following properties are passed as None-values: {}".format(none_value_keys))

        log.info(
            "Created {} packages from {} sources. Ready for upload to ckan".format(len(dataset_dicts), service_type))
        return dataset_dicts
