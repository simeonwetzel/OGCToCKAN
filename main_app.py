import yaml

from ckan_tools import CkanInstance
from ftp_tools import FtpRekis
from ows_tools import OGCHelperClass

import logging

ckan = CkanInstance()
ftp = FtpRekis()
ows = OGCHelperClass()

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

config = yaml.safe_load(open('config.yml'))

"""
Main App calling methods of different classes
"""



# Collecting data from ows source and push it to ckan
wcs_datasets = ows.create_dataset_dicts_from_wfs_or_wcs(service_type='wcs')

log.info("Uploading wcs/wms resources to CKAN")
for dataset in wcs_datasets:
    ckan.push_dataset_to_ckan(dataset)

wfs_datasets = ows.create_dataset_dicts_from_wfs_or_wcs(service_type='wfs')

log.info("Uploading wfs/wms resources to CKAN")
for dataset in wfs_datasets:
    ckan.push_dataset_to_ckan(dataset)


# Create dictionaries that list all rekis files and their corresponding directories on the ftp-server
# Start collecting data from single state-folders (to look into necessary folders)
rekis_ftp_file_dict_sn = ftp.create_ftp_file_dict(config['ftp']['data_paths']['SN'])
rekis_ftp_file_dict_th = ftp.create_ftp_file_dict(config['ftp']['data_paths']['TN'])
rekis_ftp_file_dict_st = ftp.create_ftp_file_dict(config['ftp']['data_paths']['ST'])
# Merge multiple dicts into one
#
rekis_ftp_file_dict_all = {**rekis_ftp_file_dict_sn, **rekis_ftp_file_dict_th, **rekis_ftp_file_dict_st}


# TODO: Move these two functions to ftp or ckan class
def create_dict_with_ftp_files_with_matching_ckan_packages(ftp_file_dict):
    """Checks for each key in ftp_file_dict whether there is a matching dataset in ckan"""
    packages = ckan.get_packages_of_ckan_instance()
    ftp_files_with_matching_packages = {}
    for key in ftp_file_dict:
        filename_without_suffix = key[:-4]
        ckan_compliant_name = ckan.create_ckan_compliant_url_from_name(filename_without_suffix)
        if ckan_compliant_name in packages:
            ftp_files_with_matching_packages[key] = ftp_file_dict[key]

    return ftp_files_with_matching_packages


def upload_ftp_resources(ftp_file_dict):
    file_dict = create_dict_with_ftp_files_with_matching_ckan_packages(ftp_file_dict)
    for key in file_dict:
        name = key[:-4]
        path = file_dict[key]['path']
        create_date = file_dict[key]['create_date']
        ckan.upload_ftp_file_resource_to_ckan_dataset(name, path, create_date)


upload_ftp_resources(rekis_ftp_file_dict_all)