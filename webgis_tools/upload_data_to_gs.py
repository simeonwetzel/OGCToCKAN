import yaml
from gs_config import GeoserverConfig
import glob
import os
import logging

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

# init geoserver class
gs = GeoserverConfig()

# load config
config = yaml.safe_load(open('config.yml'))

# declare config parameters
workspace = config['gs_config']['workspace']
store_name = config['gs_config']['store_name']
ftp_path = config['gs_config']['path_to_zipped_shapefiles']


def upload_all_shapes_of_a_directory(input_dir, gs_workspace):
    """path must contain zipped shapefiles"""
    for zipped_file in glob.glob(input_dir + "*.zip"):
        log.debug("Uploading following zip: {0}".format(zipped_file))

        # Automatically derive store_name from file name
        # Took this code from https://github.com/gicait/geoserver-rest/blob/master/geo/Geoserver.py #
        gs_store_name = os.path.basename(zipped_file)
        f = gs_store_name.split(".")
        if len(f) > 0:
            gs_store_name = f[0]

        url = gs.create_url_for_upload(workspace=gs_workspace, store_name=gs_store_name, file_extension='shp')
        gs.upload_file_to_gs(url=url, path_to_file=zipped_file, file_type='shapefile')


def upload_all_netcdfs_of_a_directory(input_dir, gs_workspace):
    """path must contain netcdf files"""
    for nc_file in glob.glob(input_dir + "*.nc"):
        log.debug("Uploading following netcdf: {0}".format(nc_file))

        # Automatically derive store_name from file name
        # Took this code from https://github.com/gicait/geoserver-rest/blob/master/geo/Geoserver.py #
        gs_store_name = os.path.basename(nc_file)
        f = gs_store_name.split(".")
        if len(f) > 0:
            gs_store_name = f[0]

        url = gs.create_url_for_upload(workspace=gs_workspace, store_name=gs_store_name, file_extension='nc')
        gs.upload_file_to_gs(url=url, path_to_file=nc_file, file_type='netcdf')

"""
upload_all_shapes_of_a_directory(input_dir=path,
                                 gs_workspace=workspace,
                                 )
"""
upload_all_netcdfs_of_a_directory(input_dir=ftp_path,
                                  gs_workspace=workspace,
                                  )

"""
url = gs.create_url_for_shapefile_upload(workspace=workspace, store_name=store_name)
gs.upload_shapefile_to_gs(url=url, path_to_zipped_shapefile=path)
"""
