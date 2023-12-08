import ftplib
import logging
import tempfile
from io import BytesIO
import os

import yaml
from ftp_walk import FTPWalk

dirname = os.path.dirname(__file__)
config_filename = os.path.join(dirname, '../config/config.yml')

config = yaml.safe_load(open(config_filename))

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


class FtpRekis:
    """This class contains all necessary functions to collect resources from ftp source"""

    def __init__(self):
        self.ip = config['ftp']['ip']
        self.user = config['ftp']['user']
        self.passwd = config['ftp']['passwd']


    def create_ftp_file_dict(self, base_directory):
        """This function walks through ftp dir-tree starting with given base_directory
         and then creates dictionary in following format:
         {filename: {path: file/pth/..., create_date: ISO_date}}"""

        log.info("Start collecting directories for ftp-files // starting from following folder: {}".format(base_directory))
        file_dict = {}
        with ftplib.FTP(self.ip, self.user, self.passwd) as connection:
            connection.login(self.user, self.passwd)
            connection.encoding = "iso-8859-15"
            ftpwalk = FTPWalk(connection)

            ftp_dir_tree = []

            # Walk through all sub-dirs of base path and create structured list
            for i in ftpwalk.walk(base_directory):
                if not i[1]:  # entry for files
                    for file in i[-1]:
                        ftp_dir_tree.append([i[0],  # Path
                                             file[0],  # Filename
                                             file[1]  # Date of creation
                                             ])

            # Create dictionary from structured list above
            for file_entry in ftp_dir_tree:
                file_dict[file_entry[0] + '/' + file_entry[1]] = { # Full path
                    'file_name': file_entry[1], # Filename
                    'create_date': file_entry[2] # Create Date
                }

            return file_dict

    @staticmethod
    def create_filtered_subdict_with_ncdf_ftp_files(dict):
        """Checks for each key in ftp_file_dict whether there is a matching dataset in ckan"""
        ncdf_files = {}
        for key in dict:
            if '.nc' or '.rar' in key:
                #ncdf_files[key] = dict[key]
                #  FTP paths are changed to
                #  https://rekisviewer.hydro.tu-dresden.de/geoserver/klima/taegliche_daten/SA/Niederschlag.nc
                #  maybe possible to do this more generic
                path_split_and_modified = key.split('/')[3:]
                path_split_and_modified.insert(0, 'https://rekisviewer.hydro.tu-dresden.de/geoserver')
                new_path = "/".join(path_split_and_modified)
                ncdf_files[new_path] = dict[key]
        return ncdf_files

    @staticmethod
    def create_filtered_subdict_with_ascii_ftp_files(dict):
        """Checks for each key in ftp_file_dict whether there is a matching dataset in ckan"""
        ascii_files = {}
        for key in dict:
            if '.asc' in key:
                #  maybe possible to do this more generic
                path_split_and_modified = key.split('/')[3:]
                path_split_and_modified.insert(0, 'https://rekisviewer.hydro.tu-dresden.de/viewer')
                new_path = "/".join(path_split_and_modified)
                ascii_files[new_path] = dict[key]
        return ascii_files

    def create_tempfile_from_ftp_file(self, directory, filename):
        with ftplib.FTP(self.ip, self.user, self.passwd) as ftp:
            ftp.login(self.user, self.passwd)
            # Change working directory
            ftp.cwd(directory)
            filename += ".asc"  # Make this more generic

            # Read file content as binary
            r = BytesIO()
            ftp.retrbinary('RETR {}'.format(filename), r.write)

            # Create temporary file with suffix .asc
            tf = tempfile.NamedTemporaryFile(suffix='.asc')
            tf.name = filename
            # Write binary content into temporary file
            tf.write(r.getvalue())
            tf.seek(0)

            return tf
