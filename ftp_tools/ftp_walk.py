# -*- coding: utf-8 -*-

from os import path as ospath
import logging

import dateutil.parser

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


class FTPWalk:
    """
    This class is containing corresponding functions for traversing the FTP
    servers using BFS algorithm.
    """
    def __init__(self, connection):
        self.connection = connection

    def listdir(self, _path):
        """
        return files and directory names within a path (directory)
        """

        file_list, dirs, nondirs = [], [], []
        try:
            self.connection.cwd(_path)
        except Exception as exp:
            log.debug("Current FTP path is : ", self.connection.pwd(), exp.__str__(),_path)
            return [], []
        else:
            self.connection.retrbinary('LIST', lambda x: file_list.append(x.splitlines()))
            if file_list: # If folder is not empty
                files_and_dirs = [i.split() for i in file_list[0]]
                for info in files_and_dirs:
                    ls_type, name = info[-2], info[-1]
                    if ls_type == b'<DIR>':
                        try:
                            #dirs.append(name.decode("utf-8"))
                            dirs.append(name.decode("iso-8859-15"))
                        except UnicodeDecodeError as e:
                            log.debug("Cant read ftp-directory because of German umlauts: {}".format(name.decode('iso-8859-15')))
                            pass
                    else:
                        try:
                            name_str, date_str, time_str = name.decode('iso-8859-15'), \
                                                           info[0].decode('iso-8859-15'), \
                                                           info[1].decode('iso-8859-15')
                            """
                                                            name.decode('utf-8'), \
                                                            info[0].decode('utf-8'), \
                                                            info[1].decode('utf-8')
                            """
                            create_date = dateutil.parser \
                                .parse("{0}, {1}".format(date_str, time_str)) \
                                .isoformat()
                            nondirs.append([name_str, create_date])
                        except UnicodeDecodeError as e:
                            log.debug("Cant read ftp-file because of German umlauts: {}".format(name))
                            pass

            return dirs, nondirs

    def walk(self, path='/'):
        """
        Walk through FTP server's directory tree, based on a BFS algorithm.
        """
        dirs, nondirs = self.listdir(path)
        yield path, dirs, nondirs

        for name in dirs:
            path = ospath.join(path, name).replace("\\", "/")
            yield from self.walk(path)
            self.connection.cwd('..')
            path = ospath.dirname(path)
