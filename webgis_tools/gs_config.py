import yaml
import logging
import requests
import xml.etree.ElementTree as ET

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())
config = yaml.safe_load(open('../config/config.yml'))


class GeoserverConfig:
    """Class for REST requests, config etc."""

    def __init__(self):
        self.service_url = config['gs_config']['gs_url']
        self.gs_user = config['gs_config']['gs_user']
        self.gs_pw = config['gs_config']['gs_pw']

        self.headers = {'Content-Type': 'text/xml'}
        self.auth_credentials = (self.gs_user, self.gs_pw)

    def create_url_for_datastore(self, workspace, store_name):
        url = "{0}/rest/workspaces/{1}/datastores/{2}/featuretypes/{2}.xml".format(
            self.service_url, workspace, store_name
        )
        return url

    def create_url_for_upload(self, workspace, store_name, file_extension):
        file_extension = "shp"
        url = "{0}/rest/workspaces/{1}/datastores/{2}/file.{3}?filename={2}&update=overwrite".format(
            self.service_url, workspace, store_name, file_extension
        )
        return url

    def get_list_of_datastores_of_a_workspace(self, workspace):
        url = "{}/rest/workspaces/{}/datastores.json".format(
            self.service_url, workspace
        )
        log.debug(url)
        r = requests.get(url, auth=self.auth_credentials)
        data_stores = r.json()
        data_store_names = [i['name'] for i in data_stores['dataStores']['dataStore']]
        return data_store_names

    def get_metadata_from_datastore(self, url):
        r = requests.get(url=url, headers=self.headers, auth=self.auth_credentials)
        root = ET.fromstring(r.content)
        xml_encoding = ET.tostring(root, encoding='utf8', method='xml')
        return xml_encoding

    def upload_metadata_for_datastore(self, url, data):
        # Update metadata via put request
        r = requests.put(url=url, headers=self.headers, auth=self.auth_credentials, data=data)
        log.debug(r)
        return

    def reset_cache(self):
        url = "{}/rest/reset".format(self.service_url)
        r = requests.post(url, self.auth_credentials)
        return "Status code: {}".format(r.status_code)

    def upload_shapefile_to_gs(self, url, path_to_zipped_shapefile):
        headers = {
            "Content-type": "application/zip",
            "Accept": "application/xml",
        }
        with open(path_to_zipped_shapefile, "rb") as f:
            r = requests.put(
                url=url,
                data=f.read(),
                auth=self.auth_credentials,
                headers=headers,
            )
            if r.status_code in [200, 201]:
                print("The shapefile datastore created successfully!")

            else:
                print("{}: The shapefile datastore can not be created! {}".format(
                    r.status_code, r.content)
                )

    def upload_file_to_gs(self, url, path_to_file, file_type):
        headers = {
            "Accept": "application/xml",
        }

        if file_type == 'shapefile':
            headers["Content-type"] = "application/zip"
        elif file_type == 'netcdf':
            headers["Content-type"] = "application/x-netcdf"
        # Add more conditions for other file types if needed

        with open(path_to_file, "rb") as f:
            r = requests.put(
                url=url,
                data=f.read(),
                auth=self.auth_credentials,
                headers=headers,
            )
            if r.status_code in [200, 201]:
                print(f"The {file_type} datastore created successfully!")
            else:
                print(f"{r.status_code}: The {file_type} datastore cannot be created! {r.content}")


"""
    def create_datastore(self, workspace, store_name, datastore_type):
        # Update connection parameters based on the datastore type
        connection_parameters = {
            "url": self.service_url,
            "user": self.gs_user,
            "passwd": self.gs_pw,
            # Add more parameters based on the datastore type
        }

        url = f"{self.service_url}/rest/workspaces/{workspace}/datastores/{store_name}.xml"

        headers = {
            "Content-Type": "application/xml",
            "Accept": "application/xml",
        }

        data = {
            "dataStore": {
                "name": store_name,
                "type": datastore_type,
                "enabled": True,
                "connectionParameters": connection_parameters,
            }
        }

        response = requests.post(url, headers=headers, auth=self.auth_credentials, json=data)

        if response.status_code == 201:
            log.debug(f"Datastore '{store_name}' created successfully.")
        else:
            log.debug(f"Failed to create datastore. Status code: {response.status_code}, Response: {response.text}")
"""
