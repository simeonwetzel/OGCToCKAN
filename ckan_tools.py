import json
import logging
import urllib

import requests
import text_unidecode
import yaml

from main import rekis_create_tempfile_from_ftp_file

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

config = yaml.safe_load(open('config.yml'))


class CkanInstance:
    """This class contains ckan properties and functions"""

    def __init__(self):
        self.ckan_url = config['ckan']['url']
        self.api_action_url = self.ckan_url + '/api/action/'
        self.api_package_list = self.api_action_url + 'package_list'
        self.api_package_create = self.api_action_url + 'package_create'
        self.api_package_patch = self.api_action_url + 'package_patch'
        self.api_resource_create = self.api_action_url + 'resource_create'
        self.api_resource_patch = self.api_action_url + 'resource_patch'
        self.header = {'Authorization': config['ckan']['apikey']}

    @staticmethod
    def create_ckan_compliant_url_from_name(name):
        url = text_unidecode.unidecode(name).lower().replace(' ', '-')
        return url

    def create_ckan_organization_if_not_exists(self, org_name):
        # log.info("Check if needed to create a new organization")
        # org_url_str = text_unidecode.unidecode(org_name).lower().replace(' ', '-')
        org_url_str = self.create_ckan_compliant_url_from_name(org_name)

        # log.debug('ORG URL = {}'.format(org_url_str))
        organization_data = {'name': org_url_str,
                             'title': org_name}
        data_string = urllib.parse.quote(json.dumps(organization_data))
        data = data_string.encode('ascii')

        with urllib.request.urlopen(self.api_action_url + 'organization_list') as response:
            contents = json.loads(response.read().decode('utf8'))
            packages = contents['result']
            # log.debug(packages)

        if org_url_str not in packages:
            #   log.info("Creating new organization with name: {}".format(org_name))
            req = urllib.request.Request(self.api_action_url + 'organization_create', data, self.header)
            with urllib.request.urlopen(req) as response:
                the_page = response.read()
                assert response.code == 200

    def get_packages_of_ckan_instance(self):
        with urllib.request.urlopen(self.api_package_list) as response:
            contents = json.loads(response.read().decode('utf8'))
            packages = contents['result']

        return packages

    def check_if_ckan_dataset_resource_exists(self, dataset_name, resource_cache_url, resource_create_date):
        """This function checks for given ckan dataset if there are already resources
        if there are existing resources, the function returns a list of them
        """
        resource_already_exists = bool

        dataset_name_url = self.create_ckan_compliant_url_from_name(dataset_name)

        existing_resources_with_matching_name = []

        with urllib.request.urlopen(self.api_package_list) as response:
            contents = json.loads(response.read().decode('utf8'))
            packages = contents['result']
            # log.debug(packages)

        if dataset_name_url in packages:
            """Get existing resources of ckan-dataset in order 
            to find out if resource already exists and just needs to be updated"""
            dataset_package = requests.get(self.api_action_url + 'package_show?id={}'.format(dataset_name_url)).json()
            resources = dataset_package['result']['resources']
            # Filter out wms/wfs resources
            for resource in resources:
                if resource['cache_url'] == resource_cache_url:
                    existing_resources_with_matching_name.append(resource)

            if existing_resources_with_matching_name:
                for item in existing_resources_with_matching_name:

                    if item['cache_url'] == resource_cache_url and item['created'] == resource_create_date:
                        """
                        Case 1: Resource with equal name, url and create-date already exists, but file content maybe changed
                        ... do update 
                        """
                        log.info('Resource {} already exists... Updating existing resource'.format(dataset_name_url))
                        resource_already_exists = True
                    else:
                        """
                        Case 2: Resource with equal name already exists but has different create date: suspecting different file
                        ... do insert
                        """
                        resource_already_exists = False
            else:
                """
                Case 3: No Resource with equal name already exists:
                ... do insert
                """
                resource_already_exists = False

        return resource_already_exists, existing_resources_with_matching_name

    def upload_ftp_file_resource_to_ckan_dataset(self, dataset_name, resource_path, file_create_date):
        dataset_name_url = self.create_ckan_compliant_url_from_name(dataset_name)

        # Create tempory file from ftp resource:
        # Todo: create rekis class with function
        temporary_ftp_file = rekis_create_tempfile_from_ftp_file(directory=resource_path, filename=dataset_name)

        package_data = {'package_id': dataset_name_url,
                        'cache_url': resource_path,
                        'created': file_create_date
                        }
        dst_resource_exists, existing_resources = self.check_if_ckan_dataset_resource_exists(dataset_name,
                                                                                             resource_path,
                                                                                             file_create_date)

        if dst_resource_exists:
            for item in existing_resources:
                log.info('Updating exisiting resource with name {}'.format(item['name']))
                package_data['id'] = item['id']
                requests.post(url=self.api_resource_patch,
                              data=package_data,
                              headers=self.header,
                              files=[('upload', temporary_ftp_file)])
        else:
            log.info('Create new resource with name {}'.format(dataset_name_url))
            requests.post(url=self.api_resource_create,
                          data=package_data,
                          headers=self.header,
                          files=[('upload', temporary_ftp_file)])

    def push_dataset_to_ckan(self, dataset_dict):
        log.debug("Uploading following data: \n {}".format(dataset_dict))
        dataset_name = dataset_dict['name']
        dataset_dict['id'] = dataset_dict['name']
        # additional_resource_paths = dataset_dict['url']
        dataset_dict['url'] = None

        # Use the json module to dump the dictionary to a string for posting.
        data_string = urllib.parse.quote(json.dumps(dataset_dict))
        # data_string = urllib.parse.urlencode(dataset_dict)
        data = data_string.encode('ascii')

        '''New Packages'''
        if dataset_name not in self.get_packages_of_ckan_instance():
            log.debug("Inserting following new dataset: {}".format(dataset_name))
            # We'll use the package_create function to create a new dataset.
            # Creating a dataset requires an authorization header.
            req = urllib.request.Request(self.api_package_create, data, self.header)

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
                # if additional_resource_paths:
                #     for resource_path in additional_resource_paths:
                #         log.info('Try to upload local file {0} for resource {1}'.format(resource_path, dataset_name))
                #         upload_local_file_resource_to_ckan_dataset(dataset_name=dataset_name,
                #                                                    resource_path=resource_path)

        else:
            '''Update of package'''

            log.debug("Updating following dataset: {}".format(dataset_name))

            # Creating a dataset requires an authorization header.
            # Uses package_patch function because this will only overwrite given parameters
            # Change url to package_update if you prefer
            req = urllib.request.Request(self.api_package_patch, data, self.header)

            # Make the HTTP request.
            with urllib.request.urlopen(req) as response:
                the_page = response.read()
                assert response.code == 200

                # if additional_resource_paths:
                #     for resource_path in additional_resource_paths:
                #         log.info('Try to upload local file {0} for resource {1}'.format(resource_path, dataset_name))
                #         upload_local_file_resource_to_ckan_dataset(dataset_name=dataset_name,
                #                                                    resource_path=resource_path,
                #                                                    resource_storage_type='local')
