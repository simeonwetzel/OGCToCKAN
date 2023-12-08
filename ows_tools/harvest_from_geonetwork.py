from ckan_tools.ckan_tools import CkanInstance
from ows_tools.rdf_tools import RdfInstance
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo
import yaml
import logging
import urllib

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

config = yaml.safe_load(open('../config/config.yml'))

base_url = config['geonetwork']['url']

# capabilities_url = f'{base_url}catalogue/srv/eng/csw?service=CSW&version=2.0.2&request=GetCapabilities'
capabilities_url = f'{base_url}catalogue/climate-adapt/eng/csw?service=CSW&version=2.0.2&request=GetCapabilities'

csw = CatalogueServiceWeb(capabilities_url)

ckan = CkanInstance()
rdf = RdfInstance()


def get_records_for_given_keyword(keyword=None, max_records=1):
    if keyword is not None:
        con1 = [PropertyIsEqualTo('csw:AnyText', keyword)]
        csw.getrecords2(constraints=con1, maxrecords=max_records)

    else:
        csw.getrecords2(maxrecords=max_records)


    return [csw.records[rec].identifier for rec in csw.records]


def create_ckan_package_dict_for_records(list_of_record_ids):
    list_of_dicts = []
    for record_id in list_of_record_ids:
        package = rdf.get_ckan_package_dict_from_geonetwork_rdf(record_id=record_id)
        list_of_dicts.append(package)

    return list_of_dicts


list_of_ids = get_records_for_given_keyword(max_records=500)

packages = create_ckan_package_dict_for_records(list_of_ids)

for package in packages:
    try:
        ckan.push_dataset_to_ckan(package)
    except urllib.error.HTTPError:
        log.info('Cant upload to CKAN due to an Validation Error. See CKAN logs for further information')
