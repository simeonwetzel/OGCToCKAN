import rdflib
from rdflib import Graph, Namespace
from ckan_tools import CkanInstance
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo, PropertyIsLike, BBox
import yaml
import logging
import requests
import re

import datetime
import ast
import json

from public.dcat_ckan_mapping import mapping, ckan_rdf
from lxml.etree import XML, XSLT, tostring, fromstring
from urllib.request import urlopen
import argparse

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

config = yaml.safe_load(open('../config/config.yml', encoding='UTF-8'))


class RdfInstance:
    """This class contains ckan properties and functions"""

    def __init__(self):
        self.base_url = '{}/catalogue/srv/api/records'.format(config['geonetwork']['url'])
        self.dcat_ckan_mapping = mapping
        self.ckan_rdf_template = ckan_rdf
        self.ckan_instance = CkanInstance()

    def _transform_ISO_19139_dcat(self, xml_string):
        xsl = "https://raw.githubusercontent.com/SEMICeu/iso-19139-to-dcat-ap/master/iso-19139-to-dcat-ap.xsl"

        if xsl.startswith("http://") or xsl.startswith("https://"):
            with urlopen(xsl) as response:
                xsl_string = response.read()
        else:
            with open(xsl, mode="rb") as xsl_file:
                xsl_string = xsl_file.read()

        xml = fromstring(xml_string)
        xsl = XML(xsl_string)
        transform = XSLT(xsl)
        return tostring(transform(xml), pretty_print=True).decode("utf-8")

    def _merge_bounding_boxes(self, bboxes):
        lats = [lat for bbox in bboxes for lat, lon in bbox]
        lons = [lon for bbox in bboxes for lat, lon in bbox]
        min_lat, max_lat = min(lats), max(lats)
        min_lon, max_lon = min(lons), max(lons)
        return [[min_lat, max_lon], [max_lat, max_lon], [max_lat, min_lon], [min_lat, min_lon], [min_lat, max_lon]]

    def _bbox_to_geojson(self, bbox):
        polygon = [bbox]
        multi_polygon = [polygon]
        geojson = {
            "type": "MultiPolygon",
            "coordinates": multi_polygon
        }
        return json.dumps(geojson)

    def get_record_as_rdf(self, record_id):
        r = requests.get('{0}/{1}/formatters/xml'.format(self.base_url, record_id)).content
        if r:
            return self._transform_ISO_19139_dcat(r)
        else:
            log.debug(f'No record found for given ID: {record_id}')
            return None

    def query_top_level_dcat_properties(self, graph, ckan_property):
        dct_prop = self.dcat_ckan_mapping[ckan_property]
        sparql = """
                  PREFIX dct: <http://purl.org/dc/terms/>
                  PREFIX dcat: <http://www.w3.org/ns/dcat#>
                  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                  PREFIX prov: <http://www.w3.org/ns/prov#>
                  PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>

                  SELECT ?o WHERE {
                    ?dataset a dcat:Dataset ;
                            %(dct_prop)s ?o .
                  }""" % {'dct_prop': dct_prop}

        results = graph.query(sparql)
        if results:
            results = [row[0].toPython() for row in results]
            return results[0]
        else:
            return ''

    def get_temporal(self, graph):
        sparql = """
                PREFIX dcat: <http://www.w3.org/ns/dcat#>

                SELECT ?start_date ?end_date
                WHERE {
                  ?period dcat:startDate ?start_date .
                  ?period dcat:endDate ?end_date .
                  }
                """
        results = graph.query(sparql)
        if results:
            start_date = [row[0].toPython() for row in results]
            start_date = start_date[0].strftime('%Y-%m-%d')
            end_date = [row[1].toPython() for row in results]
            end_date = end_date[0].strftime('%Y-%m-%d')
            return start_date, end_date
        else:
            return None, None

    def get_spatial(self, graph):
        sparql = """
                PREFIX dcat: <http://www.w3.org/ns/dcat#>

                SELECT ?bbox
                WHERE {
                  ?a dct:spatial  ?spatial .
                  ?spatial dcat:bbox ?bbox .
                  FILTER(datatype(?bbox) = <https://www.iana.org/assignments/media-types/application/vnd.geo+json>)
                  }
                """
        results = graph.query(sparql)
        if results:
            bbox_list = [bbox.toPython() for (bbox,) in results]
            merge_bboxes = self._merge_bounding_boxes([ast.literal_eval(str(bbox))['coordinates'][0] for bbox in bbox_list])
            convert_to_geojson = self._bbox_to_geojson(merge_bboxes)
            return convert_to_geojson
        else:
            return

    def get_keywords(self, graph):
        keywords = []
        sparql = """
          SELECT ?keyword WHERE {
            ?dataset dcat:theme ?theme .
            ?theme skos:prefLabel ?keyword .
          }
      """
        results = graph.query(sparql)
        if results:
            for row in results:
                tag = ''.join(filter(lambda c: c.isalnum() or c.isspace(), str(row['keyword'])))
                keywords.append({"name": tag})
            return keywords
        else:
            return

    def get_conforms_to(self, graph):
        sparql = """
          SELECT ?epsg WHERE {
            ?dataset a dcat:Dataset .
            ?dataset dct:conformsTo ?epsg .
          }
      """
        results = graph.query(sparql)
        if results:
            return [row[0].toPython() for row in results][0]
        else:
            return

    def get_resources(self, graph, package_id):
        sparql = """
          SELECT ?title ?url ?capabilities WHERE {
            ?dist a dcat:Distribution .
            ?dist dcat:accessService ?s .
            ?s dct:title ?title .
            ?s dcat:endpointURL ?url .
            ?s dcat:endpointDescription  ?capabilities
          }
      """
        results = graph.query(sparql)
        if results:
            resources = [row for row in results]

            resources_list = []
            resource_dict = {}

            for resource in resources:
                title, url, capabilities = resource[0].toPython(), resource[1].toPython(), resource[2].toPython()
                resource_dict['package_id'] = package_id
                resource_dict['title'] = title
                resource_dict['url'] = capabilities
                # resource_dict['capabilities'] = capabilities
                resources_list.append(resource_dict)
            return resources_list
        else:
            return

    def get_contact(self, graph):
        sparql = """
          SELECT ?name ?url ?email WHERE {
            ?dataset a dcat:Dataset .
            ?dataset dcat:contactPoint ?cp .
            ?cp vcard:fn ?name .
            ?cp vcard:hasURL ?url .
            ?cp vcard:hasEmail ?email
          }
        """
        sparql_dataset_has_no_cp = """
          SELECT ?name ?url ?email WHERE {
            ?a dcat:contactPoint ?cp .
            ?cp vcard:fn ?name .
            ?cp vcard:hasEmail ?email .
          }
        """
        results = graph.query(sparql)
        contact_dict = {
            'name': '',
            'url': '',
            'email': ''

        }
        if results:
            contact_data = [row for row in results]
            name, url, email = contact_data[0][0].toPython(), contact_data[0][1].toPython(), contact_data[0][2].toPython()
            contact_dict['name'] = name
            contact_dict['url'] = url
            contact_dict['email'] = email
        else:
            results = graph.query(sparql_dataset_has_no_cp)
            if results:
                contact_data = [row for row in results]
                name, url, email = contact_data[0][0].toPython(), '', contact_data[0][2].toPython()
                contact_dict['name'] = name
                contact_dict['url'] = url
                contact_dict['email'] = email

        return contact_dict


    def get_spatial_res(self, graph):
        sparql = """
          SELECT ?res WHERE {
            ?dataset a dcat:Dataset .
            ?dataset dcat:spatialResolutionInMeters ?res .
          }
      """
        results = graph.query(sparql)

        if results:
            return [int(row[0].toPython()) for row in results][0]

        # results = [int(row[0].toPython()) for row in graph.query(sparql)][0]
        else:
            return

    def get_ckan_package_dict_from_geonetwork_rdf(self, record_id):
        geonetwork_rdf = self.get_record_as_rdf(record_id)

        if geonetwork_rdf is not None:
            # Define the DCMI namespace
            DCMI = Namespace("http://purl.org/dc/terms/")

            # Load the ckan RDF DCAT template
            g1 = Graph()
            g1.parse(data=self.ckan_rdf_template, format='xml')
            # Bind the "dct" prefix to the DCMI namespace
            g1.bind("dct", DCMI)

            # Load the geonetwork RDF DCAT file
            g2 = Graph()
            g2.parse(data=geonetwork_rdf, format='xml')
            # Bind the "dct" prefix to the DCMI namespace
            g2.bind("dct", DCMI)

            #### CREATE CKAN PACKAGE_DICT

            package_dict = {}

            # Get general metadata:
            for k, v in self.dcat_ckan_mapping.items():
                package_dict[k] = self.query_top_level_dcat_properties(g2, ckan_property=k)

            package_dict['name'] = self.ckan_instance.create_ckan_compliant_url_from_name(package_dict['id'])


            # Remove german special characters
            package_dict['title'] = package_dict['title'].replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace(
                "ß", "ss").replace("²", "2").replace("–", "-")
            package_dict['notes'] = package_dict['notes'].replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace(
                "ß", "ss").replace("²", "2").replace("–", "-")

            # Update temporal coverage fields:
            start_date, end_date = self.get_temporal(g2)

            if start_date is not None:
                package_dict['temporal_start'] = start_date
                package_dict['temporal_end'] = end_date

            # Get Spatial properties:
            package_dict['spatial'] = self.get_spatial(g2)

            # Get Keywords
            package_dict['tags'] = self.get_keywords(g2)

            # Get conforms to:
            package_dict['conforms_to'] = self.get_conforms_to(g2)
            regex = re.compile(r'http://www\.opengis\.net/def/crs/EPSG/')
            if package_dict['conforms_to'] and not regex.search(package_dict['conforms_to']):
                package_dict['conforms_to'] = ''


            # Get resources:
            package_dict['resources'] = self.get_resources(g2, package_dict['id'])

            # Get contact data
            contact_dict = self.get_contact(g2)
            package_dict['contact_name'] = contact_dict['name']
            package_dict['contact_uri'] = contact_dict['url']
            package_dict['owner_org'] = 'test'

            # Get inspire theme:
            package_dict['theme'] = ["http://inspire.ec.europa.eu/theme/ef"]

            # Get spatial resolution:
            package_dict['spatial_resolution'] = self.get_spatial_res(g2)

            # Update metadata create and update dates:
            package_dict['metadata_created'] = package_dict['metadata_created'].strftime("%Y-%m-%dT%H:%M:%S.%f") if \
            package_dict['metadata_created'] else ''
            package_dict['metadata_modified'] = package_dict['metadata_modified'].strftime("%Y-%m-%dT%H:%M:%S.%f") if \
            package_dict['metadata_modified'] else ''

            # Remove unused attributes:
            package_dict = {k: v for k, v in package_dict.items() if v}

            return package_dict
        else:
            log.info(f'No package created because no Geonetwork record found for given ID')
            return
