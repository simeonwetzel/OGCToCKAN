# OGCToCKAN Harvester
Scalable Harvester to transfer geospatial (meta-)datasets from Geoserver to CKAN using OWSlib and CKAN-API
## Features:
- Implements OWSlib https://geopython.github.io/OWSLib/
- Puts together metadatasets from multiple sources (mainly OWS) and creates a CKAN-dataset compliant to GeoDCAT-AP schema
- Harvesting of metadata from external resource (excel-workbook)
- Upload of local files as additional CKAN-resources


### Metadata fields that can be taken from getCapabilities request:
- Geoserver-datastore
- title
- abstract
- CRS (conforms to)
- BBOX
- temporal start/ temporal end
- contact name / Contact email <- providor information from geoserver
- resources (wcs/wfs/wms getCapabilities url-request)
- keywords


### Metadata fields to be collected from external resources (excel):
- alternate title
- CKAN-Organistation (if None -> default: Geoserver-datastore)
- alternate abstract
- local file resource (path)
- documentation (link to publication)
- contact-name
- contact-uri
- dataset DOI
- licence-ID
- spatial-resolution
- spatial-resolution-type (unit of measurement)
- temporal start/ temporal end
- temporal resolution
- keyword-tags
- is version of
- related resource
- derived from

