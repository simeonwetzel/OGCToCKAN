# OGCToCKAN Harvester
Scalable Harvester to transfer geospatial (meta-)datasets from Geoserver to CKAN
## Features:
- Implements OWSlib https://geopython.github.io/OWSLib/
- Puts together metadatasets from multiple sources (mainly OWS) and creates a CKAN-dataset compliant to GeoDCAT-AP schema
- Harvesting of metadata from external resource (excel-workbook)
- Upload of local files as additional CKAN-resources


## Metadata information that can be taken from getCapabilities request:
- Geoserver-datastore
- title
- abstract
- CRS (conforms to)
- BBOX
- temporal start/ temporal end
- contact name / Contact email <- providor information from geoserver
- resources (wcs/wfs/wms getCapabilities url-request)
