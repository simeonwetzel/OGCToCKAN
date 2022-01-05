# OGCToCKAN Harvester
Scalable Harvester to transfer geospatial (meta-)datasets from OWS provided e.g. by Geoserver to CKAN using OWSlib and CKAN-API. This application was developed as a result of special requirements in the research project [KlimaKonform](https://klimakonform.uw.tu-dresden.de/). 
## Features:
- Implements [OWSlib](https://geopython.github.io/OWSLib/)
- Puts together metadatasets from multiple sources (mainly OWS) and creates a CKAN-dataset compliant to GeoDCAT-AP schema
- Implements additional harvesting of metadata from external resource (excel-workbook)
- Additional feature (due to specific requirements of the research project): 
  - search for files on ftp-server 
  - for each file looking based on filename if there is an matching CKAN dataset
  - uploading selected files as CKAN dataset resource


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

