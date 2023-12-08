mapping = {
    "title": "dct:title",
    "notes": "dct:description",
    "id": "dct:identifier",
    "license_id": "dct:license",
    "tags": "dcat:keyword",
    "url": "foaf:page",
    "version": "dcat:version",
    "spatial": "locn:geometry",
    "temporal_start": "dcat:startDate",
    "temporal_end": "dcat:endDate",
    "temporal_resolution": "dcat:temporalResolution",
    "spatial_resolution": "dcat:spatialResolutionInMeters",
    "spatial_resolution_type": "dcat:spatialResolution",
    "conforms_to": "dcat:conformsTo",
    "was_derived_from": "prov:wasDerivedFrom",
    "is_version_of": "dct:isVersionOf",
    "related_resource": "dcat:relatedResource",
    "alternate_identifier": "dct:alternative",
    "organization": "foaf:member",
    "author": "dct:creator",
    "author_email": "foaf:mbox",
    "maintainer": "foaf:member",
    "maintainer_email": "foaf:mbox",
    "contact_name": "vcard:fn",
    "contact_uri": "vcard:hasURL",
    "relationships_as_object": "prov:wasGeneratedBy",
    "relationships_as_subject": "prov:generated",
    "num_resources": "dcat:distribution",
    "resources": "dcat:distribution",
    "private": "dcat:accessRights",
    "isopen": "dcat:accessRights",
    "state": "dct:accrualMethod",
    "creator_user_id": "foaf:member",
    "metadata_created": "dct:created",
    "metadata_modified": "dct:modified",
    }

ckan_rdf = """<?xml version="1.0" encoding="utf-8"?>
<rdf:RDF
  xmlns:foaf="http://xmlns.com/foaf/0.1/"
  xmlns:locn="http://www.w3.org/ns/locn#"
  xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  xmlns:dcat="http://www.w3.org/ns/dcat#"
  xmlns:dct="http://purl.org/dc/terms/"
  xmlns:schema="http://schema.org/"
  xmlns:vcard="http://www.w3.org/2006/vcard/ns#"
  xmlns:adms="http://www.w3.org/ns/adms#"
>
  <dcat:Dataset rdf:about="http://172.26.62.26/dataset/6582c58e-f386-47e4-801f-5a3b92fdbc03">
    <dcat:keyword>tag1</dcat:keyword>
    <dcat:landingPage rdf:resource="https://website.de"/>
    <dct:description>description</dct:description>
    <dct:conformsTo>http://www.opengis.net/def/crs/EPSG/0/31468</dct:conformsTo>
    <dcat:temporalResolution rdf:datatype="http://www.w3.org/2001/XMLSchema#duration">P1D</dcat:temporalResolution>
    <dcat:contactPoint>
      <vcard:Organization rdf:about="https://orcid.de">
        <vcard:fn>c name</vcard:fn>
      </vcard:Organization>
    </dcat:contactPoint>
    <dcat:theme rdf:resource="http://inspire.ec.europa.eu/theme/af"/>
    <dct:temporal>
      <dct:PeriodOfTime rdf:nodeID="Nfbb8b3ab78dd4336a8adf36fc0fd941a">
        <dcat:startDate rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">1111-11-11T00:00:00</dcat:startDate>
        <dcat:endDate rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">1111-11-11T00:00:00</dcat:endDate>
      </dct:PeriodOfTime>
    </dct:temporal>
    <dct:temporal>
      <dct:PeriodOfTime rdf:nodeID="N42056d31648f489c9ab48c13fe8dfd77">
        <schema:endDate rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">1111-11-11T00:00:00</schema:endDate>
        <schema:startDate rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">1111-11-11T00:00:00</schema:startDate>
      </dct:PeriodOfTime>
    </dct:temporal>
    <dct:spatial>
      <dct:Location rdf:nodeID="N001036bc11114b8a9edaa30766ad9ca2">
        <locn:geometry rdf:datatype="http://www.opengis.net/ont/geosparql#wktLiteral">MULTIPOLYGON (((-11.6016 17.3171, -11.6016 49.3881, 35.5078 49.3881, 35.5078 17.3171, -11.6016 17.3171)))</locn:geometry>
        <locn:geometry rdf:datatype="https://www.iana.org/assignments/media-types/application/vnd.geo+json">{"type":"MultiPolygon","coordinates":[[[[-11.6015625,17.317081823949536],[-11.6015625,49.3880962970685],[35.5078125,49.3880962970685],[35.5078125,17.317081823949536],[-11.6015625,17.317081823949536]]]]}</locn:geometry>
      </dct:Location>
    </dct:spatial>
    <dcat:distribution>
      <dcat:Distribution rdf:about="http://172.26.62.26/dataset/6582c58e-f386-47e4-801f-5a3b92fdbc03/resource/e69a0ffc-5c6d-48e6-a003-f981ae40eae6">
        <dct:modified rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2023-02-16T12:34:34.198988</dct:modified>
        <dct:title>https://documentation.de</dct:title>
        <dct:issued rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2023-02-16T12:34:34.207125</dct:issued>
      </dcat:Distribution>
    </dcat:distribution>
    <dct:identifier>6582c58e-f386-47e4-801f-5a3b92fdbc03</dct:identifier>
    <dct:issued rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2023-02-16T12:34:24.745408</dct:issued>
    <dct:title>Title</dct:title>
    <dct:publisher>
      <foaf:Organization rdf:about="http://172.26.62.26/organization/c6213598-b450-43a3-9f09-557411d46516">
        <foaf:name>Test</foaf:name>
      </foaf:Organization>
    </dct:publisher>
    <adms:identifier rdf:resource="https://doi.de"/>
    <dct:modified rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2023-02-16T12:34:34.605991</dct:modified>
    <foaf:page rdf:resource="https://documentation.de"/>
  </dcat:Dataset>
</rdf:RDF>
"""