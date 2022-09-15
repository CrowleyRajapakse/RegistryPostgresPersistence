CREATE TABLE API_ARTIFACTS (
	id serial NOT NULL PRIMARY KEY,
	org VARCHAR(100) NOT NULL,
	uuid  VARCHAR(100) NOT NULL,
	artefact jsonb NOT NULL,
	apiDefinition bytea,
	mediaType VARCHAR(100),
	wsdlDefinition bytea,
	wsdlMediaType VARCHAR(100),
	thumbnail bytea,
	thumbnailMediaType VARCHAR(100)
);

CREATE TABLE AM_API_DOCUMENT (
	id serial NOT NULL PRIMARY KEY,
	org VARCHAR(100) NOT NULL,
	apiUUID  VARCHAR(100) NOT NULL,
	docUUID  VARCHAR(100) NOT NULL,
	doc bytea,
	docContent tsvector
);

CREATE TABLE AM_API_DOCUMENT_METADATA (
	docName  VARCHAR(100) NOT NULL,
	summary  VARCHAR(100),
	docType  VARCHAR(100) NOT NULL,
	docSourceType VARCHAR(100) NOT NULL,
	docSourceUrl VARCHAR(100),
	visibility  VARCHAR(100)
) INHERITS (AM_API_DOCUMENT);