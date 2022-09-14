package org.wso2.apk.extensions.persistence.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.apk.extensions.persistence.postgres.utils.HikariCPDataSource;
import org.wso2.apk.extensions.persistence.postgres.utils.PostgresDBConnectionUtil;
import org.wso2.carbon.apimgt.api.model.API;
import org.wso2.carbon.apimgt.api.model.Tag;
import org.wso2.carbon.apimgt.persistence.APIConstants;
import org.wso2.carbon.apimgt.persistence.APIPersistence;
import org.wso2.carbon.apimgt.persistence.dto.*;
import org.wso2.carbon.apimgt.persistence.exceptions.*;
import org.wso2.carbon.apimgt.persistence.mapper.APIMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.wso2.carbon.apimgt.persistence.utils.PublisherAPISearchResultComparator;
import org.wso2.carbon.apimgt.persistence.utils.RegistryPersistenceDocUtil;
import org.wso2.carbon.apimgt.persistence.utils.RegistryPersistenceUtil;
import org.wso2.carbon.registry.core.Resource;
import org.wso2.carbon.registry.core.session.UserRegistry;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


public class PostgresDBPersistentImpl implements APIPersistence {

    private static final Log log = LogFactory.getLog(PostgresDBPersistentImpl.class);

    @Override
    public PublisherAPI addAPI(Organization organization, PublisherAPI publisherAPI) throws APIPersistenceException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String addAPIQuery = "INSERT INTO API_ARTIFACTS (org, uuid, artefact, apiDefinition, mediaType) VALUES(?,?,to_json(?::json),?,?);";
        API api = APIMapper.INSTANCE.toApi(publisherAPI);
        String uuid = UUID.nameUUIDFromBytes(api.getId().getApiName().getBytes()).toString();
        String json = "";
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            json = ow.writeValueAsString(publisherAPI);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(addAPIQuery);
            preparedStatement.setString(1, organization.getName());
            preparedStatement.setString(2, uuid);
            preparedStatement.setString(3, json);
            if (api.getSwaggerDefinition() != null) {
                byte[] apiDefinitionBytes = api.getSwaggerDefinition().getBytes();
                preparedStatement.setBinaryStream(4, new ByteArrayInputStream(apiDefinitionBytes));
                preparedStatement.setString(5, "swagger.json");
            }
            preparedStatement.executeUpdate();
            connection.commit();

        } catch (SQLException e) {
            PostgresDBConnectionUtil.rollbackConnection(connection,"add api");
            if (log.isDebugEnabled()) {
                log.debug("Error occurred while adding entry to API_ARTIFACTS table ", e);
            }
            handleException("Error while persisting entry to API_ARTIFACTS table ", e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, null);
        }
        api.setUuid(uuid);
        api.setCreatedTime(String.valueOf(new Date().getTime()));// set current time as created time for returning api.
        PublisherAPI returnAPI = APIMapper.INSTANCE.toPublisherApi(api);
        return returnAPI;
    }

    private void handleException(String message, Throwable e) throws APIPersistenceException {

        throw new APIPersistenceException(message, e);
    }

    @Override
    public String addAPIRevision(Organization organization, String s, int i) throws APIPersistenceException {
        return null;
    }

    @Override
    public void restoreAPIRevision(Organization organization, String s, String s1, int i) throws APIPersistenceException {

    }

    @Override
    public void deleteAPIRevision(Organization organization, String s, String s1, int i) throws APIPersistenceException {

    }

    @Override
    public PublisherAPI updateAPI(Organization organization, PublisherAPI publisherAPI) throws APIPersistenceException {
        return null;
    }

    @Override
    public PublisherAPI getPublisherAPI(Organization organization, String apiUUID) throws APIPersistenceException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        PublisherAPI publisherAPI = null;
        String getAPIArtefactQuery = "SELECT artefact from API_ARTIFACTS WHERE org=? AND uuid=?;";

        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(getAPIArtefactQuery);
            preparedStatement.setString(1, organization.getName());
            preparedStatement.setString(2, apiUUID);
            resultSet = preparedStatement.executeQuery();
            connection.commit();
            while (resultSet.next()) {
                String json = resultSet.getString(1);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode tree = mapper.readTree(json);
                publisherAPI = mapper.treeToValue(tree, PublisherAPI.class);
            }
        } catch (SQLException e) {
            if (log.isDebugEnabled()) {
                log.debug("Error while retrieving api artefact for API uuid: " + apiUUID);
            }
            handleException("Error while retrieving api artefact for API uuid: " + apiUUID, e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, resultSet);
        }
        return publisherAPI;
    }

    @Override
    public DevPortalAPI getDevPortalAPI(Organization organization, String s) throws APIPersistenceException {
        return null;
    }

    @Override
    public void deleteAPI(Organization organization, String apiUUID) throws APIPersistenceException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String deleteAPIQuery = "DELETE FROM api_artifacts WHERE org=? AND uuid=?;";

        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(deleteAPIQuery);
            preparedStatement.setString(1, organization.getName());
            preparedStatement.setString(2, apiUUID);
            preparedStatement.executeUpdate();
            connection.commit();

        } catch (SQLException e) {
            PostgresDBConnectionUtil.rollbackConnection(connection,"delete api");
            if (log.isDebugEnabled()) {
                log.debug("Error occurred while deleting entry from API_ARTIFACTS table ", e);
            }
            handleException("Error occurred while deleting entry from API_ARTIFACTS table ", e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, null);
        }

    }

    @Override
    public void deleteAllAPIs(Organization organization) throws APIPersistenceException {

    }

    @Override
    public PublisherAPISearchResult searchAPIsForPublisher(Organization organization, String searchQuery, int start,
                                                           int offset, UserContext ctx, String sortBy, String sortOrder)
            throws APIPersistenceException {
        PublisherAPISearchResult result = null;
        String searchAllQuery = "SELECT artefact,uuid FROM api_artifacts WHERE org=?;";

        if (StringUtils.isEmpty(searchQuery)) {
            result = searchPaginatedPublisherAPIs(organization.getName(), searchAllQuery, start, offset);
        }
        return result;
    }

    private PublisherAPISearchResult searchPaginatedPublisherAPIs(String org, String searchQuery, int start,
                                                                  int offset) throws APIPersistenceException {

        int totalLength = 0;
        PublisherAPISearchResult searchResults = new PublisherAPISearchResult();
        PublisherAPI publisherAPI;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(searchQuery);
            preparedStatement.setString(1, org);
            resultSet = preparedStatement.executeQuery();
            connection.commit();
            List<PublisherAPIInfo> publisherAPIInfoList = new ArrayList<>();
            while (resultSet.next()) {
                String json = resultSet.getString(1);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode tree = mapper.readTree(json);
                publisherAPI = mapper.treeToValue(tree, PublisherAPI.class);
                PublisherAPIInfo apiInfo = new PublisherAPIInfo();
                apiInfo.setType(publisherAPI.getType());
                apiInfo.setId(resultSet.getString(2));
                apiInfo.setApiName(publisherAPI.getApiName());
                apiInfo.setDescription(publisherAPI.getDescription());
                apiInfo.setContext(publisherAPI.getContext());
                apiInfo.setProviderName(publisherAPI.getProviderName());
                apiInfo.setStatus(publisherAPI.getStatus());
                apiInfo.setThumbnail(publisherAPI.getThumbnail());
                apiInfo.setVersion(publisherAPI.getVersion());
                apiInfo.setAudience(publisherAPI.getAudience());
                apiInfo.setCreatedTime(publisherAPI.getCreatedTime());
                apiInfo.setUpdatedTime(publisherAPI.getUpdatedTime());
                publisherAPIInfoList.add(apiInfo);
                totalLength ++;
            }
            Collections.sort(publisherAPIInfoList, new PublisherAPISearchResultComparator());
            searchResults.setPublisherAPIInfoList(publisherAPIInfoList);
            searchResults.setReturnedAPIsCount(publisherAPIInfoList.size());
            searchResults.setTotalAPIsCount(totalLength);
        } catch (SQLException e) {
            if (log.isDebugEnabled()) {
                log.debug("Error while retrieving api artefacts");
            }
            handleException("Error while retrieving api artefacts", e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, resultSet);
        }
        return searchResults;
    }

    @Override
    public DevPortalAPISearchResult searchAPIsForDevPortal(Organization organization, String s, int i, int i1, UserContext userContext) throws APIPersistenceException {
        return null;
    }

    @Override
    public PublisherContentSearchResult searchContentForPublisher(Organization org, String searchQuery, int i, int i1, UserContext userContext) throws APIPersistenceException {
        int totalLength = 0;
        PublisherContentSearchResult searchResults = new PublisherContentSearchResult();
        PublisherAPI publisherAPI;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        PreparedStatement preparedStatementDoc = null;
        ResultSet resultSet = null;
        ResultSet resultSetDoc = null;

        String searchContentQuery = "SELECT DISTINCT artefact,uuid FROM api_artifacts JOIN jsonb_each_text(artefact) e ON true \n" +
                " WHERE org=? AND e.value LIKE ?;";

        String modifiedSearchQuery = "%" + searchQuery.substring(8) +"%";

        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(searchContentQuery);
            preparedStatement.setString(1, org.getName());
            preparedStatement.setString(2, modifiedSearchQuery);
            resultSet = preparedStatement.executeQuery();
            connection.commit();
            List<SearchContent> contentData = new ArrayList<>();
            while (resultSet.next()) {
                String json = resultSet.getString(1);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode tree = mapper.readTree(json);
                publisherAPI = mapper.treeToValue(tree, PublisherAPI.class);
                PublisherSearchContent content = new PublisherSearchContent();
                content.setContext(publisherAPI.getContext());
                content.setDescription(publisherAPI.getDescription());
                content.setId(resultSet.getString(2));
                content.setName(publisherAPI.getApiName());
                content.setProvider(publisherAPI.getProviderName());
                content.setType(APIConstants.API);
                content.setVersion(publisherAPI.getVersion());
                content.setStatus(publisherAPI.getStatus());
                contentData.add(content);
                totalLength ++;
            }

            // Adding doc search
            String docSearchQuery = "SELECT ad.apiUUID, ad.docUUID, ar.api_name, ar.api_version, ar.api_provider, ar.api_type, ad.docName, ad.docType, ad.docSourceType, ad.visibility FROM AM_API_DOCUMENT_METADATA ad, AM_API ar WHERE ad.org=? AND ad.docContent @@ to_tsquery(?) AND ad.apiUUID=ar.api_uuid;";
            String modifiedDocQuery = "";
            if (searchQuery.substring(8).split(" ").length <= 1) {
                modifiedDocQuery = searchQuery.substring(8);
            } else {
                modifiedDocQuery = searchQuery.substring(8).replace(" "," & ");
            }
            preparedStatementDoc = connection.prepareStatement(docSearchQuery);
            preparedStatementDoc.setString(1, org.getName());
            preparedStatementDoc.setString(2, modifiedDocQuery);
            resultSetDoc = preparedStatementDoc.executeQuery();
            connection.commit();
            while (resultSetDoc.next()) {
                DocumentSearchContent docSearch = new DocumentSearchContent();
                String apiUUID = resultSetDoc.getString("apiUUID");
                String docUUID = resultSetDoc.getString("docUUID");
                String apiType = resultSetDoc.getString("api_type");
                String accociatedType;
                if (apiType.
                        equals(APIConstants.AuditLogConstants.API_PRODUCT)) {
                    accociatedType = APIConstants.API_PRODUCT;
                } else {
                    accociatedType = APIConstants.API;
                }
                String docType = resultSetDoc.getString("docType");
                DocumentationType type;
                if (docType.equalsIgnoreCase(DocumentationType.HOWTO.getType())) {
                    type = DocumentationType.HOWTO;
                } else if (docType.equalsIgnoreCase(DocumentationType.PUBLIC_FORUM.getType())) {
                    type = DocumentationType.PUBLIC_FORUM;
                } else if (docType.equalsIgnoreCase(DocumentationType.SUPPORT_FORUM.getType())) {
                    type = DocumentationType.SUPPORT_FORUM;
                } else if (docType.equalsIgnoreCase(DocumentationType.API_MESSAGE_FORMAT.getType())) {
                    type = DocumentationType.API_MESSAGE_FORMAT;
                } else if (docType.equalsIgnoreCase(DocumentationType.SAMPLES.getType())) {
                    type = DocumentationType.SAMPLES;
                } else {
                    type = DocumentationType.OTHER;
                }

                String visibilityAttr = resultSetDoc.getString("visibility");
                Documentation.DocumentVisibility documentVisibility = Documentation.DocumentVisibility.API_LEVEL;

                if (visibilityAttr != null) {
                    if (visibilityAttr.equals(Documentation.DocumentVisibility.API_LEVEL.name())) {
                        documentVisibility = Documentation.DocumentVisibility.API_LEVEL;
                    } else if (visibilityAttr.equals(Documentation.DocumentVisibility.PRIVATE.name())) {
                        documentVisibility = Documentation.DocumentVisibility.PRIVATE;
                    } else if (visibilityAttr.equals(Documentation.DocumentVisibility.OWNER_ONLY.name())) {
                        documentVisibility = Documentation.DocumentVisibility.OWNER_ONLY;
                    }
                }

                Documentation.DocumentSourceType docSourceType = Documentation.DocumentSourceType.INLINE;
                String artifactAttribute = resultSetDoc.getString("docSourceType");

                if (Documentation.DocumentSourceType.URL.name().equals(artifactAttribute)) {
                    docSourceType = Documentation.DocumentSourceType.URL;
                } else if (Documentation.DocumentSourceType.FILE.name().equals(artifactAttribute)) {
                    docSourceType = Documentation.DocumentSourceType.FILE;
                } else if (Documentation.DocumentSourceType.MARKDOWN.name().equals(artifactAttribute)) {
                    docSourceType = Documentation.DocumentSourceType.MARKDOWN;
                }
                docSearch.setApiName(resultSetDoc.getString("api_name"));
                docSearch.setApiProvider(resultSetDoc.getString("api_provider"));
                docSearch.setApiVersion(resultSetDoc.getString("api_version"));
                docSearch.setApiUUID(apiUUID);
                docSearch.setAssociatedType(accociatedType);
                docSearch.setDocType(type);
                docSearch.setId(docUUID);
                docSearch.setSourceType(docSourceType);
                docSearch.setVisibility(documentVisibility);
                docSearch.setName(resultSetDoc.getString("docName"));
                contentData.add(docSearch);
                totalLength ++;
            }
            searchResults.setResults(contentData);
            searchResults.setReturnedCount(contentData.size());
            searchResults.setTotalCount(totalLength);

        } catch (SQLException e) {
            if (log.isDebugEnabled()) {
                log.debug("Error while content searching api artefacts");
            }
            handleException("Error while content searching api artefacts", e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, resultSet);
        }
        return searchResults;
    }

    @Override
    public DevPortalContentSearchResult searchContentForDevPortal(Organization organization, String s, int i, int i1, UserContext userContext) throws APIPersistenceException {
        return null;
    }

    @Override
    public void changeAPILifeCycle(Organization organization, String s, String s1) throws APIPersistenceException {

    }

    @Override
    public void saveWSDL(Organization organization, String s, ResourceFile resourceFile) throws WSDLPersistenceException {

    }

    @Override
    public ResourceFile getWSDL(Organization organization, String s) throws WSDLPersistenceException {
        return null;
    }

    @Override
    public void saveOASDefinition(Organization organization, String apiId, String apiDefinition) throws OASPersistenceException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String saveOASQuery = "UPDATE API_ARTIFACTS SET apiDefinition=?,mediaType=? WHERE org=? AND uuid=?;";
        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(saveOASQuery);
            if (apiDefinition != null) {
                byte[] apiDefinitionBytes = apiDefinition.getBytes();
                preparedStatement.setBinaryStream(1, new ByteArrayInputStream(apiDefinitionBytes));
                preparedStatement.setString(2, "swagger.json");
            }
            preparedStatement.setString(3, organization.getName());
            preparedStatement.setString(4, apiId);
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            PostgresDBConnectionUtil.rollbackConnection(connection,"Save OAS definition");
            if (log.isDebugEnabled()) {
                log.debug("Error occurred while updating entry in API_ARTIFACTS table ", e);
            }
            throw new OASPersistenceException("Error while updating entry in API_ARTIFACTS table ", e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, null);
        }
    }

    @Override
    public String getOASDefinition(Organization organization, String apiId) throws OASPersistenceException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String oasDefinition = null;
        String getOASDefinitionQuery = "SELECT apiDefinition,mediaType from API_ARTIFACTS WHERE org=? AND uuid=?;";
        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(getOASDefinitionQuery);
            preparedStatement.setString(1, organization.getName());
            preparedStatement.setString(2, apiId);
            resultSet = preparedStatement.executeQuery();
            connection.commit();
            while (resultSet.next()) {
                String mediaType = resultSet.getString("mediaType");
                InputStream apiDefinitionBlob = resultSet.getBinaryStream("apiDefinition");
                if (apiDefinitionBlob != null) {
                    oasDefinition = PostgresDBConnectionUtil.getStringFromInputStream(apiDefinitionBlob);
                }
            }
        } catch (SQLException e) {
            if (log.isDebugEnabled()) {
                log.debug("Error while retrieving oas definition for api uuid: " + apiId);
            }
            throw new OASPersistenceException("Error while retrieving oas definition for api uuid: " + apiId, e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, resultSet);
        }
        return oasDefinition;
    }

    @Override
    public void saveAsyncDefinition(Organization organization, String s, String s1) throws AsyncSpecPersistenceException {

    }

    @Override
    public String getAsyncDefinition(Organization organization, String s) throws AsyncSpecPersistenceException {
        return null;
    }

    @Override
    public void saveGraphQLSchemaDefinition(Organization organization, String s, String s1) throws GraphQLPersistenceException {

    }

    @Override
    public String getGraphQLSchema(Organization organization, String s) throws GraphQLPersistenceException {
        return null;
    }

    @Override
    public Documentation addDocumentation(Organization organization, String apiUUID, Documentation documentation) throws DocumentationPersistenceException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String addDocumentQuery =
                "INSERT INTO AM_API_DOCUMENT_METADATA (org, apiUUID, docUUID, docName, summary, docType, docSourceType, docSourceUrl, visibility) VALUES(?,?,?,?,?,?,?,?,?);";
        String docUUID = UUID.nameUUIDFromBytes((documentation.getName() + apiUUID).getBytes()).toString();

        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(addDocumentQuery);
            preparedStatement.setString(1, organization.getName());
            preparedStatement.setString(2, apiUUID);
            preparedStatement.setString(3, docUUID);
            preparedStatement.setString(4, documentation.getName());
            preparedStatement.setString(5, documentation.getSummary());
            preparedStatement.setString(6, documentation.getType().getType());
            preparedStatement.setString(7, documentation.getSourceType().toString());
            preparedStatement.setString(8, documentation.getSourceUrl());
            preparedStatement.setString(9, documentation.getVisibility().toString());
            preparedStatement.executeUpdate();
            connection.commit();

        } catch (SQLException e) {
            PostgresDBConnectionUtil.rollbackConnection(connection,"add document");
            if (log.isDebugEnabled()) {
                log.debug("Error occurred while adding entry to AM_API_DOCUMENT_METADATA table ", e);
            }
            throw new DocumentationPersistenceException("Error while persisting entry to AM_API_DOCUMENT_METADATA table ", e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, null);
        }
        documentation.setId(docUUID);
        return documentation;
    }

    @Override
    public Documentation updateDocumentation(Organization organization, String s, Documentation documentation) throws DocumentationPersistenceException {
        return null;
    }

    @Override
    public Documentation getDocumentation(Organization organization, String apiUUID, String docUUID) throws DocumentationPersistenceException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Documentation documentation = null;
        String getDocumentQuery = "SELECT org, apiUUID, docUUID, docName, summary, docType, docSourceType, docSourceUrl, visibility from AM_API_DOCUMENT_METADATA WHERE org=? AND apiUUID=? AND docUUID=?;";

        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(getDocumentQuery);
            preparedStatement.setString(1, organization.getName());
            preparedStatement.setString(2, apiUUID);
            preparedStatement.setString(3, docUUID);
            resultSet = preparedStatement.executeQuery();
            connection.commit();
            while (resultSet.next()) {
                String docType = resultSet.getString("docType");
                DocumentationType type;
                if (docType.equalsIgnoreCase(DocumentationType.HOWTO.getType())) {
                    type = DocumentationType.HOWTO;
                } else if (docType.equalsIgnoreCase(DocumentationType.PUBLIC_FORUM.getType())) {
                    type = DocumentationType.PUBLIC_FORUM;
                } else if (docType.equalsIgnoreCase(DocumentationType.SUPPORT_FORUM.getType())) {
                    type = DocumentationType.SUPPORT_FORUM;
                } else if (docType.equalsIgnoreCase(DocumentationType.API_MESSAGE_FORMAT.getType())) {
                    type = DocumentationType.API_MESSAGE_FORMAT;
                } else if (docType.equalsIgnoreCase(DocumentationType.SAMPLES.getType())) {
                    type = DocumentationType.SAMPLES;
                } else {
                    type = DocumentationType.OTHER;
                }
                String docName = resultSet.getString("docName");
                documentation = new Documentation(type, docName);
                documentation.setId(docUUID);
                documentation.setSummary(resultSet.getString("summary"));
                documentation.setSourceUrl(resultSet.getString("docSourceUrl"));
                String visibilityAttr = resultSet.getString("visibility");
                Documentation.DocumentVisibility documentVisibility = Documentation.DocumentVisibility.API_LEVEL;

                if (visibilityAttr != null) {
                    if (visibilityAttr.equals(Documentation.DocumentVisibility.API_LEVEL.name())) {
                        documentVisibility = Documentation.DocumentVisibility.API_LEVEL;
                    } else if (visibilityAttr.equals(Documentation.DocumentVisibility.PRIVATE.name())) {
                        documentVisibility = Documentation.DocumentVisibility.PRIVATE;
                    } else if (visibilityAttr.equals(Documentation.DocumentVisibility.OWNER_ONLY.name())) {
                        documentVisibility = Documentation.DocumentVisibility.OWNER_ONLY;
                    }
                }
                documentation.setVisibility(documentVisibility);

                Documentation.DocumentSourceType docSourceType = Documentation.DocumentSourceType.INLINE;
                String artifactAttribute = resultSet.getString("docSourceType");

                if (Documentation.DocumentSourceType.URL.name().equals(artifactAttribute)) {
                    docSourceType = Documentation.DocumentSourceType.URL;
                    documentation.setSourceUrl(resultSet.getString("docSourceUrl"));
                } else if (Documentation.DocumentSourceType.FILE.name().equals(artifactAttribute)) {
                    docSourceType = Documentation.DocumentSourceType.FILE;
                } else if (Documentation.DocumentSourceType.MARKDOWN.name().equals(artifactAttribute)) {
                    docSourceType = Documentation.DocumentSourceType.MARKDOWN;
                }
                documentation.setSourceType(docSourceType);
            }
        } catch (SQLException e) {
            if (log.isDebugEnabled()) {
                log.debug("Error while retrieving document for doc uuid: " + docUUID);
            }
            throw new DocumentationPersistenceException("Error while retrieving document for doc uuid: " + docUUID, e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, resultSet);
        }
        return documentation;
    }

    @Override
    public DocumentContent getDocumentationContent(Organization organization, String apiUUID, String docUUID) throws DocumentationPersistenceException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        DocumentContent documentContent = null;
        String getDocumentContentQuery = "SELECT docSourceType, doc, docContent, docSourceUrl from AM_API_DOCUMENT_METADATA WHERE org=? AND apiUUID=? AND docUUID=?;";

        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(getDocumentContentQuery);
            preparedStatement.setString(1, organization.getName());
            preparedStatement.setString(2, apiUUID);
            preparedStatement.setString(3, docUUID);
            resultSet = preparedStatement.executeQuery();
            connection.commit();
            while (resultSet.next()) {
                String docSourceType = resultSet.getString("docSourceType");
                InputStream docBlob = resultSet.getBinaryStream("doc");
                String doc = null;
                if (docBlob != null) {
                    doc = PostgresDBConnectionUtil.getStringFromInputStream(docBlob);
                }
                documentContent = new DocumentContent();
                if (StringUtils.equals(docSourceType,Documentation.DocumentSourceType.FILE.toString())) {
                    if (doc != null) {
                        ResourceFile resourceFile = new ResourceFile(docBlob, "PDF");
                        documentContent.setResourceFile(resourceFile);
                        documentContent
                                .setSourceType(DocumentContent.ContentSourceType.valueOf(docSourceType));
                    }
                } else if (StringUtils.equals(docSourceType,Documentation.DocumentSourceType.INLINE.toString())
                        || StringUtils.equals(docSourceType,Documentation.DocumentSourceType.MARKDOWN.toString())) {
                    if (doc != null) {
                        documentContent.setTextContent(doc);
                        documentContent
                                .setSourceType(DocumentContent.ContentSourceType.valueOf(docSourceType));
                    }

                } else if (StringUtils.equals(docSourceType,Documentation.DocumentSourceType.URL.toString())) {

                    String sourceUrl = resultSet.getString("docSourceUrl");
                    documentContent.setTextContent(sourceUrl);
                    documentContent
                            .setSourceType(DocumentContent.ContentSourceType.valueOf(docSourceType));
                }
            }
        } catch (SQLException e) {
            if (log.isDebugEnabled()) {
                log.debug("Error while retrieving document content for doc uuid: " + docUUID);
            }
            throw new DocumentationPersistenceException("Error while retrieving document content for doc uuid: " + docUUID, e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, resultSet);
        }
        return documentContent;
    }

    @Override
    public DocumentContent addDocumentationContent(Organization organization, String apiUUID, String docUUID, DocumentContent documentContent) throws DocumentationPersistenceException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String addDocumentContentQuery = "UPDATE AM_API_DOCUMENT SET doc=?,docContent=to_tsvector(?) WHERE org=? AND apiUUID=? AND docUUID=?;";

        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(addDocumentContentQuery);
            if (documentContent.getResourceFile() != null && documentContent.getResourceFile().getContent() != null) {
                byte[] docByte = documentContent.getResourceFile().getContent().toString().getBytes();
                preparedStatement.setBinaryStream(1, new ByteArrayInputStream(docByte));
            } else {
                preparedStatement.setBinaryStream(1, new ByteArrayInputStream(documentContent.getTextContent().getBytes()));
            }
            preparedStatement.setString(2, documentContent.getTextContent());
            preparedStatement.setString(3, organization.getName());
            preparedStatement.setString(4, apiUUID);
            preparedStatement.setString(5, docUUID);
            preparedStatement.executeUpdate();
            connection.commit();

        } catch (SQLException e) {
            PostgresDBConnectionUtil.rollbackConnection(connection,"add document content");
            if (log.isDebugEnabled()) {
                log.debug("Error occurred while adding entry to AM_API_DOCUMENT table ", e);
            }
            throw new DocumentationPersistenceException("Error while persisting entry to AM_API_DOCUMENT table ", e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, null);
        }
        return documentContent;
    }

    @Override
    public DocumentSearchResult searchDocumentation(Organization org, String apiUUID, int start, int offset,
                                                    String searchQuery, UserContext ctx) throws DocumentationPersistenceException {
        DocumentSearchResult result = new DocumentSearchResult();

        int totalLength = 0;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        String searchAllQuery = "SELECT * FROM AM_API_DOCUMENT_METADATA WHERE org=? and apiUUID=?;";

        try {
            connection = HikariCPDataSource.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(searchAllQuery);
            preparedStatement.setString(1, org.getName());
            preparedStatement.setString(2, apiUUID);
            resultSet = preparedStatement.executeQuery();
            connection.commit();
            List<Documentation> documentationList = new ArrayList<Documentation>();
            Documentation documentation = null;
            while (resultSet.next()) {
                String docType = resultSet.getString("docType");
                DocumentationType type;
                if (docType.equalsIgnoreCase(DocumentationType.HOWTO.getType())) {
                    type = DocumentationType.HOWTO;
                } else if (docType.equalsIgnoreCase(DocumentationType.PUBLIC_FORUM.getType())) {
                    type = DocumentationType.PUBLIC_FORUM;
                } else if (docType.equalsIgnoreCase(DocumentationType.SUPPORT_FORUM.getType())) {
                    type = DocumentationType.SUPPORT_FORUM;
                } else if (docType.equalsIgnoreCase(DocumentationType.API_MESSAGE_FORMAT.getType())) {
                    type = DocumentationType.API_MESSAGE_FORMAT;
                } else if (docType.equalsIgnoreCase(DocumentationType.SAMPLES.getType())) {
                    type = DocumentationType.SAMPLES;
                } else {
                    type = DocumentationType.OTHER;
                }
                String docName = resultSet.getString("docName");
                documentation = new Documentation(type, docName);
                documentation.setId(resultSet.getString("docUUID"));
                documentation.setSummary(resultSet.getString("summary"));
                documentation.setSourceUrl(resultSet.getString("docSourceUrl"));
                String visibilityAttr = resultSet.getString("visibility");
                Documentation.DocumentVisibility documentVisibility = Documentation.DocumentVisibility.API_LEVEL;

                if (visibilityAttr != null) {
                    if (visibilityAttr.equals(Documentation.DocumentVisibility.API_LEVEL.name())) {
                        documentVisibility = Documentation.DocumentVisibility.API_LEVEL;
                    } else if (visibilityAttr.equals(Documentation.DocumentVisibility.PRIVATE.name())) {
                        documentVisibility = Documentation.DocumentVisibility.PRIVATE;
                    } else if (visibilityAttr.equals(Documentation.DocumentVisibility.OWNER_ONLY.name())) {
                        documentVisibility = Documentation.DocumentVisibility.OWNER_ONLY;
                    }
                }
                documentation.setVisibility(documentVisibility);

                Documentation.DocumentSourceType docSourceType = Documentation.DocumentSourceType.INLINE;
                String artifactAttribute = resultSet.getString("docSourceType");

                if (Documentation.DocumentSourceType.URL.name().equals(artifactAttribute)) {
                    docSourceType = Documentation.DocumentSourceType.URL;
                    documentation.setSourceUrl(resultSet.getString("docSourceUrl"));
                } else if (Documentation.DocumentSourceType.FILE.name().equals(artifactAttribute)) {
                    docSourceType = Documentation.DocumentSourceType.FILE;
                } else if (Documentation.DocumentSourceType.MARKDOWN.name().equals(artifactAttribute)) {
                    docSourceType = Documentation.DocumentSourceType.MARKDOWN;
                }
                documentation.setSourceType(docSourceType);
                if (searchQuery != null) {
                    if (searchQuery.toLowerCase().startsWith("name:")) {
                        String requestedDocName = searchQuery.split(":")[1];
                        if (documentation.getName().equalsIgnoreCase(requestedDocName)) {
                            documentationList.add(documentation);
                        }
                    } else {
                        log.warn("Document search not implemented for the query " + searchQuery);
                    }
                } else {
                    documentationList.add(documentation);
                }
                totalLength ++;
            }
            result.setDocumentationList(documentationList);
            result.setTotalDocsCount(totalLength);
            result.setReturnedDocsCount(totalLength);
        } catch (SQLException e) {
            if (log.isDebugEnabled()) {
                log.debug("Error while retrieving documents");
            }
            throw new DocumentationPersistenceException("Error while retrieving documents", e);
        } finally {
            PostgresDBConnectionUtil.closeAllConnections(preparedStatement, connection, resultSet);
        }
        return result;
    }

    @Override
    public void deleteDocumentation(Organization organization, String s, String s1) throws DocumentationPersistenceException {

    }

    @Override
    public Mediation addMediationPolicy(Organization organization, String s, Mediation mediation) throws MediationPolicyPersistenceException {
        return null;
    }

    @Override
    public Mediation updateMediationPolicy(Organization organization, String s, Mediation mediation) throws MediationPolicyPersistenceException {
        return null;
    }

    @Override
    public Mediation getMediationPolicy(Organization organization, String s, String s1) throws MediationPolicyPersistenceException {
        return null;
    }

    @Override
    public List<MediationInfo> getAllMediationPolicies(Organization organization, String s) throws MediationPolicyPersistenceException {
        return null;
    }

    @Override
    public void deleteMediationPolicy(Organization organization, String s, String s1) throws MediationPolicyPersistenceException {

    }

    @Override
    public void saveThumbnail(Organization organization, String s, ResourceFile resourceFile) throws ThumbnailPersistenceException {

    }

    @Override
    public ResourceFile getThumbnail(Organization organization, String s) throws ThumbnailPersistenceException {
        return null;
    }

    @Override
    public void deleteThumbnail(Organization organization, String s) throws ThumbnailPersistenceException {

    }

    @Override
    public PublisherAPIProduct addAPIProduct(Organization organization, PublisherAPIProduct publisherAPIProduct) throws APIPersistenceException {
        return null;
    }

    @Override
    public PublisherAPIProduct updateAPIProduct(Organization organization, PublisherAPIProduct publisherAPIProduct) throws APIPersistenceException {
        return null;
    }

    @Override
    public PublisherAPIProduct getPublisherAPIProduct(Organization organization, String s) throws APIPersistenceException {
        return null;
    }

    @Override
    public PublisherAPIProductSearchResult searchAPIProductsForPublisher(Organization organization, String s, int i, int i1, UserContext userContext) throws APIPersistenceException {
        return null;
    }

    @Override
    public void deleteAPIProduct(Organization organization, String s) throws APIPersistenceException {

    }

    @Override
    public Set<Tag> getAllTags(Organization organization, UserContext userContext) throws APIPersistenceException {
        return null;
    }
}
