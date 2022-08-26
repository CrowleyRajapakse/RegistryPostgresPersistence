package org.wso2.apk.extensions.persistence.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.wso2.carbon.apimgt.persistence.utils.RegistryPersistenceUtil;

import java.io.IOException;
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
        String addAPIQuery = "INSERT INTO API_ARTIFACTS (org, uuid, artefact) VALUES(?,?,to_json(?::json));";
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
            connection = PostgresDBConnectionUtil.getConnection();
            connection.setAutoCommit(false);

            preparedStatement = connection.prepareStatement(addAPIQuery);
            preparedStatement.setString(1, organization.getName());
            preparedStatement.setString(2, uuid);
            preparedStatement.setString(3, json);
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
            connection = PostgresDBConnectionUtil.getConnection();
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
            connection = PostgresDBConnectionUtil.getConnection();
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
            connection = PostgresDBConnectionUtil.getConnection();
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
        ResultSet resultSet = null;

        String searchContentQuery = "SELECT DISTINCT artefact,uuid FROM api_artifacts JOIN jsonb_each_text(artefact) e ON true \n" +
                " WHERE org=? AND e.value LIKE ?;";

        String modifiedSearchQuery = "%" + searchQuery.substring(8) +"%";

        try {
            connection = PostgresDBConnectionUtil.getConnection();
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
    public void saveOASDefinition(Organization organization, String s, String s1) throws OASPersistenceException {

    }

    @Override
    public String getOASDefinition(Organization organization, String s) throws OASPersistenceException {
        return null;
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
    public Documentation addDocumentation(Organization organization, String s, Documentation documentation) throws DocumentationPersistenceException {
        return null;
    }

    @Override
    public Documentation updateDocumentation(Organization organization, String s, Documentation documentation) throws DocumentationPersistenceException {
        return null;
    }

    @Override
    public Documentation getDocumentation(Organization organization, String s, String s1) throws DocumentationPersistenceException {
        return null;
    }

    @Override
    public DocumentContent getDocumentationContent(Organization organization, String s, String s1) throws DocumentationPersistenceException {
        return null;
    }

    @Override
    public DocumentContent addDocumentationContent(Organization organization, String s, String s1, DocumentContent documentContent) throws DocumentationPersistenceException {
        return null;
    }

    @Override
    public DocumentSearchResult searchDocumentation(Organization organization, String s, int i, int i1, String s1, UserContext userContext) throws DocumentationPersistenceException {
        return null;
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
