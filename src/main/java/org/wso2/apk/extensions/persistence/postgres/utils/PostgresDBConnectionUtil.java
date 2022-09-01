package org.wso2.apk.extensions.persistence.postgres.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.apimgt.api.APIManagementException;
import org.wso2.carbon.apimgt.api.APIManagerDatabaseException;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Map;

public final class PostgresDBConnectionUtil {
    private static final Log log = LogFactory.getLog(PostgresDBConnectionUtil.class);
    private static volatile DataSource dataSource = null;
    private static final String DB_CHECK_SQL = "SELECT * FROM AM_API";

    private static final String DATA_SOURCE_NAME = "DataSourceName";

    /**
     * Initializes the data source
     *
     * @throws APIManagementException if an error occurs while loading DB configuration
     */
    public static void initialize() throws APIManagerDatabaseException {
        if (dataSource != null) {
            return;
        }

        synchronized (PostgresDBConnectionUtil.class) {
            if (dataSource == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Initializing data source");
                }
                String dataSourceName = "jdbc/WSO2AM_DB";

                if (dataSourceName != null) {
                    try {
                        Context ctx = new InitialContext();
                        dataSource = (DataSource) ctx.lookup(dataSourceName);
                    } catch (NamingException e) {
                        throw new APIManagerDatabaseException("Error while looking up the data " +
                                "source: " + dataSourceName, e);
                    }
                } else {
                    log.error(DATA_SOURCE_NAME + " not defined in api-manager.xml.");
                }
            }
        }
    }

    /**
     * Utility method to get a new database connection
     *
     * @return Connection
     * @throws java.sql.SQLException if failed to get Connection
     */
    public static Connection getConnection() throws SQLException {
//        if (dataSource != null) {
//            return dataSource.getConnection();
//        }
//        throw new SQLException("Data source is not configured properly.");
//        return DriverManager.getConnection("jdbc:postgresql://localhost:5432/amdb",
//                "sampath", "sampath");
        return DriverManager.getConnection("jdbc:postgresql://localhost:5433/amdb",
                "postgres", "SUAJiPyiHsfV93EucjOa4qWwQP7Qb2KXLqDFxI8K8LUaNLvvI0xzaddx2iheRGB1");
    }

    /**
     * Utility method to close the connection streams.
     *
     * @param preparedStatement PreparedStatement
     * @param connection        Connection
     * @param resultSet         ResultSet
     */
    public static void closeAllConnections(PreparedStatement preparedStatement, Connection connection,
                                           ResultSet resultSet) {
        closeConnection(connection);
        closeResultSet(resultSet);
        closeStatement(preparedStatement);
    }

    /**
     * Close Connection
     *
     * @param dbConnection Connection
     */
    private static void closeConnection(Connection dbConnection) {
        if (dbConnection != null) {
            try {
                dbConnection.close();
            } catch (SQLException e) {
                log.warn("Database error. Could not close database connection. Continuing with " +
                        "others. - " + e.getMessage(), e);
            }
        }
    }

    /**
     * Close ResultSet
     *
     * @param resultSet ResultSet
     */
    private static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.warn("Database error. Could not close ResultSet  - " + e.getMessage(), e);
            }
        }

    }

    /**
     * Close PreparedStatement
     *
     * @param preparedStatement PreparedStatement
     */
    public static void closeStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                log.warn("Database error. Could not close PreparedStatement. Continuing with" +
                        " others. - " + e.getMessage(), e);
            }
        }

    }

    /**
     * Function converts IS to String
     * Used for handling blobs
     *
     * @param is - The Input Stream
     * @return - The inputStream as a String
     */
    public static String getStringFromInputStream(InputStream is) {
        String str = null;
        try {
            str = IOUtils.toString(is, "UTF-8");
        } catch (IOException e) {
            log.error("Error occurred while converting input stream to string.", e);
        }
        return str;
    }

    /**
     * Function converts IS to byte[]
     * Used for handling inputstreams
     *
     * @param is - The Input Stream
     * @return - The inputStream as a byte array
     */
    public static byte[] getBytesFromInputStream(InputStream is) {
        byte[] byteArray = null;
        try {
            byteArray = IOUtils.toByteArray(is);
        } catch (IOException e) {
            log.error("Error occurred while converting input stream to byte array.", e);
        }
        return byteArray;
    }

    /**
     * Set autocommit state of the connection
     *
     * @param dbConnection Connection
     * @param autoCommit   autoCommitState
     */
    public static void setAutoCommit(Connection dbConnection, boolean autoCommit) {
        if (dbConnection != null) {
            try {
                dbConnection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
                log.error("Could not set auto commit back to initial state", e);
            }
        }
    }

    /**
     * Handle connection rollback logic. Rethrow original exception so that it can be handled centrally.
     *
     * @param connection Connection
     * @param error      Error message to be logged
     * @throws SQLException
     */
    public static void rollbackConnection(Connection connection, String error) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException rollbackException) {
                // rollback failed
                log.error(error, rollbackException);
            }
            // Rethrow original exception so that it can be handled in the common catch clause of the calling method
        }
    }

    /**
     * Converts a JSON Object String to a String Map
     *
     * @param jsonString JSON String
     * @return String Map
     * @throws APIManagementException if errors occur during parsing the json string
     */
    public static Map<String, Object> convertJSONStringToMap(String jsonString) throws APIManagementException {
        Map<String, Object> map = null;
        if (StringUtils.isNotEmpty(jsonString)) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                map = objectMapper.readValue(jsonString, Map.class);
            } catch (IOException e) {
                String msg = "Error while parsing JSON string";
                log.error(msg, e);
                throw new APIManagementException(msg, e);
            }
        }
        return map;
    }
}