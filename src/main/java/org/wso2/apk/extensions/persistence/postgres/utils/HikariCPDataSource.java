package org.wso2.apk.extensions.persistence.postgres.utils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class HikariCPDataSource {

    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;

    static {
        config.setJdbcUrl( "jdbc:postgresql://localhost:5433/amdb" );
        config.setUsername( "postgres" );
        config.setPassword( "SUAJiPyiHsfV93EucjOa4qWwQP7Qb2KXLqDFxI8K8LUaNLvvI0xzaddx2iheRGB1" );
//        config.setJdbcUrl( "jdbc:postgresql://localhost:5432/amdb2" );
//        config.setUsername( "sampath" );
//        config.setPassword( "sampath" );
        config.setMaximumPoolSize(50);
        config.setMinimumIdle(10);
        config.setMaxLifetime(60000);
        config.setAutoCommit(true);
        config.setConnectionTestQuery("SELECT 1");
        config.setValidationTimeout(30000);
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        ds = new HikariDataSource( config );
    }

    private HikariCPDataSource() {}

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
