package tech.ydb.jdbc;

import tech.ydb.jdbc.context.YdbConfig;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.impl.YdbConnectionImpl;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class YdbDataSource implements DataSource {
    private final String jdbcUrl;
    private final Properties connectionProperties;

    protected PrintWriter printWriter;
    protected int loginTimeoutInSeconds = 0;


    public YdbDataSource(String jdbcUrl) {
        this(jdbcUrl, new Properties());
    }

    public YdbDataSource(String jdbcUrl, Properties connInfo) {
        if (jdbcUrl == null) {
            throw new IllegalArgumentException("jdbcUrl must not be null");
        }
        this.jdbcUrl = jdbcUrl;
        connectionProperties = new Properties();
        if (connInfo != null) {
            connectionProperties.putAll(connInfo);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(connectionProperties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        if (username == null) {
            throw new IllegalArgumentException("user cannot be null");
        }
        Properties properties = new Properties();
        properties.putAll(connectionProperties);
        properties.put("user", username);
        if (password == null) {
            properties.put("password", "");
        } else {
            properties.put("password", password);
        }
        return getConnection(properties);
    }

    public Connection getConnection(Properties properties) throws SQLException {
        final YdbConfig config = new YdbConfig(jdbcUrl, properties);
        final YdbContext context = YdbContext.createContext(config);
        return new YdbConnectionImpl(context) {
            @Override
            public void close() throws SQLException {
                super.close();
                context.close();
            }
        };
    }

    @Override
    public PrintWriter getLogWriter() {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        printWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) {
        loginTimeoutInSeconds = seconds;
    }

    @Override
    public int getLoginTimeout() {
        return loginTimeoutInSeconds;
    }

    @Override
    public Logger getParentLogger() {
        return YdbDriver.PARENT_LOGGER;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException(YdbConst.CANNOT_UNWRAP_TO + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
