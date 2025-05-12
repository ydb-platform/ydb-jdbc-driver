package tech.ydb.jdbc;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.impl.YdbConnectionImpl;
import tech.ydb.jdbc.settings.YdbConfig;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.TableClient;


/**
 * YDB JDBC driver, basic implementation supporting {@link TableClient} and {@link SchemeClient}
 */
@SuppressWarnings("ClassWithMultipleLoggers")
public class YdbDriver implements Driver {
    private static final Logger PARENT_LOGGER = Logger.getLogger("tech.ydb.jdbc");
    private static final Logger LOGGER = Logger.getLogger(YdbDriver.class.getName());

    @Nullable
    private static YdbDriver registeredDriver;

    static {
        try {
            register();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final ConcurrentHashMap<YdbConfig, YdbContext> cache = new ConcurrentHashMap<>();

    @Override
    public YdbConnection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        YdbConfig config = YdbConfig.from(url, info);
        LOGGER.log(Level.FINE, "Connect to [{0}] using properties {1}", new Object[] {
            config.getSafeUrl(),
            config.getSafeProps()
        });

        if (!config.isCacheConnectionsInDriver()) {
            final YdbContext context = YdbContext.createContext(config);
            return new YdbConnectionImpl(context) {
                @Override
                public void close() throws SQLException {
                    super.close();
                    context.close();
                }
            };
        }

        YdbContext cached = getCachedContext(config);
        return new YdbConnectionImpl(cached) {
            @Override
            public void close() throws SQLException {
                super.close();
                if (!cached.hasConnections() && cache.remove(config, cached)) {
                    cached.close();
                }
            }
        };
    }

    public YdbContext getCachedContext(YdbConfig config) throws SQLException {
        // Workaround for https://bugs.openjdk.java.net/browse/JDK-8161372 to prevent unnecessary locks in Java 8
        // Was fixed in Java 9+
        YdbContext context = cache.get(config);
        if (context != null) {
            LOGGER.log(Level.FINEST, "Reusing YDB connection to {0}", config.getSafeUrl());
            return context;
        }

        context = YdbContext.createContext(config);
        YdbContext old = cache.putIfAbsent(config, context);
        if (old != null) {
            context.close();
            return old;
        }
        return context;
    }

    @Override
    public boolean acceptsURL(String url) {
        return YdbConfig.isYdb(url);
    }

    @Override
    public String toString() {
        return YdbDriverInfo.DRIVER_FULL_NAME;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        YdbConfig config = YdbConfig.from(url, info);
        return config.toPropertyInfo();
    }

    @Override
    public int getMajorVersion() {
        return YdbDriverInfo.DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return YdbDriverInfo.DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false; // YDB is non-compliant
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return PARENT_LOGGER;
    }

    public int getConnectionCount() {
        return cache.size();
    }

    public void close() {
        if (!cache.isEmpty()) {
            LOGGER.log(Level.FINE, "Closing {0} cached connection(s)...", cache.size());
            cache.values().forEach(YdbContext::close);
            cache.clear();
        }
    }

    public static boolean isRegistered() {
        return registeredDriver != null;
    }

    public static void register() throws SQLException {
        if (isRegistered()) {
            throw new IllegalStateException(YdbConst.DRIVER_IS_ALREADY_REGISTERED);
        }
        YdbDriver driver = new YdbDriver();
        DriverManager.registerDriver(driver);
        YdbDriver.registeredDriver = driver;
        LOGGER.log(Level.INFO, "YDB JDBC Driver registered: {0}", registeredDriver);
    }

    public static void deregister() throws SQLException {
        if (!isRegistered()) {
            throw new IllegalStateException(YdbConst.DRIVER_IS_NOT_REGISTERED);
        }
        DriverManager.deregisterDriver(registeredDriver);
        registeredDriver.close();

        LOGGER.log(Level.INFO, "YDB JDBC Driver deregistered: {0}", registeredDriver);
        registeredDriver = null;
    }
}
