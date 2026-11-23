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

        if (config.isCacheConnectionsInDriver()) {
            return newCachedConnection(config);
        }

        YdbContext context = YdbContext.createContext(config);
        try {
            return new YdbConnectionImpl(context, context::close);
        } catch (SQLException | RuntimeException ex) {
            context.close();
            throw ex;
        }
    }

    private YdbConnection newCachedConnection(YdbConfig config) throws SQLException {
        while (true) {
            YdbContext cached = cache.get(config);
            if (cached != null) {
                cached.registerConnection();
                if (cache.get(config) == cached) {
                    LOGGER.log(Level.FINEST, "Reusing YDB connection to {0}", config.getSafeUrl());
                    return makeCachedConnection(config, cached);
                }
            } else {
                cached = YdbContext.createContext(config);
                cached.registerConnection();
                if (cache.putIfAbsent(config, cached) == null) {
                    return makeCachedConnection(config, cached);
                }
            }

            cached.deregisterConnection();
            if (cached.isEmpty()) {
                cached.close();
            }
        }
    }

    private void closeCachedContext(YdbConfig config, YdbContext ctx) {
        ctx.deregisterConnection();
        if (!ctx.isEmpty()) {
            return;
        }

        cache.remove(config, ctx);

        if (ctx.isEmpty()) {
            ctx.close();
        }
    }

    private YdbConnectionImpl makeCachedConnection(YdbConfig config, YdbContext ctx) throws SQLException {
        try {
            return new YdbConnectionImpl(ctx, () -> closeCachedContext(config, ctx));
        } catch (SQLException | RuntimeException ex) {
            closeCachedContext(config, ctx);
            throw ex;
        }
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

    public void clear() {
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
        registeredDriver.clear();

        LOGGER.log(Level.INFO, "YDB JDBC Driver deregistered: {0}", registeredDriver);
        registeredDriver = null;
    }
}
