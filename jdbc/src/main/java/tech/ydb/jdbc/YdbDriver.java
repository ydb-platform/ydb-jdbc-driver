package tech.ydb.jdbc;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    private final ConcurrentHashMap<YdbConfig, CachedContext> cache = new ConcurrentHashMap<>();

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

        CachedContext cached = acquireCachedContext(config);
        try {
            return new YdbConnectionImpl(cached.context) {
                private final AtomicBoolean released = new AtomicBoolean();

                @Override
                public void close() throws SQLException {
                    try {
                        super.close();
                    } finally {
                        if (released.compareAndSet(false, true)) {
                            releaseCachedContext(config, cached);
                        }
                    }
                }
            };
        } catch (SQLException | RuntimeException e) {
            releaseCachedContext(config, cached);
            throw e;
        }
    }

    public YdbContext getCachedContext(YdbConfig config) throws SQLException {
        return getOrCreateCachedContext(config).context;
    }

    private CachedContext acquireCachedContext(YdbConfig config) throws SQLException {
        while (true) {
            CachedContext cached = getOrCreateCachedContext(config);
            if (cached.tryAcquire()) {
                return cached;
            }
            // The last connection has just closed this context, drop the stale entry and try again
            cache.remove(config, cached);
        }
    }

    private CachedContext getOrCreateCachedContext(YdbConfig config) throws SQLException {
        // Workaround for https://bugs.openjdk.java.net/browse/JDK-8161372 to prevent unnecessary locks in Java 8
        // Was fixed in Java 9+
        CachedContext cached = cache.get(config);
        if (cached != null) {
            LOGGER.log(Level.FINEST, "Reusing YDB connection to {0}", config.getSafeUrl());
            return cached;
        }

        CachedContext created = new CachedContext(YdbContext.createContext(config));
        CachedContext old = cache.putIfAbsent(config, created);
        if (old != null) {
            created.context.close();
            return old;
        }
        return created;
    }

    private void releaseCachedContext(YdbConfig config, CachedContext cached) {
        if (cached.release()) {
            cache.remove(config, cached);
            cached.context.close();
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
            cache.values().forEach(CachedContext::close);
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

    /**
     * Context shared by all connections with the same config. A context is closed by the last connection, and
     * once closed it can't be acquired again, so a connection is never opened on a context being shut down.
     */
    private static final class CachedContext {
        private static final int CLOSED = -1;

        private final YdbContext context;
        private final AtomicInteger leases = new AtomicInteger();

        CachedContext(YdbContext context) {
            this.context = context;
        }

        boolean tryAcquire() {
            int actual = leases.get();
            while (actual >= 0) {
                if (leases.compareAndSet(actual, actual + 1)) {
                    return true;
                }
                actual = leases.get();
            }
            return false;
        }

        /**
         * @return true if the caller released the last lease and must close the context
         */
        boolean release() {
            return leases.decrementAndGet() == 0 && leases.compareAndSet(0, CLOSED);
        }

        /**
         * Closes the context regardless of open connections. Their later releases never reach zero again,
         * so the context is not closed twice.
         */
        void close() {
            if (leases.getAndSet(CLOSED) != CLOSED) {
                context.close();
            }
        }
    }
}
