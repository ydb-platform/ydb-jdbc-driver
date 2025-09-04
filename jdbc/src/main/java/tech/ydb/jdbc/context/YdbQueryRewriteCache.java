package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.Hashing;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.query.QueryKey;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.settings.YdbQueryProperties;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveValue;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryRewriteCache extends YdbCache {
    private static final Logger LOGGER = Logger.getLogger(YdbQueryRewriteCache.class.getName());
    private static final ReentrantLock VALIDATE_LOCK = new ReentrantLock();

    private static final String CREATE_SQL = ""
            + "CREATE TABLE IF NOT EXISTS `%s` ("
            + "   hash Text NOT NULL,"
            + "   query Text NOT NULL,"
            + "   rewritten Text,"
            + "   used_at Timestamp,"
            + "   PRIMARY KEY (hash)"
            + ") WITH ("
            + "   AUTO_PARTITIONING_BY_LOAD=ENABLED,"
            + "   AUTO_PARTITIONING_BY_SIZE=ENABLED,"
            + "   AUTO_PARTITIONING_PARTITION_SIZE_MB=100"
            + ");";

    private static final String UPDATE_SQL = ""
            + "DECLARE $h AS Text; "
            + "DECLARE $q AS Text; "
            + "UPSERT INTO `%s` (hash, query, used_at) VALUES ($h, $q, CurrentUtcTimestamp()) RETURNING rewritten;";

    private final String rewriteTable;
    private final Duration rewriteTtl;
    private final Cache<QueryKey, CachedQuery> rewriteCache;

    public YdbQueryRewriteCache(YdbContext ctx, String tableName, Duration ttl, YdbQueryProperties options,
            int cacheSize, boolean fullScanDetector) {
        super(ctx, options, cacheSize, fullScanDetector);
        this.rewriteTable = tableName;
        this.rewriteTtl = ttl;
        this.rewriteCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
    }

    @Override
    public YdbQuery parseYdbQuery(QueryKey key) throws SQLException {
        CachedQuery cached = rewriteCache.getIfPresent(key);
        if (cached == null) {
            cached = new CachedQuery(key);
            rewriteCache.put(key, cached);
        }

        return super.parseYdbQuery(cached.update(key));
    }

    @Override
    public void validate() throws SQLException {
        if (tableDescribeCache.getIfPresent(rewriteTable) != null) {
            return;
        }

        VALIDATE_LOCK.lock();
        try {
            LOGGER.log(Level.INFO, "Validate QueryRewrite {0}", rewriteTable);
            if (tableDescribeCache.getIfPresent(rewriteTable) != null) {
                return;
            }

            // validate table name
            Result<TableDescription> res = retryCtx.supplyResult(s -> s.describeTable(rewriteTable)).join();
            LOGGER.log(Level.INFO, "Describe QueryRewrite {0} -> {1}", new Object[] {rewriteTable, res.getStatus()});
            if (res.isSuccess()) {
                tableDescribeCache.put(rewriteTable, res.getValue());
                return;
            }

            if (res.getStatus().getCode() != StatusCode.SCHEME_ERROR) {
                throw ExceptionFactory.createException("Cannot initialize executor with rewrite table " + rewriteTable,
                        new UnexpectedResultException("Cannot describe", res.getStatus()));
            }

            // Try to create a table
            String query = String.format(CREATE_SQL, rewriteTable);
            Status status = retryCtx.supplyStatus(session -> session.executeSchemeQuery(query)).join();
            LOGGER.log(Level.INFO, "Create rewrite table {0} -> {1}", new Object[] {rewriteTable, status});
            if (!status.isSuccess()) {
                throw ExceptionFactory.createException("Cannot initialize executor with rewrite table " + rewriteTable,
                        new UnexpectedResultException("Cannot create table", status));
            }

            Result<TableDescription> res2 = retryCtx.supplyResult(s -> s.describeTable(rewriteTable)).join();
            LOGGER.log(Level.INFO, "Validate rewrite table {0} -> {1}", new Object[] {rewriteTable, res2.getStatus()});
            if (!res2.isSuccess()) {
                throw ExceptionFactory.createException("Cannot initialize executor with rewrite table " + rewriteTable,
                        new UnexpectedResultException("Cannot describe after creating", res2.getStatus()));
            }

            tableDescribeCache.put(rewriteTable, res2.getValue());
        } finally {
            VALIDATE_LOCK.unlock();
        }
    }

    private class CachedQuery {
        private final String hash;
        private final String query;
        private final AtomicReference<QueryKey> rewritten;
        private final AtomicReference<Instant> ttl;

        CachedQuery(QueryKey origin) {
            this.query = origin.getReturning() != null ? origin.getQuery() + origin.getReturning() : origin.getQuery();
            this.hash = Hashing.sha256().hashBytes(query.getBytes()).toString();
            this.rewritten = new AtomicReference<>();
            this.ttl = new AtomicReference<>(Instant.MIN);
        }

        public QueryKey update(QueryKey origin) {
            Instant now = Instant.now();
            Instant localTtl = ttl.get();
            while (localTtl.isBefore(now)) {
                if (ttl.compareAndSet(localTtl, now.plus(rewriteTtl))) {
                    Params params = Params.of(
                            "$h", PrimitiveValue.newText(hash),
                            "$q", PrimitiveValue.newText(query)
                    );
                    String updateQuery = String.format(UPDATE_SQL, rewriteTable);
                    Result<DataQueryResult> res = retryCtx.supplyResult(
                            session -> session.executeDataQuery(updateQuery, TxControl.serializableRw(), params)
                    ).join();

                    if (res.isSuccess()) {
                        ResultSetReader rs = res.getValue().getResultSet(0);
                        if (rs.next() && rs.getColumn(0).isOptionalItemPresent()) {
                            String rewrittenQuery = rs.getColumn(0).getText();
                            rewritten.set(new QueryKey(rewrittenQuery));
                        }
                    } else {
                        LOGGER.log(Level.WARNING, "Cannot read table {0} -> {1}", new Object[] {
                            rewriteTable, res.getStatus()
                        });
                    }
                }
                localTtl = ttl.get();
            }

            QueryKey local = rewritten.get();
            return local != null ? local : origin;
        }
    }
}
