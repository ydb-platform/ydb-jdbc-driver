package tech.ydb.jdbc.spi;

import java.sql.SQLException;

import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class WithTraceIdSpi implements YdbQueryExtentionService {
    private static final ThreadLocal<Tx> LOCAL_TX = new ThreadLocal<>();

    public static void use(String tracePrefix) {
        LOCAL_TX.set(new Tx(tracePrefix));
    }

    @Override
    public void onNewTransaction() {
        Tx tx = LOCAL_TX.get();
        if (tx != null && tx.queryCount > 0) {
            LOCAL_TX.set(null);
        }
    }

    @Override
    public QueryCall newDataQuery(YdbStatement statement, YdbQuery query, String yql) throws SQLException {
        Tx tx = LOCAL_TX.get();
        if (tx == null) {
            return new QueryCall() { };
        }

        tx.newQuery();
        return tx;
    }

    private static class Tx implements QueryCall {
        private final String prefix;
        private int queryCount = 0;

        Tx(String prefix) {
            this.prefix = prefix;
        }

        public void newQuery() {
            queryCount++;
        }

        @Override
        public ExecuteQuerySettings.Builder prepareQuerySettings(ExecuteQuerySettings.Builder builder) {
            return builder.withTraceId(prefix + "-" + queryCount);
        }

        @Override
        public ExecuteDataQuerySettings prepareDataQuerySettings(ExecuteDataQuerySettings settings) {
            return settings.setTraceId(prefix + "-" + queryCount);
        }
    }
}
