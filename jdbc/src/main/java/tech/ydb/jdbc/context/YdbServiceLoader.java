package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import tech.ydb.core.Status;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.spi.YdbQueryExtentionService;
import tech.ydb.query.result.QueryStats;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbServiceLoader {
    private static final YdbQueryExtentionService.QueryCall DUMP = new YdbQueryExtentionService.QueryCall() { };
    private static final YdbQueryExtentionService EMPTY = (statement, query, yql) -> DUMP;

    private YdbServiceLoader() { }

    public static YdbQueryExtentionService loadQuerySpi() {
        List<YdbQueryExtentionService> spis = new ArrayList<>();
        ServiceLoader.load(YdbQueryExtentionService.class).forEach(spis::add);

        if (spis.isEmpty()) {
            return EMPTY;
        }

        if (spis.size() == 1) {
            return spis.get(0);
        }

        return new ProxySpi(spis);
    }

    private static class ProxySpi implements YdbQueryExtentionService {
        private final List<YdbQueryExtentionService> spis;

        ProxySpi(List<YdbQueryExtentionService> spis) {
            this.spis = spis;
        }

        @Override
        public QueryCall newDataQuery(YdbStatement statement, YdbQuery query, String yql) throws SQLException {
            List<QueryCall> proxed = new ArrayList<>();
            try {
                for (YdbQueryExtentionService spi: spis) {
                    proxed.add(spi.newDataQuery(statement, query, yql));
                }
            } catch (SQLException | RuntimeException th) {
                for (QueryCall call: proxed) {
                    call.onQueryResult(null, th);
                }
                throw th;
            }

            return new QueryCall() {
                @Override
                public ExecuteQuerySettings.Builder prepareQuerySettings(ExecuteQuerySettings.Builder builder) {
                    ExecuteQuerySettings.Builder local = builder;
                    for (QueryCall proxy: proxed) {
                        local = proxy.prepareQuerySettings(local);
                    }
                    return local;
                }

                @Override
                public ExecuteDataQuerySettings prepareDataQuerySettings(ExecuteDataQuerySettings settings) {
                    ExecuteDataQuerySettings local = settings;
                    for (QueryCall proxy: proxed) {
                        local = proxy.prepareDataQuerySettings(local);
                    }
                    return local;
                }

                @Override
                public void onQueryResult(Status status, Throwable th) {
                    for (QueryCall proxy: proxed) {
                        proxy.onQueryResult(status, th);
                    }
                }

                @Override
                public void onQueryStats(QueryStats stats) {
                    for (QueryCall proxy: proxed) {
                        proxy.onQueryStats(stats);
                    }
                }
            };
        }
    }
}
