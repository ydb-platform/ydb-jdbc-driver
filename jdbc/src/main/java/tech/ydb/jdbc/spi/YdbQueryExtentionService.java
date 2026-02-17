package tech.ydb.jdbc.spi;

import java.sql.SQLException;

import tech.ydb.core.Status;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.query.result.QueryStats;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;


/**
 * SPI for extending YdbStatement execution process. Currently, supports data queries from TableService and
 * QueryService. Allows to gather execution statistics and alter settings and statements before execution.
 *
 * @see tech.ydb.jdbc.context.YdbExecutor#executeDataQuery(tech.ydb.jdbc.YdbStatement, tech.ydb.jdbc.query.YdbQuery,
 * java.lang.String, tech.ydb.table.query.Params)
 */
public interface YdbQueryExtentionService {
    interface QueryCall {
        /**
         * Handler to customize QueryService's query execution settings.
         *
         * @param builder
         * @return customized ExecuteQuerySettings.Builder
         */
        default ExecuteQuerySettings.Builder prepareQuerySettings(ExecuteQuerySettings.Builder builder) {
            return builder;
        }

        /**
         * Handler to customize TableService's query execution settings.
         *
         * @param settings
         * @return customized ExecuteDataQuerySettings
         */
        default ExecuteDataQuerySettings prepareDataQuerySettings(ExecuteDataQuerySettings settings) {
            return settings;
        }

        /**
         * Called on the query execution statistics handling (if that was enabled on
         * {@link QueryCall#prepareQuerySettings(tech.ydb.query.settings.ExecuteQuerySettings.Builder)} or
         * {@link QueryCall#prepareDataQuerySettings(tech.ydb.table.settings.ExecuteDataQuerySettings)} ).
         *
         * @param stats
         */
        default void onQueryStats(QueryStats stats)  {

        }

        /**
         * Called on the query execution result handling.
         *
         * @param status result of query execution
         * @param th exception if query execution was interrupted
         */
        default void onQueryResult(Status status, Throwable th)  {

        }
    }

    /**
     * Handler which will be called on every statement execution for data queries.
     *
     * @param statement Current statement
     * @param query     Internal query information
     * @param yql       Prepared YQL query, might be different for different parameters
     * @return current query handler
     * @throws java.sql.SQLException if SPI rejected query execution
     */
    QueryCall newDataQuery(YdbStatement statement, YdbQuery query, String yql) throws SQLException;

    default void onNewTransaction() {

    }
}
