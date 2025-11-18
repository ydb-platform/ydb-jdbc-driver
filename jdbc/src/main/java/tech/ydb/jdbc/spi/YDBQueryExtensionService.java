package tech.ydb.jdbc.spi;

import tech.ydb.jdbc.YdbQueryResult;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.query.Params;


/**
 * SPI for extending YdbStatement execution process.
 * Currently, supports data queries from TableService and QueryService.
 * Allows to gather execution statistics and alter settings and statements before execution.
 *
 * @see tech.ydb.jdbc.impl.YdbStatementBase#executeDataQuery
 */
public interface YDBQueryExtensionService {
    /**
     * Handler which will be called before statement execution for data queries.
     * Example use: modify some statements to enable statistics collection
     *
     * @param ctx       Current connection context
     * @param statement Current statement
     * @param query     Current query
     * @param yql       Query sql
     * @param params    Query parameters
     */
    void dataQueryPreExecute(
            YdbContext ctx,
            YdbStatement statement,
            YdbQuery query,
            String yql,
            Params params
    );

    /**
     * Handler which will be called after successful statement execution for data queries.
     * Example use: analyze query statistics
     *
     * @param ctx       Current connection context
     * @param statement Current statement
     * @param query     Current query
     * @param yql       Query sql
     * @param params    Query parameters
     * @param result    Query result, may optionally include query statistics if it was enabled
     */
    void dataQueryPostExecute(
            YdbContext ctx,
            YdbStatement statement,
            YdbQuery query,
            String yql,
            Params params,
            YdbQueryResult result
    );
}
