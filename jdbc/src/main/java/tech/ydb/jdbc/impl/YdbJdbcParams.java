package tech.ydb.jdbc.impl;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbPrepareMode;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.impl.params.BatchedParams;
import tech.ydb.jdbc.impl.params.InMemoryParams;
import tech.ydb.jdbc.impl.params.PreparedParams;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.query.YdbQueryOptions;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbJdbcParams {
    void clearParameters();

    void setParam(int index, @Nullable Object obj, @Nonnull Type type) throws SQLException;
    void setParam(String name, @Nullable Object obj, @Nonnull Type type) throws SQLException;

    String getNameByIndex(int index) throws SQLException;

    void addBatch() throws SQLException;
    void clearBatch();
    int batchSize();

    int parametersCount();

    TypeDescription getDescription(int index) throws SQLException;

    List<Params> getBatchParams() throws SQLException;
    Params getCurrentParams() throws SQLException;

    static YdbJdbcParams create(YdbConnectionImpl connection, YdbQuery query, YdbPrepareMode mode) throws SQLException {
        YdbQueryOptions opts = connection.getCtx().getQueryOptions();

        if (query.hasIndexesParameters()
                || mode == YdbPrepareMode.IN_MEMORY
                || !opts.iPrepareDataQueries()) {
            return new InMemoryParams(query.getIndexesParameters());
        }

        DataQuery prepared = connection.prepareDataQuery(query);

        boolean requireBatch = mode == YdbPrepareMode.DATA_QUERY_BATCH;
        if (requireBatch || (mode == YdbPrepareMode.AUTO && opts.isDetectBatchQueries())) {
            BatchedParams params = BatchedParams.tryCreateBatched(prepared.types());
            if (params != null) {
                return params;
            }

            if (requireBatch) {
                throw new SQLDataException(YdbConst.STATEMENT_IS_NOT_A_BATCH + query.originSQL());
            }
        }

        return new PreparedParams(prepared.types());
    }
}
