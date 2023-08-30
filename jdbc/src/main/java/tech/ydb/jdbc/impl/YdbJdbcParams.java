package tech.ydb.jdbc.impl;

import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nullable;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbPrepareMode;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.context.YdbQuery;
import tech.ydb.jdbc.exception.YdbExecutionException;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.jdbc.impl.params.BatchedParams;
import tech.ydb.jdbc.impl.params.InMemoryParams;
import tech.ydb.jdbc.impl.params.PreparedParams;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbJdbcParams {
    void clearParameters();

    void setParam(int index, @Nullable Object obj, @Nullable Type type);
    void setParam(String name, @Nullable Object obj, @Nullable Type type);

    void addBatch();
    void clearBatch();

    int parametersCount();

    int getIndexByName(String name);
    String getNameByIndex(int index);

    TypeDescription getDescription(int index);

    List<Params> toYdbParams();

    static YdbJdbcParams create(YdbConnectionImpl connection, YdbQuery query, YdbPrepareMode mode) throws SQLException {
        YdbOperationProperties props = connection.getCtx().getOperationProperties();

        if (mode == YdbPrepareMode.IN_MEMORY || (mode == YdbPrepareMode.AUTO && props.isPrepareDataQueryDisabled())) {
            return new InMemoryParams(query);
        }

        if (query.hasIndexesParameters()) {
            throw new YdbExecutionException(YdbConst.INDEXES_PARAMETERS_UNSUPPORTED + query.originSQL());
        }

        DataQuery prepared = connection.prepareDataQuery(query);

        boolean requireBatch = mode == YdbPrepareMode.DATA_QUERY_BATCH;
        if (requireBatch || (mode == YdbPrepareMode.AUTO && !props.isAutoPreparedBatchesDisabled())) {
            BatchedParams params = BatchedParams.fromDataQueryTypes(prepared.types());
            if (params != null) {
                return params;
            }

            if (requireBatch) {
                throw new YdbExecutionException(YdbConst.STATEMENT_IS_NOT_A_BATCH + query.originSQL());
            }
        }

        return new PreparedParams(prepared.types());
    }
}
