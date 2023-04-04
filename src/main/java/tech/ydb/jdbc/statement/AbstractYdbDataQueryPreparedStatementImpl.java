package tech.ydb.jdbc.statement;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbParameterMetaData;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.common.YdbQuery;
import tech.ydb.jdbc.connection.YdbConnectionImpl;
import tech.ydb.table.query.DataQuery;

public abstract class AbstractYdbDataQueryPreparedStatementImpl extends AbstractYdbPreparedStatementImpl {

    private final Supplier<YdbParameterMetaData> metaDataSupplier;
    private final DataQuery dataQuery;

    protected AbstractYdbDataQueryPreparedStatementImpl(YdbConnectionImpl connection,
                                                        int resultSetType,
                                                        YdbQuery query,
                                                        DataQuery dataQuery) throws SQLException {
        super(connection, query, resultSetType);
        this.dataQuery = Objects.requireNonNull(dataQuery);
        this.metaDataSupplier = Suppliers.memoize(() ->
                new YdbParameterMetaDataImpl(getParameterTypes()))::get;
    }

    @Override
    public YdbParameterMetaData getParameterMetaData() {
        return metaDataSupplier.get();
    }

    protected DataQuery getDataQuery() {
        return dataQuery;
    }

    @Override
    protected boolean executeImpl() throws SQLException {
        switch (query.type()) {
            case DATA_QUERY:
                return executeDataQueryImpl(query, getParams());
            case SCAN_QUERY:
                return executeScanQueryImpl(query, getParams());
            default:
                throw new SQLException(YdbConst.UNSUPPORTED_QUERY_TYPE_IN_PS + query.type());
        }
    }

    protected abstract Map<String, TypeDescription> getParameterTypes();
}
