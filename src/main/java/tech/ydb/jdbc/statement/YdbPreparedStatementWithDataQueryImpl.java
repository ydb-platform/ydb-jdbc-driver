package tech.ydb.jdbc.statement;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Nullable;

import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.common.YdbQuery;
import tech.ydb.jdbc.connection.YdbConnectionImpl;
import tech.ydb.jdbc.exception.YdbExecutionException;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import static tech.ydb.jdbc.YdbConst.BATCH_UNSUPPORTED;
import static tech.ydb.jdbc.YdbConst.INDEXED_PARAMETER_PREFIX;
import static tech.ydb.jdbc.YdbConst.PARAMETER_NOT_FOUND;
import static tech.ydb.jdbc.YdbConst.VARIABLE_PARAMETER_PREFIX;

public class YdbPreparedStatementWithDataQueryImpl extends AbstractYdbDataQueryPreparedStatementImpl {

    private final DataQuery dataQuery;
    private final MutableState state = new MutableState();
    private final PreparedConfiguration cfg;
    private final boolean enforceVariablePrefix;

    public YdbPreparedStatementWithDataQueryImpl(YdbConnectionImpl connection,
                                                 int resultSetType,
                                                 YdbQuery query,
                                                 DataQuery dataQuery) throws SQLException {
        super(connection, resultSetType, query, dataQuery);
        this.dataQuery = dataQuery;
        this.cfg = asPreparedConfiguration(dataQuery.types());
        this.enforceVariablePrefix = connection.getCtx().getOperationProperties().isEnforceVariablePrefix();
        this.clearParameters();
    }

    @Override
    public void clearParameters() {
        this.state.params = dataQuery.newParams();
    }

    @Override
    protected void afterExecute() {
        this.clearParameters();
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException(BATCH_UNSUPPORTED);
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException(BATCH_UNSUPPORTED);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException(BATCH_UNSUPPORTED);
    }

    //

    @Override
    protected void setImpl(String origParameterName, @Nullable Object x,
                           int sqlType, @Nullable String typeName, @Nullable Type type)
            throws SQLException {
        String parameterName;
        if (enforceVariablePrefix && !origParameterName.startsWith(VARIABLE_PARAMETER_PREFIX)) {
            parameterName = VARIABLE_PARAMETER_PREFIX + origParameterName;
        } else {
            parameterName = origParameterName;
        }
        TypeDescription description = getParameter(parameterName);
        Value<?> value = getValue(parameterName, description, x);
        state.params.put(parameterName, value);
    }

    @Override
    protected void setImpl(int parameterIndex, @Nullable Object x,
                           int sqlType, @Nullable String typeName, @Nullable Type type)
            throws SQLException {
        setImpl(INDEXED_PARAMETER_PREFIX + parameterIndex, x, sqlType, typeName, null);
    }

    @Override
    protected Params getParams() {
        return state.params;
    }

    @Override
    protected Map<String, TypeDescription> getParameterTypes() {
        Map<String, Type> source = dataQuery.types();
        Map<String, TypeDescription> target = new LinkedHashMap<>(source.size());
        for (Map.Entry<String, Type> entry : source.entrySet()) {
            target.put(entry.getKey(), TypeDescription.of(entry.getValue()));
        }
        return target;
    }

    //

    private TypeDescription getParameter(String name) throws YdbExecutionException {
        TypeDescription description = cfg.descriptions.get(name);
        if (description == null) {
            throw new YdbExecutionException(PARAMETER_NOT_FOUND + name);
        }
        return description;
    }

    private static PreparedConfiguration asPreparedConfiguration(Map<String, Type> types) {
        int count = types.size();

        Map<String, TypeDescription> descriptions = new HashMap<>(count);
        for (Map.Entry<String, Type> entry : types.entrySet()) {
            descriptions.put(entry.getKey(), TypeDescription.of(entry.getValue()));
        }

        return new PreparedConfiguration(descriptions);
    }

    private static class PreparedConfiguration {
        private final Map<String, TypeDescription> descriptions;

        private PreparedConfiguration(Map<String, TypeDescription> descriptions) {
            this.descriptions = descriptions;
        }
    }

    private static class MutableState {
        private Params params;
    }
}
