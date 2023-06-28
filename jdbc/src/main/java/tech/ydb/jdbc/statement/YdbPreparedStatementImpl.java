package tech.ydb.jdbc.statement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbParameterMetaData;
import tech.ydb.jdbc.YdbTypes;
import tech.ydb.jdbc.common.QueryType;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.common.YdbQuery;
import tech.ydb.jdbc.exception.YdbExecutionException;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import static tech.ydb.jdbc.YdbConst.BATCH_INVALID;
import static tech.ydb.jdbc.YdbConst.DEFAULT_BATCH_PARAMETER;
import static tech.ydb.jdbc.YdbConst.PARAMETER_TYPE_UNKNOWN;
import static tech.ydb.jdbc.YdbConst.TRY_EXECUTE_ON_BATCH_STATEMENT;
import static tech.ydb.jdbc.YdbConst.UNKNOWN_PARAMETER_IN_BATCH;
import static tech.ydb.jdbc.YdbConst.VARIABLE_PARAMETER_PREFIX;

// This implementation support all possible scenarios with batched and simple execution mode
// It's a default configuration for all queries
public class YdbPreparedStatementImpl extends AbstractYdbPreparedStatementImpl {

    private final MutableState state = new MutableState();
    private final boolean enforceVariablePrefix;

    public YdbPreparedStatementImpl(YdbConnection connection, YdbQuery query, int resultSetType) throws SQLException {
        super(connection, query, resultSetType);

        YdbOperationProperties properties = connection.getCtx().getOperationProperties();
        this.enforceVariablePrefix = properties.isEnforceVariablePrefix();
        this.state.params = new LinkedHashMap<>(this.state.descriptions.size());
    }

    @Override
    public void clearParameters() {
        this.state.params.clear();
    }

    @Override
    protected void afterExecute() {
        this.clearBatch();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void addBatch() throws SQLException {
        QueryType queryType = getQueryType();
        if (queryType != QueryType.DATA_QUERY) {
            throw new SQLException(BATCH_INVALID + queryType);
        }
        initBatchStruct();

        // All values are passed in sorted order
        state.batch.add(state.batchStruct.newValue((Map) state.params));
        this.clearParameters();
    }

    @Override
    public void clearBatch() {
        state.batch.clear();
        state.executeBatch = false;
        this.clearParameters();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int batchSize = state.batch.size();
        if (batchSize == 0) {
            return new int[0];
        }
        state.executeBatch = true;
        super.execute();
        int[] ret = new int[batchSize];
        Arrays.fill(ret, SUCCESS_NO_INFO);
        return ret;
    }

    @Override
    public YdbParameterMetaData getParameterMetaData() {
        if (enforceVariablePrefix) {
            Map<String, TypeDescription> descriptions = new LinkedHashMap<>(state.descriptions.size());
            for (Map.Entry<String, TypeDescription> entry : state.descriptions.entrySet()) {
                String name = entry.getKey();
                String parameterName;
                if (!name.startsWith(VARIABLE_PARAMETER_PREFIX)) {
                    parameterName = VARIABLE_PARAMETER_PREFIX + name;
                } else {
                    parameterName = name;
                }
                descriptions.put(parameterName, entry.getValue());
            }
            return new YdbParameterMetaDataImpl(descriptions);
        } else {
            return new YdbParameterMetaDataImpl(state.descriptions);
        }
    }

    @Override
    protected void setImpl(String parameterName, @Nullable Object value,
                           int sqlType, @Nullable String typeName, @Nullable Type type)
            throws SQLException {
        TypeDescription description = getParameter(parameterName, value, sqlType, typeName, type);
        Value<?> ydbValue = getValue(parameterName, description, value);
        state.params.put(parameterName, ydbValue);
    }

    @Override
    protected void setImpl(int parameterIndex, @Nullable Object value,
                           int sqlType, @Nullable String typeName, @Nullable Type type)
            throws SQLException {
        setImpl(query.getParameterName(parameterIndex), value, sqlType, typeName, type);
    }

    @Override
    protected Params getParams() throws SQLException {
        if (state.batch.isEmpty()) {
            // Single params
            if (enforceVariablePrefix) {
                Params params = Params.create(state.params.size());
                for (Map.Entry<String, Value<?>> entry : state.params.entrySet()) {
                    String name = entry.getKey();
                    String parameterName;
                    if (!name.startsWith(VARIABLE_PARAMETER_PREFIX)) {
                        parameterName = VARIABLE_PARAMETER_PREFIX + name;
                    } else {
                        parameterName = name;
                    }
                    params.put(parameterName, entry.getValue());
                }
                return params;
            } else {
                return Params.copyOf(state.params);
            }

        } else {
            if (!state.executeBatch) {
                throw new YdbExecutionException(TRY_EXECUTE_ON_BATCH_STATEMENT);
            }

            // Batch params
            ListValue listValue = state.batchList.newValue(state.batch);
            return Params.of(DEFAULT_BATCH_PARAMETER, listValue);
        }
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

    private TypeDescription getParameter(String name,
                                         @Nullable Object value,
                                         int sqlType,
                                         @Nullable String typeName,
                                         @Nullable Type type) throws YdbExecutionException {
        TypeDescription description = state.descriptions.get(name);
        if (description != null) {
            return description;
        }
        if (!state.batch.isEmpty()) {
            throw new YdbExecutionException(UNKNOWN_PARAMETER_IN_BATCH + name);
        }
        Type actualType = toYdbType(name, value, sqlType, typeName, type);
        TypeDescription newDescription = TypeDescription.of(actualType);
        state.descriptions.put(name, newDescription);
        return newDescription;

    }

    private Type toYdbType(String parameterName,
                           @Nullable Object value,
                           int sqlType,
                           @Nullable String typeName,
                           @Nullable Type type)
            throws YdbExecutionException {
        if (value instanceof Value<?>) {
            Value<?> actualValue = (Value<?>) value;
            return actualValue.getType();
        }
        if (type != null) {
            return type;
        }

        YdbTypes types = getConnection().getYdbTypes();
        Type actualType = null;
        if (typeName != null) {
            actualType = types.toYdbType(typeName);
        }
        if (actualType == null) {
            actualType = types.toYdbType(sqlType);
        }
        if (actualType == null && value != null) {
            actualType = types.toYdbType(value.getClass());
        }
        // All types are optional when detected from sqlType
        if (actualType == null) {
            throw new YdbExecutionException(String.format(PARAMETER_TYPE_UNKNOWN,
                    sqlType, typeName, parameterName));
        }
        return actualType;
    }

    private void initBatchStruct() {
        if (state.batchStruct == null) {
            Map<String, Type> typeMap = new LinkedHashMap<>(state.params.size());
            for (Map.Entry<String, Value<?>> entry : state.params.entrySet()) {
                typeMap.put(entry.getKey(), entry.getValue().getType());
            }
            state.batchStruct = StructType.of(typeMap);
            state.batchList = ListType.of(state.batchStruct);
        }
    }

    @SuppressWarnings("rawtypes")
    private static class MutableState {
        // Batches
        private final List<Value<?>> batch = new ArrayList<>();
        private StructType batchStruct;
        private ListType batchList;
        private boolean executeBatch;

        // All known descriptions
        private final Map<String, TypeDescription> descriptions = new LinkedHashMap<>();
        private Map<String, Value<?>> params;
    }


    private static class Key {
        private final String query;
        private final List<Type> types;

        private Key(String query, Params params) {
            this.query = query;
            this.types = params.values().values().stream()
                    .map(Value::getType)
                    .collect(Collectors.toList());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Key)) {
                return false;
            }
            Key key = (Key) o;
            return Objects.equals(query, key.query) &&
                    Objects.equals(types, key.types);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, types);
        }
    }
}
