package tech.ydb.jdbc.impl.params;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.exception.YdbNonRetryableException;
import tech.ydb.jdbc.impl.YdbJdbcParams;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class BatchedParams implements YdbJdbcParams {
    private final String batchParamName;
    private final Map<String, ParamDescription> paramsByName;
    private final ParamDescription[] params;

    private final List<StructValue> batchList = new ArrayList<>();
    private final Map<String, Value<?>> currentValues = new HashMap<>();

    private BatchedParams(String listName, StructType structType) {
        this.batchParamName = listName;
        this.paramsByName = new HashMap<>();
        this.params = new ParamDescription[structType.getMembersCount()];

        for (int idx = 0; idx < structType.getMembersCount(); idx += 1) {
            String paramName = structType.getMemberName(idx);
            String displayName = YdbConst.VARIABLE_PARAMETER_PREFIX + paramName;
            TypeDescription type = TypeDescription.of(structType.getMemberType(idx));

            ParamDescription param = new ParamDescription(idx, paramName, displayName, type);
            params[idx] = param;
            paramsByName.put(paramName, param);
        }
    }

    @Override
    public int parametersCount() {
        return params.length;
    }

    @Override
    public int batchSize() {
        return batchList.size();
    }

    @Override
    public void clearParameters() {
        currentValues.clear();
    }

    @Override
    public void addBatch() throws SQLException {
        batchList.add(validatedCurrentStruct());
        currentValues.clear();
    }

    @Override
    public void clearBatch() {
        batchList.clear();
    }

    private StructValue validatedCurrentStruct() throws SQLException {
        for (ParamDescription prm: params) {
            if (currentValues.containsKey(prm.name())) {
                continue;
            }

            if (prm.type().isOptional()) {
                currentValues.put(prm.name(), prm.type().nullValue());
            }

            throw new YdbNonRetryableException(
                    YdbConst.MISSING_VALUE_FOR_PARAMETER + prm.displayName(),
                    StatusCode.BAD_REQUEST
            );
        }
        return StructValue.of(currentValues);
    }

    @Override
    public Params getCurrentParams() throws SQLException {
        ListValue list = ListValue.of(validatedCurrentStruct());
        return Params.of(batchParamName, list);
    }

    @Override
    public List<Params> getBatchParams() {
        if (batchList.isEmpty()) {
            return Collections.emptyList();
        }

        Value<?>[] batchStructs = batchList.toArray(new Value<?>[0]);
        ListValue list = ListValue.of(batchStructs);
        return Collections.singletonList(Params.of(batchParamName, list));
    }

    @Override
    public void setParam(int index, Object obj, Type type) throws SQLException {
        if (index <= 0 || index > params.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        ParamDescription desc = params[index - 1];
        Value<?> value = desc.getValue(obj);
        currentValues.put(desc.name(), value);
    }

    @Override
    public void setParam(String name, Object obj, Type type) throws SQLException {
        if (!paramsByName.containsKey(name)) {
            throw new SQLException(YdbConst.PARAMETER_NOT_FOUND + name);
        }
        ParamDescription desc = paramsByName.get(name);
        Value<?> value = desc.getValue(obj);
        currentValues.put(desc.name(), value);
    }

    @Override
    public String getNameByIndex(int index) throws SQLException {
        if (index <= 0 || index > params.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        return params[index - 1].name();
    }

    @Override
    public TypeDescription getDescription(int index) throws SQLException {
        if (index <= 0 || index > params.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        return params[index - 1].type();
    }

    public static BatchedParams tryCreateBatched(Map<String, Type> types) {
        // Only single parameter
        if (types.size() != 1) {
            return null;
        }

        String listName = types.keySet().iterator().next();
        Type type = types.get(listName);

        // Only list of values
        if (type.getKind() != Type.Kind.LIST) {
            return null;
        }

        ListType listType = (ListType) type;
        Type innerType = listType.getItemType();

        // Component - must be struct (i.e. list of structs)
        if (innerType.getKind() != Type.Kind.STRUCT) {
            return null;
        }

        StructType itemType = (StructType)innerType;
        return new BatchedParams(listName, itemType);
    }
}
