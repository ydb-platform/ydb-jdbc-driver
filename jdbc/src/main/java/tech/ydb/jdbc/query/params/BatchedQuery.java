package tech.ydb.jdbc.query.params;


import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.query.ParamDescription;
import tech.ydb.jdbc.query.YdbPreparedQuery;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.query.YqlBatcher;
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
public class BatchedQuery implements YdbPreparedQuery {
    private final String yql;
    private final String batchParamName;
    private final Map<String, ParamDescription> paramsByName;
    private final ParamDescription[] params;

    private final List<StructValue> batchList = new ArrayList<>();
    private final Map<String, Value<?>> currentValues = new HashMap<>();

    private BatchedQuery(String yql, String listName, StructType structType) {
        this.yql = yql;
        this.batchParamName = listName;
        this.paramsByName = new HashMap<>();
        this.params = new ParamDescription[structType.getMembersCount()];

        Map<String, Type> types = new HashMap<>();
        for (int idx = 0; idx < structType.getMembersCount(); idx += 1) {
            types.put(structType.getMemberName(idx), structType.getMemberType(idx));
        }

        // Firstly put all indexed params (p1, p2, ...,  pN) in correct places of paramNames
        Set<String> indexedNames = new HashSet<>();
        for (int idx = 0; idx < structType.getMembersCount(); idx += 1) {
            String indexedName = YdbConst.INDEXED_PARAMETER_PREFIX + (1 + idx);
            if (types.containsKey(indexedName)) {
                String displayName = YdbConst.VARIABLE_PARAMETER_PREFIX + indexedName;
                TypeDescription typeDesc = TypeDescription.of(types.get(indexedName));
                ParamDescription paramDesc = new ParamDescription(indexedName, displayName, typeDesc);

                params[idx] = paramDesc;
                paramsByName.put(indexedName, paramDesc);
                indexedNames.add(indexedName);
            }
        }

        // Then put all others params in free places of paramNames in alphabetic order
        Iterator<String> sortedIter = new TreeSet<>(types.keySet()).iterator();
        for (int idx = 0; idx < params.length; idx += 1) {
            if (params[idx] != null) {
                continue;
            }

            String param = sortedIter.next();
            while (indexedNames.contains(param)) {
                param = sortedIter.next();
            }

            String displayName = YdbConst.VARIABLE_PARAMETER_PREFIX + param;
            TypeDescription typeDesc = TypeDescription.of(types.get(param));
            ParamDescription paramDesc = new ParamDescription(param, displayName, typeDesc);

            params[idx] = paramDesc;
            paramsByName.put(param, paramDesc);
        }
    }

    @Override
    public String getQueryText(Params prms) {
        return yql;
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
                continue;
            }

            throw new SQLDataException(YdbConst.MISSING_VALUE_FOR_PARAMETER + prm.displayName());
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
    public void setParam(int index, Object obj, int sqlType) throws SQLException {
        if (index <= 0 || index > params.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        ParamDescription desc = params[index - 1];
        Value<?> value = ValueFactory.readValue(desc.displayName(), obj, desc.type());
        currentValues.put(desc.name(), value);
    }

    @Override
    public void setParam(String name, Object obj, int sqlType) throws SQLException {
        if (!paramsByName.containsKey(name)) {
            throw new SQLException(YdbConst.PARAMETER_NOT_FOUND + name);
        }
        ParamDescription desc = paramsByName.get(name);
        Value<?> value = ValueFactory.readValue(desc.displayName(), obj, desc.type());
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

    public static BatchedQuery tryCreateBatched(YdbQuery query, Map<String, Type> types) {
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

        StructType itemType = (StructType) innerType;
        return new BatchedQuery(query.getPreparedYql(), listName, itemType);
    }

    public static BatchedQuery createAutoBatched(YqlBatcher batcher, Map<String, Type> tableColumns) {
        StringBuilder sb = new StringBuilder();
        Map<String, Type> structTypes = new HashMap<>();

        sb.append("DECLARE $batch AS List<Struct<");
        int idx = 1;
        for (String column: batcher.getColumns()) {
            Type type = tableColumns.get(column);
            if (type == null) {
                return null;
            }
            if (idx > 1) {
                sb.append(", ");
            }
            sb.append("p").append(idx).append(":").append(type.toString());
            structTypes.put("p" + idx, type);
            idx++;
        }
        sb.append(">>;\n");

        if (batcher.isInsert()) {
            sb.append("INSERT ");
        }
        if (batcher.isUpsert()) {
            sb.append("UPSERT ");
        }

        sb.append("INTO `").append(batcher.getTableName()).append("` SELECT ");

        idx = 1;
        for (String column: batcher.getColumns()) {
            if (idx > 1) {
                sb.append(", ");
            }
            sb.append("p").append(idx).append(" AS `").append(column).append("`");
            idx++;
        }

        sb.append(" FROM AS_TABLE($batch);");

        return new BatchedQuery(sb.toString(), "$batch", StructType.of(structTypes));
    }
}
