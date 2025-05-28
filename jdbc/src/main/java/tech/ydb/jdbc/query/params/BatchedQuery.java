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
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.jdbc.query.ParamDescription;
import tech.ydb.jdbc.query.YdbPreparedQuery;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.jdbc.query.YqlBatcher;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
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
    private final String singleQuery;
    private final String batchQuery;
    private final String batchParamName;
    private final Map<String, ParamDescription> paramsByName;
    private final ParamDescription[] params;

    private final List<StructValue> batchList = new ArrayList<>();
    private final Map<String, Value<?>> currentValues = new HashMap<>();

    protected BatchedQuery(String single, String batched, String prm, ParamDescription[] params) throws SQLException {
        this.singleQuery = single;
        this.batchQuery = batched;
        this.batchParamName = prm;
        this.paramsByName = new HashMap<>();
        this.params = params;

        for (ParamDescription pd: params) {
            paramsByName.put(pd.name(), pd);
        }
    }

    @Override
    public String getQueryText(Params prms) {
        return singleQuery != null ? singleQuery : batchQuery;
    }

    @Override
    public String getBatchText(Params prms) {
        return batchQuery;
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
        batchList.add(StructValue.of(validateValues()));
        currentValues.clear();
    }

    @Override
    public void clearBatch() {
        batchList.clear();
    }

    protected Map<String, Value<?>> validateValues() throws SQLException {
        for (ParamDescription prm: params) {
            if (!currentValues.containsKey(prm.name())) {
                throw new SQLDataException(YdbConst.MISSING_VALUE_FOR_PARAMETER + prm.displayName());
            }
        }
        return currentValues;
    }

    protected List<StructValue> getBatchedValues() {
        return batchList;
    }

    @Override
    public Params getCurrentParams() throws SQLException {
        Map<String, Value<?>> vv = validateValues();
        if (singleQuery == null) {
            return Params.of(batchParamName, ListValue.of(StructValue.of(vv)));
        }
        Params prms = Params.create(vv.size());
        vv.forEach((name, value) -> prms.put(YdbConst.VARIABLE_PARAMETER_PREFIX + name, value));
        return prms;
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

    public static BatchedQuery tryCreateBatched(YdbTypes types, YdbQuery query, Map<String, Type> preparedTypes)
            throws SQLException {
        // Only single parameter
        if (preparedTypes.size() != 1) {
            return null;
        }

        String listName = preparedTypes.keySet().iterator().next();
        Type type = preparedTypes.get(listName);

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

        String[] columns = new String[itemType.getMembersCount()];
        Map<String, Type> paramTypes = new HashMap<>();
        for (int idx = 0; idx < itemType.getMembersCount(); idx += 1) {
            paramTypes.put(itemType.getMemberName(idx), itemType.getMemberType(idx));
        }

        // Firstly put all indexed params (p1, p2, ...,  pN) in correct places of paramNames
        Set<String> indexedNames = new HashSet<>();
        for (int idx = 0; idx < itemType.getMembersCount(); idx += 1) {
            String indexedName = YdbConst.INDEXED_PARAMETER_PREFIX + (1 + idx);
            if (paramTypes.containsKey(indexedName)) {
                columns[idx] = indexedName;
                indexedNames.add(indexedName);
            }
        }

        // Then put all others params in free places of paramNames in alphabetic order
        Iterator<String> sortedIter = new TreeSet<>(paramTypes.keySet()).iterator();
        for (int idx = 0; idx < columns.length; idx += 1) {
            if (columns[idx] != null) {
                continue;
            }

            String param = sortedIter.next();
            while (indexedNames.contains(param)) {
                param = sortedIter.next();
            }

            columns[idx] = param;
        }

        ParamDescription[] descriptions = new ParamDescription[columns.length];
        for (int idx = 0; idx < columns.length; idx += 1) {
            String name = columns[idx];
            if (!paramTypes.containsKey(name)) {
                throw new SQLException(YdbConst.INVALID_BATCH_COLUMN + name);
            }
            descriptions[idx] = new ParamDescription(name, types.find(paramTypes.get(name)));
        }
        return new BatchedQuery(null, query.getPreparedYql(), listName, descriptions);
    }

    public static BatchedQuery createAutoBatched(YdbTypes types, YqlBatcher batcher, TableDescription description)
            throws SQLException {

        // DELETE and UPDATE may be batched only if WHERE contains only primary key columns
        if (batcher.getCommand() == YqlBatcher.Cmd.DELETE || batcher.getCommand() == YqlBatcher.Cmd.UPDATE) {
            Set<String> primaryKey = new HashSet<>(description.getPrimaryKeys());
            for (String keyColumn: batcher.getKeyColumns()) {
                if (!primaryKey.remove(keyColumn)) {
                    return null;
                }
            }
            if (!primaryKey.isEmpty()) {
                return null;
            }
        }

        Map<String, Type> columnTypes = new HashMap<>();
        for (TableColumn column: description.getColumns()) {
            columnTypes.put(column.getName(), column.getType());
        }

        List<String> columns = new ArrayList<>();
        columns.addAll(batcher.getColumns());
        columns.addAll(batcher.getKeyColumns());

        ParamDescription[] params = new ParamDescription[columns.size()];

        int idx = 1;
        for (String column: columns) {
            Type type = columnTypes.get(column);
            if (type == null) {
                return null;
            }
            params[idx - 1] = new ParamDescription("p" + idx, column, types.find(type));
            idx++;
        }

        return new BatchedQuery(simpleQuery(batcher, params), batchQuery(batcher, params), "$batch", params);
    }

    private static String batchQuery(YqlBatcher batcher, ParamDescription[] params) {
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $batch AS List<Struct<");
        for (int idx = 0; idx < params.length; idx++) {
            if (idx > 0) {
                sb.append(", ");
            }
            sb.append(params[idx].name()).append(":").append(params[idx].type().toYqlLiteral());
        }
        sb.append(">>;\n");

        switch (batcher.getCommand()) {
            case UPSERT:
                sb.append("UPSERT INTO `").append(batcher.getTableName()).append("` SELECT ");
                break;
            case INSERT:
                sb.append("INSERT INTO `").append(batcher.getTableName()).append("` SELECT ");
                break;
            case REPLACE:
                sb.append("REPLACE INTO `").append(batcher.getTableName()).append("` SELECT ");
                break;
            case UPDATE:
                sb.append("UPDATE `").append(batcher.getTableName()).append("` ON SELECT ");
                break;
            case DELETE:
                sb.append("DELETE FROM `").append(batcher.getTableName()).append("` ON SELECT ");
                break;
            default:
                return "UNSUPPORTED CMD " + batcher.getCommand();
        }

        for (int idx = 0; idx < params.length; idx++) {
            if (idx > 0) {
                sb.append(", ");
            }
            sb.append(params[idx].name()).append(" AS `").append(params[idx].displayName()).append("`");
        }

        sb.append(" FROM AS_TABLE($batch);");
        return sb.toString();
    }

    private static String simpleQuery(YqlBatcher batcher, ParamDescription[] params) {
        StringBuilder sb = new StringBuilder();
        for (ParamDescription p : params) {
            sb.append("DECLARE ").append(YdbConst.VARIABLE_PARAMETER_PREFIX).append(p.name())
                    .append(" AS ").append(p.type().toYqlLiteral()).append(";\n");
        }

        switch (batcher.getCommand()) {
            case UPSERT:
                sb.append("UPSERT INTO `").append(batcher.getTableName()).append("` (");
                appendColumns(sb, params);
                sb.append(") VALUES (");
                appendValues(sb, params);
                sb.append(");");
                break;
            case INSERT:
                sb.append("INSERT INTO `").append(batcher.getTableName()).append("` (");
                appendColumns(sb, params);
                sb.append(") VALUES (");
                appendValues(sb, params);
                sb.append(");");
                break;
            case REPLACE:
                sb.append("REPLACE INTO `").append(batcher.getTableName()).append("` (");
                appendColumns(sb, params);
                sb.append(") VALUES (");
                appendValues(sb, params);
                sb.append(");");
                break;
            case UPDATE:
                sb.append("UPDATE `").append(batcher.getTableName()).append("` SET ");
                for (int idx = 0; idx < batcher.getColumns().size(); idx++) {
                    if (idx > 0) {
                        sb.append(", ");
                    }
                    sb.append('`').append(params[idx].displayName()).append("` = ")
                            .append(YdbConst.VARIABLE_PARAMETER_PREFIX).append(params[idx].name());
                }
                sb.append(" WHERE ");
                appendKeys(sb, params, batcher.getColumns().size());
                break;
            case DELETE:
                sb.append("DELETE FROM `").append(batcher.getTableName()).append("` WHERE ");
                appendKeys(sb, params, batcher.getColumns().size());
                break;
            default:
                break;
        }

        return sb.toString();
    }

    private static void appendColumns(StringBuilder sb, ParamDescription[] params) {
        for (int idx = 0; idx < params.length; idx++) {
            if (idx > 0) {
                sb.append(", ");
            }
            sb.append('`').append(params[idx].displayName()).append('`');
        }
    }

    private static void appendValues(StringBuilder sb, ParamDescription[] params) {
        for (int idx = 0; idx < params.length; idx++) {
            if (idx > 0) {
                sb.append(", ");
            }
            sb.append(YdbConst.VARIABLE_PARAMETER_PREFIX).append(params[idx].name());
        }
    }

    private static void appendKeys(StringBuilder sb, ParamDescription[] params, int firstKeyIdx) {
        for (int idx = firstKeyIdx; idx < params.length; idx++) {
            if (idx > firstKeyIdx) {
                sb.append(" AND ");
            }
            sb.append('`').append(params[idx].displayName()).append("` = ")
                    .append(YdbConst.VARIABLE_PARAMETER_PREFIX).append(params[idx].name());
        }
    }
}
