package tech.ydb.jdbc.query.params;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.jdbc.query.ParamDescription;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class BulkUpsertQuery extends BatchedQuery {
    private final String tablePath;
    private final ListType bulkType;

    private BulkUpsertQuery(String tablePath, String yql, ListType tp, ParamDescription[] params) throws SQLException {
        super(null, yql, "$bulk", params);
        this.tablePath = tablePath;
        this.bulkType = tp;
    }

    public String getTablePath() {
        return tablePath;
    }

    public ListValue getCurrentBulk() throws SQLException {
        return bulkType.newValue(Collections.singletonList(StructValue.of(validateValues())));
    }

    public ListValue getBatchedBulk() {
        return bulkType.newValue(getBatchedValues());
    }

    public static BulkUpsertQuery build(YdbTypes types, String path, List<String> columns, TableDescription description)
            throws SQLException {
        StringBuilder yql = new StringBuilder();
        yql.append("BULK UPSERT INTO `");
        yql.append(path);
        yql.append("` (");
        yql.append(columns.stream().collect(Collectors.joining(", ")));
        yql.append(")");

        Map<String, Type> columnTypes = new HashMap<>();
        for (TableColumn column: description.getColumns()) {
            columnTypes.put(column.getName(), column.getType());
        }

        ParamDescription[] params = new ParamDescription[columns.size()];
        Map<String, Type> structTypes = new HashMap<>();
        int idx = 0;
        for (String column: columns) {
            if (!columnTypes.containsKey(column)) {
                throw new SQLException("Cannot parse BULK upsert: column " + column + " not found");
            }
            Type type = columnTypes.get(column);
            structTypes.put(column, type);
            params[idx++] = new ParamDescription(column, types.find(type));
        }

        return new BulkUpsertQuery(path, yql.toString(), ListType.of(StructType.of(structTypes)), params);
    }
}
