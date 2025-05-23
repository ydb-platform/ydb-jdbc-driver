package tech.ydb.jdbc.query.params;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class BulkUpsertQuery extends BatchedQuery {
    private final String tablePath;
    private final ListType bulkType;

    private BulkUpsertQuery(YdbTypes types, String path, String yql, List<String> columns, Map<String, Type> paramTypes)
            throws SQLException {
        super(types, yql, "$bulk", columns, paramTypes);
        this.tablePath = path;
        this.bulkType = ListType.of(StructType.of(paramTypes));
    }

    public String getTablePath() {
        return tablePath;
    }

    public ListValue getCurrentBulk() throws SQLException {
        return bulkType.newValue(Collections.singletonList(getCurrentValues()));
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

        Map<String, Type> structTypes = new HashMap<>();
        for (String column: columns) {
            if (!columnTypes.containsKey(column)) {
                throw new SQLException("Cannot parse BULK upsert: column " + column + " not found");
            }
            structTypes.put(column, columnTypes.get(column));
        }

        return new BulkUpsertQuery(types, path, yql.toString(), columns, structTypes);
    }
}
