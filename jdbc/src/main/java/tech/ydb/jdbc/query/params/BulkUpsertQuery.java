package tech.ydb.jdbc.query.params;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    private BulkUpsertQuery(String tablePath, String yql, List<String> columns, Map<String, Type> types)
            throws SQLException {
        super(yql, "$bulk", columns, types);
        this.tablePath = tablePath;
        this.bulkType = ListType.of(StructType.of(types));
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

    public static BulkUpsertQuery build(String tablePath, List<String> columns, TableDescription description)
            throws SQLException {
        StringBuilder yql = new StringBuilder();
        yql.append("BULK UPSERT INTO `");
        yql.append(tablePath);
        yql.append("` (");
        yql.append(columns.stream().collect(Collectors.joining(", ")));
        yql.append(")");

        Map<String, Type> columnTypes = new HashMap<>();
        for (TableColumn column: description.getColumns()) {
            columnTypes.put(column.getName(), column.getType());
        }

        Map<String, Type> structTypes = new HashMap<>();
        for (String column: columns) {
            structTypes.put(column, columnTypes.get(column));
        }

        return new BulkUpsertQuery(tablePath, yql.toString(), columns, structTypes);
    }
}
