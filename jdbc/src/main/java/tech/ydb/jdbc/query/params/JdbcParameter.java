package tech.ydb.jdbc.query.params;

import java.sql.SQLException;
import java.util.Map;

import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface JdbcParameter {
    String getName();
    String getDeclare(Map<String, Value<?>> values) throws SQLException;
    TypeDescription getForcedType();
}
