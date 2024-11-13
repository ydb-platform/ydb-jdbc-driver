package tech.ydb.jdbc.query.params;


import java.sql.SQLException;

import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.table.query.Params;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface JdbcPrm {
    String getName();
    TypeDescription getType();

    void setValue(Object obj, int sqlType) throws SQLException;
    void copyToParams(Params params) throws SQLException;

    void reset();
}
