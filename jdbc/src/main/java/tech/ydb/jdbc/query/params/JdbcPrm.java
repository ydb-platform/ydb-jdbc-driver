package tech.ydb.jdbc.query.params;


import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.common.YdbTypes;
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

    interface Factory {
        List<? extends JdbcPrm> create();
    }

    static Factory simplePrm(YdbTypes types, String name) {
        return () -> Collections.singletonList(new SimpleJdbcPrm(types, name));
    }

    static Factory uint64Prm(YdbTypes types, String name) {
        return () -> Collections.singletonList(new UInt64JdbcPrm(types, name));
    }

    static Factory inListOrm(YdbTypes types, String name, int listSize, int tupleSize) {
        return () -> new InListJdbcPrm(types, name, listSize, tupleSize).toJdbcPrmList();
    }

    static Factory jdbcTableListOrm(YdbTypes types, String name, int count) {
        return () -> new AsTableJdbcPrm(types, name, count).toJdbcPrmList();
    }
}
