package tech.ydb.jdbc.query.params;


import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

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

    interface Factory {
        List<? extends JdbcPrm> create();
    }

    static Factory simplePrm(String name) {
        return () -> Collections.singletonList(new SimpleJdbcPrm(name));
    }

    static Factory uint64Prm(String name) {
        return () -> Collections.singletonList(new UInt64JdbcPrm(name));
    }

    static Factory inListOrm(String name, int count) {
        return () -> new InListJdbcPrm(name, count).toJdbcPrmList();
    }
}
