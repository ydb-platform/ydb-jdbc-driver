package tech.ydb.jdbc.impl;


import java.sql.SQLException;

import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.query.UnifiedQueryStats;
import tech.ydb.jdbc.query.YdbQuery;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryResultStatic extends YdbQueryResultBase {
    private final YdbResultSet[] rs;
    private UnifiedQueryStats queryStats;

    public YdbQueryResultStatic(YdbQuery query, YdbResultSet... rs) {
        super(query, rs != null ? rs.length : 0);
        this.rs = rs;
    }

    @Override
    protected YdbResultSet getResultSet(int index) throws SQLException {
        if (index < 0 || index >= rs.length) {
            return null;
        }
        return rs[index];
    }

    @Override
    protected void closeResultSet(int index) throws SQLException {
        if (index < 0 || index >= rs.length) {
            return;
        }
        rs[index].close();
    }

    public UnifiedQueryStats getQueryStats() {
        return queryStats;
    }

    public void setQueryStats(tech.ydb.table.query.stats.QueryStats src) {
        if (src != null) {
            queryStats = new UnifiedQueryStats(src);
        }
    }

    public void setQueryStats(tech.ydb.query.result.QueryStats src) {
        if (src != null) {
            queryStats = new UnifiedQueryStats(src);
        }
    }
}
