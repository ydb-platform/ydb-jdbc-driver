package tech.ydb.jdbc.context;

import java.util.Collection;
import java.util.concurrent.atomic.LongAdder;

import tech.ydb.core.Status;
import tech.ydb.jdbc.common.FixedResultSetFactory;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryStat {
    private static final String PRINT_QUERY = "print_jdbc_stats();";
    private static final String RESET_QUERY = "reset_jdbc_stats();";

    private static final FixedResultSetFactory STATS_RS_FACTORY = FixedResultSetFactory.newBuilder()
            .addTextColumn("sql")
            .addBooleanColumn("is_fullscan")
            .addBooleanColumn("is_error")
            .addLongColumn("executed")
            .addTextColumn("yql")
            .addTextColumn("ast")
            .addTextColumn("plan")
            .build();

    private final String originSQL;
    private final String preparedYQL;

    private final String ast;
    private final String plan;
    private final LongAdder usage;
    private final boolean isFullScan;
    private final boolean isError;

    public QueryStat(String sql, String yql, String ast, String plan) {
        this.originSQL = sql;
        this.preparedYQL = yql;
        this.ast = ast;
        this.plan = plan;
        this.usage = new LongAdder();
        this.isFullScan = plan.contains("\"Node Type\":\"TableFullScan\"");
        this.isError = false;
    }

    public QueryStat(String sql, String yql, Status error) {
        this.originSQL = sql;
        this.preparedYQL = yql;
        this.ast = null;
        this.plan = error.toString();
        this.usage = new LongAdder();
        this.isFullScan = false;
        this.isError = true;
    }

    public long getUsageCounter() {
        return usage.longValue();
    }

    public String getOriginSQL() {
        return originSQL;
    }

    public String getPreparedYQL() {
        return preparedYQL;
    }

    public String getAat() {
        return ast;
    }

    public String getPlan() {
        return plan;
    }

    public boolean isFullScan() {
        return isFullScan;
    }

    public boolean isError() {
        return isError;
    }

    public void incrementUsage() {
        this.usage.increment();
    }

    public static ResultSetReader toResultSetReader(Collection<QueryStat> stats) {
        FixedResultSetFactory.ResultSetBuilder builder = STATS_RS_FACTORY.createResultSet();
        for (QueryStat stat: stats) {
            builder.newRow()
                    .withTextValue("sql", stat.originSQL)
                    .withBoolValue("is_fullscan", stat.isFullScan)
                    .withBoolValue("is_error", stat.isError)
                    .withLongValue("executed", stat.usage.longValue())
                    .withTextValue("yql", stat.preparedYQL)
                    .withTextValue("ast", stat.ast)
                    .withTextValue("plan", stat.plan)
                    .build();
        }
        return builder.build();
    }

    public static boolean isPrint(String sql) {
        return sql != null && PRINT_QUERY.equalsIgnoreCase(sql.trim());
    }

    public static boolean isReset(String sql) {
        return sql != null && RESET_QUERY.equalsIgnoreCase(sql.trim());
    }
}
