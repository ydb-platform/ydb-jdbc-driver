package tech.ydb.jdbc.context;

import java.util.Collection;
import java.util.concurrent.atomic.LongAdder;

import tech.ydb.core.Status;
import tech.ydb.jdbc.common.FixedResultSetFactory;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryStat {
    public static final String QUERY = "print_jdbc_stats();";

    private static final FixedResultSetFactory STATS_RS_FACTORY = FixedResultSetFactory.newBuilder()
            .addTextColumn("sql")
            .addBooleanColumn("is_fullscan")
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

    public QueryStat(YdbQuery query, String ast, String plan) {
        this.originSQL = query.getOriginQuery();
        this.preparedYQL = query.getPreparedYql();
        this.ast = ast;
        this.plan = plan;
        this.usage = new LongAdder();
        this.isFullScan = plan.contains("\"Node Type\":\"TableFullScan\"");
    }

    public QueryStat(YdbQuery query, Status error) {
        this.originSQL = query.getOriginQuery();
        this.preparedYQL = query.getPreparedYql();
        this.ast = error.toString();
        this.plan = error.toString();
        this.usage = new LongAdder();
        this.isFullScan = false;
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

    public void incrementUsage() {
        this.usage.increment();
    }

    public static ResultSetReader toResultSetReader(Collection<QueryStat> stats) {
        FixedResultSetFactory.ResultSetBuilder builder = STATS_RS_FACTORY.createResultSet();
        for (QueryStat stat: stats) {
            builder.newRow()
                    .withTextValue("sql", stat.originSQL)
                    .withBoolValue("is_fullscan", stat.isFullScan)
                    .withLongValue("executed", stat.usage.longValue())
                    .withTextValue("yql", stat.preparedYQL)
                    .withTextValue("ast", stat.ast)
                    .withTextValue("plan", stat.plan)
                    .build();
        }
        return builder.build();
    }
}
