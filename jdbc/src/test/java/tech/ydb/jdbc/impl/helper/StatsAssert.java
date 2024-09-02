package tech.ydb.jdbc.impl.helper;

/**
 *
 * @author Aleksandr Gorshenin
 */
public final class StatsAssert extends TableAssert {
    private final TextColumn querySql = addTextColumn("sql", "Text");;
    private final BoolColumn isFullScan = addBoolColumn("is_fullscan", "Bool");;
    private final BoolColumn isError = addBoolColumn("is_error", "Bool");
    private final LongColumn executed = addLongColumn("executed", "Int64");
    private final TextColumn queryYql= addTextColumn("yql", "Text");
    private final TextColumn queryAst = addTextColumn("ast", "Text");
    private final TextColumn queryPlan = addTextColumn("plan", "Text");

    public ValueAssert sql(String sql) {
        return querySql.eq(sql);
    }

    public ValueAssert yql(String yql) {
        return queryYql.eq(yql);
    }

    public ValueAssert hasAst() {
        return queryAst.isNotEmpty();
    }

    public ValueAssert hasNoAst() {
        return queryAst.isNull();
    }

    public ValueAssert hasPlan() {
        return queryPlan.isNotEmpty();
    }

    public ValueAssert executed(long count) {
        return executed.eq(count);
    }

    public ValueAssert isFullScan() {
        return isFullScan.eq(true);
    }

    public ValueAssert isNotFullScan() {
        return isFullScan.eq(false);
    }

    public ValueAssert isError() {
        return isError.eq(true);
    }

    public ValueAssert isNotError() {
        return isError.eq(false);
    }
}
