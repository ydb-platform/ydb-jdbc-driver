package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.FixedResultSetFactory;
import tech.ydb.jdbc.impl.FixedResultSetImpl;
import tech.ydb.jdbc.query.QueryStatement;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryResult {
    public static final YdbQueryResult EMPTY = new YdbQueryResult(null);

    private static final FixedResultSetFactory EXPLAIN_RS_FACTORY = FixedResultSetFactory.newBuilder()
            .addTextColumn(YdbConst.EXPLAIN_COLUMN_AST)
            .addTextColumn(YdbConst.EXPLAIN_COLUMN_PLAN)
            .build();

    private static final ExpressionResult NO_UPDATED = new ExpressionResult(0);
    // TODO: YDB doesn't return the count of affected rows, so we use little hach to return always 1
    private static final ExpressionResult HAS_UPDATED = new ExpressionResult(1);

    private static class ExpressionResult {
        private final int updateCount;
        private final YdbResultSet resultSet;

        ExpressionResult(int updateCount) {
            this.updateCount = updateCount;
            this.resultSet = null;
        }

        ExpressionResult(YdbResultSet resultSet) {
            this.updateCount = -1;
            this.resultSet = resultSet;
        }
    }

    private final List<ExpressionResult> results;
    private int resultIndex;

    YdbQueryResult(List<ExpressionResult> results) {
        this.results = results;
        this.resultIndex = 0;
    }

    public void close() {
        // nothing
    }

    public boolean hasResultSets() {
        if (results == null || resultIndex >= results.size()) {
            return false;
        }

        return results.get(resultIndex).resultSet != null;
    }

    public int getUpdateCount() {
        if (results == null || resultIndex >= results.size()) {
            return -1;
        }

        return results.get(resultIndex).updateCount;
    }

    public YdbResultSet getCurrentResultSet()  {
        if (results == null || resultIndex >= results.size()) {
            return null;
        }
        return results.get(resultIndex).resultSet;
    }

    public YdbResultSet getResultSet(int index) {
        if (results == null || index < 0 || index >= results.size()) {
            return null;
        }
        return results.get(index).resultSet;
    }

    public boolean getMoreResults(int current) throws SQLException {
        if (results == null || resultIndex >= results.size()) {
            return false;
        }

        switch (current) {
            case Statement.KEEP_CURRENT_RESULT:
                break;
            case Statement.CLOSE_CURRENT_RESULT:
                results.get(resultIndex).resultSet.close();
                break;
            case Statement.CLOSE_ALL_RESULTS:
                for (int idx = 0; idx <= resultIndex; idx += 1) {
                    results.get(idx).resultSet.close();
                }
                break;
            default:
                throw new SQLException(YdbConst.RESULT_SET_MODE_UNSUPPORTED + current);
        }

        resultIndex += 1;
        return hasResultSets();
    }

    public static YdbQueryResult fromResults(YdbQuery query, List<YdbResultSet> rsList) {
        List<ExpressionResult> results = new ArrayList<>();
        int idx = 0;
        for (QueryStatement exp: query.getStatements()) {
            if (exp.isDDL()) {
                results.add(NO_UPDATED);
                continue;
            }
            if (exp.hasUpdateCount()) {
                results.add(HAS_UPDATED);
                continue;
            }

            if (exp.hasResults() && idx < rsList.size()) {
                results.add(new ExpressionResult(rsList.get(idx)));
                idx++;
            }
        }

        while (idx < rsList.size())  {
            results.add(new ExpressionResult(rsList.get(idx)));
            idx++;
        }

        return new YdbQueryResult(results);
    }

    public static YdbQueryResult fromExplain(YdbStatement statement, String ast, String plan) {
        ResultSetReader result = EXPLAIN_RS_FACTORY.createResultSet()
                .newRow()
                .withTextValue(YdbConst.EXPLAIN_COLUMN_AST, ast)
                .withTextValue(YdbConst.EXPLAIN_COLUMN_PLAN, plan)
                .build()
                .build();
        YdbResultSet rs = new FixedResultSetImpl(statement, result);
        return new YdbQueryResult(Collections.singletonList(new ExpressionResult(rs)));
    }
}
