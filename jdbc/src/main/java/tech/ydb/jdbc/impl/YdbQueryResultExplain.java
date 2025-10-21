package tech.ydb.jdbc.impl;

import java.sql.SQLException;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.common.FixedResultSetFactory;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryResultExplain extends YdbQueryResultBase {
    private static final FixedResultSetFactory EXPLAIN_RS_FACTORY = FixedResultSetFactory.newBuilder()
            .addTextColumn(YdbConst.EXPLAIN_COLUMN_AST)
            .addTextColumn(YdbConst.EXPLAIN_COLUMN_PLAN)
            .build();

    private final YdbResultSet rs;

    public YdbQueryResultExplain(YdbTypes types, YdbStatement statement, String ast, String plan) {
        super(0);

        ResultSetReader result = EXPLAIN_RS_FACTORY.createResultSet().newRow()
                .withTextValue(YdbConst.EXPLAIN_COLUMN_AST, ast)
                .withTextValue(YdbConst.EXPLAIN_COLUMN_PLAN, plan)
                .build().build();

        this.rs = new YdbResultSetMemory(types, statement, result);
    }


    @Override
    protected YdbResultSet getResultSet(int index) throws SQLException {
        return index == 0 ? rs : null;
    }

    @Override
    protected void closeResultSet(int index) throws SQLException {
        if (index == 0) {
            rs.close();
        }
    }
}
