package tech.ydb.jdbc.statement;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.YdbResultSet;

import static tech.ydb.jdbc.YdbConst.RESULT_SET_MODE_UNSUPPORTED;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbStatementResultSets {
    public static final YdbStatementResultSets EMPTY = new YdbStatementResultSets();

    private final List<YdbResultSet> results;
    private int resultSetIndex;
    private final int updateCount;

    private YdbStatementResultSets() {
        results = null;
        resultSetIndex = -1;
        updateCount = -1;
    }

    public YdbStatementResultSets(List<YdbResultSet> list) {
        updateCount = 0;
        results = list;
        resultSetIndex = 0;
    }

    public boolean hasResultSets() {
        return results != null && !results.isEmpty();
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public YdbResultSet getCurrentResultSet() throws SQLException {
        return getResultSet(resultSetIndex);
    }

    public YdbResultSet getResultSet(int index) throws SQLException {
        if (results == null) {
            return null;
        }
        if (index < 0 || index >= results.size()) {
            return null;
        }

        YdbResultSet resultSet = results.get(index);
        if (resultSet.isClosed()) {
            throw new SQLException(YdbConst.RESULT_SET_UNAVAILABLE + index);
        }
        return resultSet;
    }


    public boolean getMoreResults(int current) throws SQLException {
        if (results == null || results.isEmpty()) {
            return false;
        }

        switch (current) {
            case Statement.KEEP_CURRENT_RESULT:
                break;
            case Statement.CLOSE_CURRENT_RESULT:
                results.get(resultSetIndex).close();
                break;
            case Statement.CLOSE_ALL_RESULTS:
                for (int idx = 0; idx <= resultSetIndex; idx += 1) {
                    results.get(idx).close();
                }
                break;
            default:
                throw new SQLException(RESULT_SET_MODE_UNSUPPORTED + current);
        }

        resultSetIndex += 1;
        return resultSetIndex >= 0 && resultSetIndex < results.size();
    }
}
